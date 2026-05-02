package tracker

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"P2P-CDN/pkg/protocol"

	"github.com/go-redis/redis/v8"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

type Store struct {
	db    *sqlx.DB
	redis *redis.Client
}

func NewStore(mysqlDSN string, redisURL string, redisPassword string, redisDB int) (*Store, error) {
	db, err := sqlx.Connect("mysql", mysqlDSN)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to mysql: %w", err)
	}

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	rdb := redis.NewClient(&redis.Options{
		Addr:     redisURL,
		Password: redisPassword,
		DB:       redisDB,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to connect to redis: %w", err)
	}

	db.Exec("SET time_zone = '+00:00'")
	db.Exec("SET sql_mode = 'TRADITIONAL'")

	store := &Store{
		db:    db,
		redis: rdb,
	}

	if err := store.InitSchema(context.Background()); err != nil {
		db.Close()
		rdb.Close()
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	return store, nil
}

func (s *Store) InitSchema(ctx context.Context) error {
	schemas := []string{
		`CREATE TABLE IF NOT EXISTS files (
			hash VARCHAR(64) PRIMARY KEY,
			size BIGINT NOT NULL,
			chunk_size INTEGER NOT NULL,
			chunk_count INTEGER NOT NULL,
			chunks JSON DEFAULT NULL,
			fallback_url TEXT NOT NULL,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			INDEX idx_files_created_at (created_at DESC)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`,

		`CREATE TABLE IF NOT EXISTS peers (
			peer_id VARCHAR(128) PRIMARY KEY,
			addrs JSON NOT NULL,
			nat_type VARCHAR(20) NOT NULL DEFAULT 'unknown',
			reachable BOOLEAN NOT NULL DEFAULT false,
			last_seen TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			INDEX idx_peers_last_seen (last_seen DESC),
			INDEX idx_peers_nat_type (nat_type)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`,

		`CREATE TABLE IF NOT EXISTS peer_files (
			peer_id VARCHAR(128) NOT NULL,
			file_hash VARCHAR(64) NOT NULL,
			announced_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (peer_id, file_hash),
			INDEX idx_peer_files_file_hash (file_hash),
			INDEX idx_peer_files_peer_id (peer_id),
			INDEX idx_peer_files_announced_at (announced_at DESC),
			FOREIGN KEY (peer_id) REFERENCES peers(peer_id) ON DELETE CASCADE,
			FOREIGN KEY (file_hash) REFERENCES files(hash) ON DELETE CASCADE
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`,

		`CREATE TABLE IF NOT EXISTS relay_servers (
			peer_id VARCHAR(128) PRIMARY KEY,
			addrs JSON NOT NULL,
			capacity INTEGER NOT NULL DEFAULT 100,
			current_load INTEGER NOT NULL DEFAULT 0,
			region VARCHAR(50),
			enabled BOOLEAN NOT NULL DEFAULT true,
			last_heartbeat TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			INDEX idx_relay_servers_enabled (enabled),
			INDEX idx_relay_servers_last_heartbeat (last_heartbeat DESC)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`,

		`CREATE TABLE IF NOT EXISTS tracker_stats (
			id INTEGER PRIMARY KEY AUTO_INCREMENT,
			total_files BIGINT NOT NULL DEFAULT 0,
			total_peers BIGINT NOT NULL DEFAULT 0,
			total_announces BIGINT NOT NULL DEFAULT 0,
			total_queries BIGINT NOT NULL DEFAULT 0,
			timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			INDEX idx_tracker_stats_timestamp (timestamp DESC)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`,
	}

	for _, schema := range schemas {
		if _, err := s.db.ExecContext(ctx, schema); err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
	}

	var count int
	err := s.db.GetContext(ctx, &count, "SELECT COUNT(*) FROM tracker_stats")
	if err != nil {
		return fmt.Errorf("failed to check tracker_stats: %w", err)
	}

	if count == 0 {
		_, err = s.db.ExecContext(ctx, `
			INSERT INTO tracker_stats (total_files, total_peers, total_announces, total_queries, timestamp)
			VALUES (0, 0, 0, 0, NOW())
		`)
		if err != nil {
			return fmt.Errorf("failed to insert initial stats: %w", err)
		}
	}

	return nil
}

func (s *Store) Close() error {
	if err := s.redis.Close(); err != nil {
		return err
	}
	return s.db.Close()
}

func (s *Store) RegisterFile(ctx context.Context, req *protocol.RegisterFileRequest) error {
	var chunksJSON *string
	if len(req.Chunks) > 0 {
		chunksBytes, err := json.Marshal(req.Chunks)
		if err != nil {
			return fmt.Errorf("failed to marshal chunks: %w", err)
		}
		chunksStr := string(chunksBytes)
		chunksJSON = &chunksStr
	}

	query := `
		INSERT INTO files (hash, size, chunk_size, chunk_count, chunks, fallback_url, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, NOW(), NOW())
		ON DUPLICATE KEY UPDATE
			size = VALUES(size),
			chunk_size = VALUES(chunk_size),
			chunk_count = VALUES(chunk_count),
			chunks = VALUES(chunks),
			fallback_url = VALUES(fallback_url),
			updated_at = NOW()
	`

	_, err := s.db.ExecContext(ctx, query,
		req.Hash,
		req.Size,
		req.ChunkSize,
		req.ChunkCount,
		chunksJSON,
		req.FallbackURL,
	)

	if err != nil {
		return fmt.Errorf("failed to register file: %w", err)
	}

	s.redis.Del(ctx, fmt.Sprintf("file:%s", req.Hash))

	return nil
}

func (s *Store) GetFile(ctx context.Context, hash string) (*FileRecord, error) {
	cacheKey := fmt.Sprintf("file:%s", hash)
	cached, err := s.redis.Get(ctx, cacheKey).Result()
	if err == nil {
		var file FileRecord
		if err := json.Unmarshal([]byte(cached), &file); err == nil {
			return &file, nil
		}
	}

	var file FileRecord
	query := `
		SELECT hash, size, chunk_size, chunk_count, chunks, fallback_url, created_at, updated_at
		FROM files
		WHERE hash = ?
	`

	err = s.db.GetContext(ctx, &file, query, hash)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("file not found")
		}
		return nil, fmt.Errorf("failed to get file: %w", err)
	}

	if data, err := json.Marshal(file); err == nil {
		s.redis.Set(ctx, cacheKey, data, 5*time.Minute)
	}

	return &file, nil
}

func (s *Store) AnnouncePeer(ctx context.Context, req *protocol.AnnouncePeerRequest) error {
	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	addrsJSON, err := json.Marshal(req.Addrs)
	if err != nil {
		return fmt.Errorf("failed to marshal addrs: %w", err)
	}

	peerQuery := `
		INSERT INTO peers (peer_id, addrs, nat_type, reachable, last_seen, created_at)
		VALUES (?, ?, ?, ?, NOW(), NOW())
		ON DUPLICATE KEY UPDATE
			addrs = VALUES(addrs),
			nat_type = VALUES(nat_type),
			last_seen = NOW()
	`

	reachable := req.NATType == string(protocol.NATTypeOpen)

	_, err = tx.ExecContext(ctx, peerQuery,
		req.PeerID,
		string(addrsJSON),
		req.NATType,
		reachable,
	)

	if err != nil {
		return fmt.Errorf("failed to upsert peer: %w", err)
	}

	deleteQuery := `DELETE FROM peer_files WHERE peer_id = ?`
	_, err = tx.ExecContext(ctx, deleteQuery, req.PeerID)
	if err != nil {
		return fmt.Errorf("failed to delete old peer files: %w", err)
	}

	if len(req.FileHashes) > 0 {
		insertQuery := `
			INSERT INTO peer_files (peer_id, file_hash, announced_at)
			VALUES (?, ?, NOW())
			ON DUPLICATE KEY UPDATE announced_at = NOW()
		`

		for _, fileHash := range req.FileHashes {
			_, err = tx.ExecContext(ctx, insertQuery, req.PeerID, fileHash)
			if err != nil {
				return fmt.Errorf("failed to insert peer file: %w", err)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	for _, fileHash := range req.FileHashes {
		s.redis.Del(ctx, fmt.Sprintf("peers:%s", fileHash))
	}

	return nil
}

func (s *Store) GetPeers(ctx context.Context, fileHash string) ([]PeerRecord, error) {
	cacheKey := fmt.Sprintf("peers:%s", fileHash)
	cached, err := s.redis.Get(ctx, cacheKey).Result()
	if err == nil {
		var peers []PeerRecord
		if err := json.Unmarshal([]byte(cached), &peers); err == nil {
			return peers, nil
		}
	}

	var peers []PeerRecord
	query := `
		SELECT DISTINCT p.peer_id, p.addrs, p.nat_type, p.reachable, p.last_seen
		FROM peers p
		INNER JOIN peer_files pf ON p.peer_id = pf.peer_id
		WHERE pf.file_hash = ?
			AND p.last_seen > DATE_SUB(NOW(), INTERVAL 5 MINUTE)
		ORDER BY p.last_seen DESC
	`

	err = s.db.SelectContext(ctx, &peers, query, fileHash)
	if err != nil {
		return nil, fmt.Errorf("failed to get peers: %w", err)
	}

	for i := range peers {
		if err := peers[i].UnmarshalAddrs(); err != nil {
			return nil, fmt.Errorf("failed to unmarshal addrs: %w", err)
		}
	}

	if data, err := json.Marshal(peers); err == nil {
		s.redis.Set(ctx, cacheKey, data, 1*time.Minute)
	}

	return peers, nil
}

func (s *Store) CleanupStalePeers(ctx context.Context) (int, error) {
	result, err := s.db.ExecContext(ctx,
		"DELETE FROM peers WHERE last_seen < DATE_SUB(NOW(), INTERVAL 10 MINUTE)")
	if err != nil {
		return 0, fmt.Errorf("failed to cleanup stale peers: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get rows affected: %w", err)
	}

	return int(rows), nil
}

func (s *Store) GetStats(ctx context.Context) (*TrackerStats, error) {
	stats := &TrackerStats{}

	err := s.db.GetContext(ctx, &stats.TotalFiles, "SELECT COUNT(*) FROM files")
	if err != nil {
		return nil, fmt.Errorf("failed to get total files: %w", err)
	}

	err = s.db.GetContext(ctx, &stats.ActivePeers,
		"SELECT COUNT(*) FROM peers WHERE last_seen > DATE_SUB(NOW(), INTERVAL 5 MINUTE)")
	if err != nil {
		return nil, fmt.Errorf("failed to get active peers: %w", err)
	}

	err = s.db.GetContext(ctx, &stats.TotalPeers, "SELECT COUNT(*) FROM peers")
	if err != nil {
		return nil, fmt.Errorf("failed to get total peers: %w", err)
	}

	return stats, nil
}

func (s *Store) GetRelayServers(ctx context.Context) ([]RelayServerRecord, error) {
	var relays []RelayServerRecord
	query := `
		SELECT peer_id, addrs, capacity, current_load, region, last_heartbeat
		FROM relay_servers
		WHERE enabled = true
			AND last_heartbeat > DATE_SUB(NOW(), INTERVAL 2 MINUTE)
		ORDER BY (current_load / capacity) ASC
		LIMIT 10
	`

	err := s.db.SelectContext(ctx, &relays, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get relay servers: %w", err)
	}

	for i := range relays {
		if err := relays[i].UnmarshalAddrs(); err != nil {
			return nil, fmt.Errorf("failed to unmarshal addrs: %w", err)
		}
	}

	return relays, nil
}

type FileRecord struct {
	Hash        string    `db:"hash"`
	Size        int64     `db:"size"`
	ChunkSize   int       `db:"chunk_size"`
	ChunkCount  int       `db:"chunk_count"`
	ChunksJSON  *string   `db:"chunks"`
	FallbackURL string    `db:"fallback_url"`
	CreatedAt   time.Time `db:"created_at"`
	UpdatedAt   time.Time `db:"updated_at"`
}

type PeerRecord struct {
	PeerID    string    `db:"peer_id"`
	AddrsJSON string    `db:"addrs"`
	Addrs     []string  `db:"-"`
	NATType   string    `db:"nat_type"`
	Reachable bool      `db:"reachable"`
	LastSeen  time.Time `db:"last_seen"`
}

func (p *PeerRecord) UnmarshalAddrs() error {
	if p.AddrsJSON == "" {
		p.Addrs = []string{}
		return nil
	}
	return json.Unmarshal([]byte(p.AddrsJSON), &p.Addrs)
}

type TrackerStats struct {
	TotalFiles  int `db:"total_files"`
	ActivePeers int `db:"active_peers"`
	TotalPeers  int `db:"total_peers"`
}

type RelayServerRecord struct {
	PeerID        string    `db:"peer_id"`
	AddrsJSON     string    `db:"addrs"`
	Addrs         []string  `db:"-"`
	Capacity      int       `db:"capacity"`
	CurrentLoad   int       `db:"current_load"`
	Region        string    `db:"region"`
	LastHeartbeat time.Time `db:"last_heartbeat"`
}

func (r *RelayServerRecord) UnmarshalAddrs() error {
	if r.AddrsJSON == "" {
		r.Addrs = []string{}
		return nil
	}
	return json.Unmarshal([]byte(r.AddrsJSON), &r.Addrs)
}
