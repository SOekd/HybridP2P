package protocol

import (
	"encoding/hex"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

type FileHash [32]byte

func (h FileHash) String() string {
	return hex.EncodeToString(h[:])
}

func FileHashFromString(s string) (FileHash, error) {
	var hash FileHash
	bytes, err := hex.DecodeString(s)
	if err != nil {
		return hash, err
	}
	copy(hash[:], bytes)
	return hash, nil
}

type ChunkInfo struct {
	Hash   FileHash `json:"hash"`
	Index  uint32   `json:"index"`
	Size   uint32   `json:"size"`
	Offset uint64   `json:"offset"`
}

type FileMetadata struct {
	Hash        FileHash    `json:"hash"`
	Size        uint64      `json:"size"`
	ChunkSize   uint32      `json:"chunk_size"`
	ChunkCount  uint32      `json:"chunk_count"`
	Chunks      []ChunkInfo `json:"chunks"`
	FallbackURL string      `json:"fallback_url"`
	CreatedAt   time.Time   `json:"created_at"`
}

type PeerInfo struct {
	PeerID     peer.ID    `json:"peer_id"`
	Addrs      []string   `json:"addrs"`
	FileHashes []FileHash `json:"file_hashes"`
	LastSeen   time.Time  `json:"last_seen"`
	NATType    NATType    `json:"nat_type"`
	Reachable  bool       `json:"reachable"`
}

type NATType string

const (
	NATTypeUnknown  NATType = "unknown"
	NATTypeOpen     NATType = "open"
	NATTypeModerate NATType = "moderate"
	NATTypeStrict   NATType = "strict"
)

type DownloadStatusCode string

const (
	DownloadStatusPending     DownloadStatusCode = "pending"
	DownloadStatusDownloading DownloadStatusCode = "downloading"
	DownloadStatusSeeding     DownloadStatusCode = "seeding"
	DownloadStatusComplete    DownloadStatusCode = "complete"
	DownloadStatusFailed      DownloadStatusCode = "failed"
	DownloadStatusCancelled   DownloadStatusCode = "cancelled"
)

type DownloadStatus struct {
	FileHash       FileHash           `json:"file_hash"`
	TotalSize      uint64             `json:"total_size"`
	Downloaded     uint64             `json:"downloaded"`
	ChunksComplete uint32             `json:"chunks_complete"`
	ChunksTotal    uint32             `json:"chunks_total"`
	Peers          []peer.ID          `json:"peers"`
	DownloadRate   float64            `json:"download_rate"`
	Status         DownloadStatusCode `json:"status"`
	UsingFallback  bool               `json:"using_fallback"`
}
