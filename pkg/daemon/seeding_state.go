package daemon

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type SeedingStateManager struct {
	filePath string
	state    *SeedingState
	mu       sync.RWMutex
}

type SeedingState struct {
	Version     string         `json:"version"`
	LastUpdated time.Time      `json:"last_updated"`
	Seeding     []SeedingEntry `json:"seeding"`
}

type SeedingEntry struct {
	FileHash    string    `json:"file_hash"`
	FilePath    string    `json:"file_path"`
	FallbackURL string    `json:"fallback_url,omitempty"`
	Size        uint64    `json:"size"`
	ChunkCount  uint32    `json:"chunk_count"`
	StartedAt   time.Time `json:"started_at"`
	BytesServed uint64    `json:"bytes_served"`
	PeersServed int       `json:"peers_served"`
}

func NewSeedingStateManager(filePath string) (*SeedingStateManager, error) {
	if expandedPath, err := ExpandPath(filePath); err == nil {
		filePath = expandedPath
	}

	ssm := &SeedingStateManager{
		filePath: filePath,
		state: &SeedingState{
			Version: "1.0",
			Seeding: make([]SeedingEntry, 0),
		},
	}

	if err := ssm.Load(); err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("failed to load seeding state: %w", err)
		}
	}

	return ssm, nil
}

func (ssm *SeedingStateManager) Load() error {
	ssm.mu.Lock()
	defer ssm.mu.Unlock()

	data, err := os.ReadFile(ssm.filePath)
	if err != nil {
		return err
	}

	var state SeedingState
	if err := json.Unmarshal(data, &state); err != nil {
		return fmt.Errorf("failed to parse seeding state: %w", err)
	}

	ssm.state = &state
	return nil
}

func (ssm *SeedingStateManager) Save() error {
	ssm.mu.Lock()
	defer ssm.mu.Unlock()

	ssm.state.LastUpdated = time.Now()

	dir := filepath.Dir(ssm.filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	data, err := json.MarshalIndent(ssm.state, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal state: %w", err)
	}

	tmpPath := ssm.filePath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write temporary file: %w", err)
	}

	if err := os.Rename(tmpPath, ssm.filePath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to rename file: %w", err)
	}

	return nil
}

func (ssm *SeedingStateManager) AddSeeding(entry SeedingEntry) error {
	ssm.mu.Lock()
	defer ssm.mu.Unlock()

	for i, existing := range ssm.state.Seeding {
		if existing.FileHash == entry.FileHash {
			ssm.state.Seeding[i] = entry
			return ssm.saveUnlocked()
		}
	}

	ssm.state.Seeding = append(ssm.state.Seeding, entry)
	return ssm.saveUnlocked()
}

func (ssm *SeedingStateManager) RemoveSeeding(fileHash string) error {
	ssm.mu.Lock()
	defer ssm.mu.Unlock()

	for i, entry := range ssm.state.Seeding {
		if entry.FileHash == fileHash {
			ssm.state.Seeding[i] = ssm.state.Seeding[len(ssm.state.Seeding)-1]
			ssm.state.Seeding = ssm.state.Seeding[:len(ssm.state.Seeding)-1]
			return ssm.saveUnlocked()
		}
	}

	return fmt.Errorf("file hash not found in seeding state")
}

func (ssm *SeedingStateManager) HasHash(fileHash string) bool {
	ssm.mu.RLock()
	defer ssm.mu.RUnlock()

	for _, entry := range ssm.state.Seeding {
		if entry.FileHash == fileHash {
			return true
		}
	}
	return false
}

func (ssm *SeedingStateManager) GetByHash(fileHash string) *SeedingEntry {
	ssm.mu.RLock()
	defer ssm.mu.RUnlock()

	for _, entry := range ssm.state.Seeding {
		if entry.FileHash == fileHash {
			entryCopy := entry
			return &entryCopy
		}
	}
	return nil
}

func (ssm *SeedingStateManager) GetAll() []SeedingEntry {
	ssm.mu.RLock()
	defer ssm.mu.RUnlock()

	result := make([]SeedingEntry, len(ssm.state.Seeding))
	copy(result, ssm.state.Seeding)
	return result
}

func (ssm *SeedingStateManager) UpdateStats(fileHash string, bytesServed uint64, peersServed int) error {
	ssm.mu.Lock()
	defer ssm.mu.Unlock()

	for i, entry := range ssm.state.Seeding {
		if entry.FileHash == fileHash {
			ssm.state.Seeding[i].BytesServed = bytesServed
			ssm.state.Seeding[i].PeersServed = peersServed
			return ssm.saveUnlocked()
		}
	}

	return fmt.Errorf("file hash not found in seeding state")
}

func (ssm *SeedingStateManager) Count() int {
	ssm.mu.RLock()
	defer ssm.mu.RUnlock()

	return len(ssm.state.Seeding)
}

func (ssm *SeedingStateManager) Clear() error {
	ssm.mu.Lock()
	defer ssm.mu.Unlock()

	ssm.state.Seeding = make([]SeedingEntry, 0)
	return ssm.saveUnlocked()
}

func (ssm *SeedingStateManager) saveUnlocked() error {
	ssm.state.LastUpdated = time.Now()

	dir := filepath.Dir(ssm.filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	data, err := json.MarshalIndent(ssm.state, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal state: %w", err)
	}

	tmpPath := ssm.filePath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write temporary file: %w", err)
	}

	if err := os.Rename(tmpPath, ssm.filePath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to rename file: %w", err)
	}

	return nil
}
