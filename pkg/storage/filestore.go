package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
)

type FileBlockstore struct {
	path       string
	hashOnRead bool
	mu         sync.RWMutex
}

func NewFileBlockstore(path string) (*FileBlockstore, error) {
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, fmt.Errorf("failed to create blockstore directory: %w", err)
	}

	return &FileBlockstore{
		path:       path,
		hashOnRead: false,
	}, nil
}

func (f *FileBlockstore) blockPath(c cid.Cid) string {
	cidStr := c.String()
	if len(cidStr) >= 2 {
		subdir := cidStr[:2]
		return filepath.Join(f.path, subdir, cidStr)
	}
	return filepath.Join(f.path, cidStr)
}

func (f *FileBlockstore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	path := f.blockPath(c)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrBlockNotFound
		}
		return nil, fmt.Errorf("failed to read block: %w", err)
	}

	return blocks.NewBlockWithCid(data, c)
}

func (f *FileBlockstore) Put(ctx context.Context, block blocks.Block) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	path := f.blockPath(block.Cid())

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	if err := os.WriteFile(path, block.RawData(), 0644); err != nil {
		return fmt.Errorf("failed to write block: %w", err)
	}

	return nil
}

func (f *FileBlockstore) PutMany(ctx context.Context, blocks []blocks.Block) error {
	for _, block := range blocks {
		if err := f.Put(ctx, block); err != nil {
			return err
		}
	}
	return nil
}

func (f *FileBlockstore) Has(ctx context.Context, c cid.Cid) (bool, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
	}

	path := f.blockPath(c)
	_, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("failed to stat block: %w", err)
	}

	return true, nil
}

func (f *FileBlockstore) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}

	path := f.blockPath(c)
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, ErrBlockNotFound
		}
		return 0, fmt.Errorf("failed to stat block: %w", err)
	}

	return int(info.Size()), nil
}

func (f *FileBlockstore) Delete(ctx context.Context, c cid.Cid) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	path := f.blockPath(c)
	if err := os.Remove(path); err != nil {
		if os.IsNotExist(err) {
			return ErrBlockNotFound
		}
		return fmt.Errorf("failed to delete block: %w", err)
	}

	return nil
}

func (f *FileBlockstore) DeleteBlock(ctx context.Context, c cid.Cid) error {
	return f.Delete(ctx, c)
}

func (f *FileBlockstore) DeleteMany(ctx context.Context, cids []cid.Cid) error {
	for _, c := range cids {
		if err := f.Delete(ctx, c); err != nil && err != ErrBlockNotFound {
			return err
		}
	}
	return nil
}

func (f *FileBlockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	ch := make(chan cid.Cid)

	go func() {
		defer close(ch)

		err := filepath.Walk(f.path, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			if info.IsDir() {
				return nil
			}

			cidStr := filepath.Base(path)
			c, err := cid.Decode(cidStr)
			if err != nil {
				return nil
			}

			select {
			case ch <- c:
			case <-ctx.Done():
				return ctx.Err()
			}

			return nil
		})

		if err != nil && err != context.Canceled {
			fmt.Fprintf(os.Stderr, "error walking blockstore: %v\n", err)
		}
	}()

	return ch, nil
}

func (f *FileBlockstore) HashOnRead(enabled bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hashOnRead = enabled
}

func (f *FileBlockstore) Close() error {
	return nil
}

func (f *FileBlockstore) View(ctx context.Context, c cid.Cid, callback func([]byte) error) error {
	f.mu.RLock()
	defer f.mu.RUnlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	path := f.blockPath(c)
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return ErrBlockNotFound
		}
		return fmt.Errorf("failed to open block: %w", err)
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return fmt.Errorf("failed to read block: %w", err)
	}

	return callback(data)
}

func (f *FileBlockstore) GetBlockCount(ctx context.Context) (int, error) {
	ch, err := f.AllKeysChan(ctx)
	if err != nil {
		return 0, err
	}

	count := 0
	for range ch {
		count++
	}

	return count, nil
}

func (f *FileBlockstore) GetTotalSize(ctx context.Context) (int64, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	var totalSize int64

	err := filepath.Walk(f.path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if !info.IsDir() {
			totalSize += info.Size()
		}

		return nil
	})

	if err != nil {
		return 0, fmt.Errorf("failed to calculate total size: %w", err)
	}

	return totalSize, nil
}
