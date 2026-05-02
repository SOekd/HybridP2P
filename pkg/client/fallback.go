package client

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"P2P-CDN/internal/logger"
	"P2P-CDN/pkg/protocol"
	"P2P-CDN/pkg/storage"

	"go.uber.org/zap"
)

type HTTPFallback struct {
	client        *http.Client
	parallelConns int
	chunkSize     int
	maxRetries    int
	retryBackoff  time.Duration
	progressChan  chan<- DownloadProgress
}

type DownloadProgress struct {
	FileHash       protocol.FileHash
	TotalSize      uint64
	Downloaded     uint64
	ChunksComplete uint32
	ChunksTotal    uint32
	DownloadRate   uint64
	Status         string
	UsingFallback  bool
}

func NewHTTPFallback(parallelConns int, progressChan chan<- DownloadProgress) *HTTPFallback {
	return &HTTPFallback{
		client: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        10,
				IdleConnTimeout:     30 * time.Second,
				MaxIdleConnsPerHost: parallelConns,
			},
		},
		parallelConns: parallelConns,
		chunkSize:     512 * 1024,
		maxRetries:    3,
		retryBackoff:  2 * time.Second,
		progressChan:  progressChan,
	}
}

func (h *HTTPFallback) DownloadFile(
	ctx context.Context,
	metadata *protocol.FileMetadata,
	outputPath string,
) error {
	logger.Info("starting HTTP fallback download",
		zap.String("url", metadata.FallbackURL),
		zap.Uint64("size", metadata.Size),
		zap.Int("parallel_conns", h.parallelConns))

	supportsRange, err := h.checkRangeSupport(ctx, metadata.FallbackURL)
	if err != nil {
		return fmt.Errorf("failed to check Range support: %w", err)
	}

	if !supportsRange {
		logger.Warn("server doesn't support Range requests, falling back to single connection")
		return h.downloadSingleConnection(ctx, metadata, outputPath)
	}

	return h.downloadParallel(ctx, metadata, outputPath)
}

func (h *HTTPFallback) checkRangeSupport(ctx context.Context, url string) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, "HEAD", url, nil)
	if err != nil {
		return false, err
	}

	resp, err := h.client.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	acceptRanges := resp.Header.Get("Accept-Ranges")
	return acceptRanges == "bytes", nil
}

func (h *HTTPFallback) downloadSingleConnection(
	ctx context.Context,
	metadata *protocol.FileMetadata,
	outputPath string,
) error {
	logger.Info("downloading without Range requests")

	req, err := http.NewRequestWithContext(ctx, "GET", metadata.FallbackURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := h.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to download: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	dir := filepath.Dir(outputPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	file, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	return h.copyWithProgress(ctx, file, resp.Body, metadata)
}

func (h *HTTPFallback) downloadParallel(
	ctx context.Context,
	metadata *protocol.FileMetadata,
	outputPath string,
) error {
	logger.Info("downloading with parallel Range requests",
		zap.Uint32("chunk_count", metadata.ChunkCount))

	dir := filepath.Dir(outputPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	file, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	if err := file.Truncate(int64(metadata.Size)); err != nil {
		return fmt.Errorf("failed to allocate file: %w", err)
	}

	type chunkJob struct {
		index  uint32
		offset uint64
		size   uint32
	}

	jobs := make(chan chunkJob, metadata.ChunkCount)
	for i := uint32(0); i < metadata.ChunkCount; i++ {
		jobs <- chunkJob{
			index:  i,
			offset: uint64(i) * uint64(metadata.ChunkSize),
			size:   metadata.ChunkSize,
		}
	}
	close(jobs)

	var (
		downloaded   uint64
		downloadedMu sync.Mutex
		errChan      = make(chan error, h.parallelConns)
		wg           sync.WaitGroup
		startTime    = time.Now()
	)

	for i := 0; i < h.parallelConns; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for job := range jobs {
				select {
				case <-ctx.Done():
					errChan <- ctx.Err()
					return
				default:
				}

				err := h.downloadChunkWithRetry(ctx, file, metadata.FallbackURL, job.offset, job.size)
				if err != nil {
					logger.Error("failed to download chunk",
						zap.Int("worker", workerID),
						zap.Uint32("chunk", job.index),
						zap.Error(err))
					errChan <- err
					return
				}

				downloadedMu.Lock()
				downloaded += uint64(job.size)
				elapsed := time.Since(startTime).Seconds()
				rate := uint64(0)
				if elapsed > 0 {
					rate = uint64(float64(downloaded) / elapsed)
				}

				if h.progressChan != nil {
					select {
					case h.progressChan <- DownloadProgress{
						FileHash:       metadata.Hash,
						TotalSize:      metadata.Size,
						Downloaded:     downloaded,
						ChunksComplete: job.index + 1,
						ChunksTotal:    metadata.ChunkCount,
						DownloadRate:   rate,
						Status:         "downloading",
						UsingFallback:  true,
					}:
					default:
					}
				}
				downloadedMu.Unlock()

				logger.Debug("chunk downloaded",
					zap.Int("worker", workerID),
					zap.Uint32("chunk", job.index),
					zap.Uint32("total", metadata.ChunkCount))
			}
		}(i)
	}

	wg.Wait()
	close(errChan)

	if err := <-errChan; err != nil {
		return fmt.Errorf("download failed: %w", err)
	}

	logger.Info("HTTP fallback download complete",
		zap.String("output", outputPath),
		zap.Uint64("size", metadata.Size))

	return nil
}

func (h *HTTPFallback) downloadChunkWithRetry(
	ctx context.Context,
	file *os.File,
	url string,
	offset uint64,
	size uint32,
) error {
	var lastErr error

	for attempt := 0; attempt < h.maxRetries; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(math.Pow(2, float64(attempt))) * h.retryBackoff
			logger.Debug("retrying chunk download",
				zap.Int("attempt", attempt),
				zap.Duration("backoff", backoff))

			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		err := h.downloadChunk(ctx, file, url, offset, size)
		if err == nil {
			return nil
		}

		lastErr = err
	}

	return fmt.Errorf("failed after %d retries: %w", h.maxRetries, lastErr)
}

func (h *HTTPFallback) downloadChunk(
	ctx context.Context,
	file *os.File,
	url string,
	offset uint64,
	size uint32,
) error {
	rangeHeader := fmt.Sprintf("bytes=%d-%d", offset, offset+uint64(size)-1)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Range", rangeHeader)

	resp, err := h.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	data := make([]byte, size)
	n, err := io.ReadFull(resp.Body, data)
	if err != nil && err != io.ErrUnexpectedEOF {
		return fmt.Errorf("failed to read chunk: %w", err)
	}

	if _, err := file.WriteAt(data[:n], int64(offset)); err != nil {
		return fmt.Errorf("failed to write chunk: %w", err)
	}

	return nil
}

func (h *HTTPFallback) copyWithProgress(
	ctx context.Context,
	dst io.Writer,
	src io.Reader,
	metadata *protocol.FileMetadata,
) error {
	var (
		downloaded uint64
		startTime  = time.Now()
		buffer     = make([]byte, 32*1024)
	)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		n, err := src.Read(buffer)
		if n > 0 {
			if _, werr := dst.Write(buffer[:n]); werr != nil {
				return werr
			}

			downloaded += uint64(n)
			elapsed := time.Since(startTime).Seconds()
			rate := uint64(0)
			if elapsed > 0 {
				rate = uint64(float64(downloaded) / elapsed)
			}

			if h.progressChan != nil {
				select {
				case h.progressChan <- DownloadProgress{
					FileHash:       metadata.Hash,
					TotalSize:      metadata.Size,
					Downloaded:     downloaded,
					ChunksComplete: uint32(float64(downloaded) / float64(metadata.ChunkSize)),
					ChunksTotal:    metadata.ChunkCount,
					DownloadRate:   rate,
					Status:         "downloading",
					UsingFallback:  true,
				}:
				default:
				}
			}
		}

		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}

	return nil
}

func (h *HTTPFallback) VerifyDownload(filePath string, metadata *protocol.FileMetadata) error {
	logger.Info("verifying downloaded file", zap.String("path", filePath))

	hash, err := storage.HashFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to hash file: %w", err)
	}

	if hash != metadata.Hash {
		return fmt.Errorf("hash mismatch: expected %x, got %x", metadata.Hash, hash)
	}

	logger.Info("file verification successful")
	return nil
}
