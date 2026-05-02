package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"P2P-CDN/pkg/protocol"

	chunker "github.com/ipfs/boxo/chunker"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
)

type Chunker struct {
	chunkSize uint32
}

func NewChunker(chunkSize uint32) *Chunker {
	if chunkSize < protocol.MinChunkSize {
		chunkSize = protocol.MinChunkSize
	}
	if chunkSize > protocol.MaxChunkSize {
		chunkSize = protocol.MaxChunkSize
	}

	return &Chunker{
		chunkSize: chunkSize,
	}
}

func (c *Chunker) ChunkFile(filePath string, fallbackURL string) (*protocol.FileMetadata, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}
	fileSize := uint64(stat.Size())

	return c.ChunkReader(file, fileSize, fallbackURL)
}

func (c *Chunker) ChunkReader(reader io.Reader, size uint64, fallbackURL string) (*protocol.FileMetadata, error) {
	splitter := chunker.NewSizeSplitter(reader, int64(c.chunkSize))

	var chunks []protocol.ChunkInfo
	var totalSize uint64
	fileHasher := HashBytes
	index := uint32(0)

	var fileData []byte
	if size > 0 {
		fileData = make([]byte, 0, size)
	}

	for {
		chunkData, err := splitter.NextBytes()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read chunk %d: %w", index, err)
		}

		chunkHash := HashBytes(chunkData)

		fileData = append(fileData, chunkData...)

		chunkInfo := protocol.ChunkInfo{
			Hash:   chunkHash,
			Index:  index,
			Size:   uint32(len(chunkData)),
			Offset: totalSize,
		}
		chunks = append(chunks, chunkInfo)

		totalSize += uint64(len(chunkData))
		index++
	}

	fileHash := fileHasher(fileData)

	metadata := &protocol.FileMetadata{
		Hash:        fileHash,
		Size:        totalSize,
		ChunkSize:   c.chunkSize,
		ChunkCount:  uint32(len(chunks)),
		Chunks:      chunks,
		FallbackURL: fallbackURL,
		CreatedAt:   time.Now(),
	}

	return metadata, nil
}

func ChunkToCID(chunkHash protocol.FileHash) (cid.Cid, error) {
	mh, err := multihash.Encode(chunkHash[:], multihash.SHA2_256)
	if err != nil {
		return cid.Cid{}, fmt.Errorf("failed to create multihash: %w", err)
	}

	return cid.NewCidV1(cid.Raw, mh), nil
}

func (c *Chunker) ReassembleFile(
	ctx context.Context,
	outputPath string,
	metadata *protocol.FileMetadata,
	chunkReader func(chunkHash protocol.FileHash) ([]byte, error),
) error {
	outFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer outFile.Close()

	for _, chunkInfo := range metadata.Chunks {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		chunkData, err := chunkReader(chunkInfo.Hash)
		if err != nil {
			return fmt.Errorf("failed to read chunk %d: %w", chunkInfo.Index, err)
		}

		if !VerifyBytes(chunkData, chunkInfo.Hash) {
			return fmt.Errorf("chunk %d hash mismatch", chunkInfo.Index)
		}

		if uint32(len(chunkData)) != chunkInfo.Size {
			return fmt.Errorf("chunk %d size mismatch: expected %d, got %d",
				chunkInfo.Index, chunkInfo.Size, len(chunkData))
		}

		if _, err := outFile.Write(chunkData); err != nil {
			return fmt.Errorf("failed to write chunk %d: %w", chunkInfo.Index, err)
		}
	}

	if err := outFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}

	outFile.Close()
	ok, err := VerifyFile(outputPath, metadata.Hash)
	if err != nil {
		return fmt.Errorf("failed to verify file: %w", err)
	}
	if !ok {
		return fmt.Errorf("file hash mismatch after reassembly")
	}

	return nil
}

func GetChunkCIDs(metadata *protocol.FileMetadata) ([]cid.Cid, error) {
	cids := make([]cid.Cid, len(metadata.Chunks))
	for i, chunk := range metadata.Chunks {
		c, err := ChunkToCID(chunk.Hash)
		if err != nil {
			return nil, fmt.Errorf("failed to create CID for chunk %d: %w", i, err)
		}
		cids[i] = c
	}
	return cids, nil
}

func (c *Chunker) ReadChunk(filePath string, offset uint64, size uint32) ([]byte, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	if _, err := file.Seek(int64(offset), io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek to offset %d: %w", offset, err)
	}

	data := make([]byte, size)
	n, err := io.ReadFull(file, data)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return nil, fmt.Errorf("failed to read chunk: %w", err)
	}

	return data[:n], nil
}

func CreateBlock(data []byte, c cid.Cid) (blocks.Block, error) {
	return blocks.NewBlockWithCid(data, c)
}
