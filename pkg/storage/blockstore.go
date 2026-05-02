package storage

import (
	"context"
	"errors"

	blockstore "github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
)

var (
	ErrBlockNotFound = errors.New("block not found")
)

type Blockstore interface {
	Get(ctx context.Context, c cid.Cid) (blocks.Block, error)

	Put(ctx context.Context, block blocks.Block) error

	PutMany(ctx context.Context, blocks []blocks.Block) error

	Has(ctx context.Context, c cid.Cid) (bool, error)

	GetSize(ctx context.Context, c cid.Cid) (int, error)

	Delete(ctx context.Context, c cid.Cid) error

	DeleteMany(ctx context.Context, cids []cid.Cid) error

	AllKeysChan(ctx context.Context) (<-chan cid.Cid, error)

	HashOnRead(enabled bool)

	Close() error
}

var _ blockstore.Blockstore = (*FileBlockstore)(nil)
