package p2p

import (
	"context"
	"fmt"

	"P2P-CDN/internal/logger"
	"P2P-CDN/pkg/protocol"

	"github.com/ipfs/boxo/bitswap"
	bsclient "github.com/ipfs/boxo/bitswap/client"
	"github.com/ipfs/boxo/bitswap/network"
	"github.com/ipfs/boxo/bitswap/network/bsnet"
	bsserver "github.com/ipfs/boxo/bitswap/server"
	blockstore "github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/host"
	"go.uber.org/zap"
)

type BitSwapExchange struct {
	bitswap    *bitswap.Bitswap
	blockstore blockstore.Blockstore
	network    network.BitSwapNetwork
}

func NewBitSwapExchange(ctx context.Context, h host.Host, bs blockstore.Blockstore) (*BitSwapExchange, error) {
	net := bsnet.NewFromIpfsHost(h, bsnet.Prefix(protocol.BitSwapProtocol))

	bs2 := bitswap.New(ctx, net, nil, bs,
		// Server options (serving blocks to others)
		bitswap.WithServerOption(bsserver.MaxOutstandingBytesPerPeer(64*1024*1024)),
		bitswap.WithServerOption(bsserver.EngineBlockstoreWorkerCount(256)),

		// Client options (downloading blocks)
		bitswap.WithClientOption(bsclient.ProviderSearchDelay(0)),
		bitswap.WithClientOption(bsclient.WithoutDuplicatedBlockStats()),
	)

	logger.Info("bitswap exchange created",
		zap.String("peer_id", h.ID().String()),
		zap.Int("max_outstanding_bytes_per_peer", 64*1024*1024),
		zap.Int("engine_blockstore_worker_count", 256))

	return &BitSwapExchange{
		bitswap:    bs2,
		blockstore: bs,
		network:    net,
	}, nil
}

func (b *BitSwapExchange) GetBlock(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	logger.Debug("requesting block via bitswap", zap.String("cid", c.String()))

	block, err := b.bitswap.GetBlock(ctx, c)
	if err != nil {
		logger.Warn("failed to get block",
			zap.String("cid", c.String()),
			zap.Error(err))
		return nil, fmt.Errorf("failed to get block: %w", err)
	}

	logger.Debug("block received",
		zap.String("cid", c.String()),
		zap.Int("size", len(block.RawData())))

	return block, nil
}

func (b *BitSwapExchange) GetBlocks(ctx context.Context, cids []cid.Cid) (<-chan blocks.Block, error) {
	logger.Info("requesting blocks via bitswap", zap.Int("count", len(cids)))

	blockCh, err := b.bitswap.GetBlocks(ctx, cids)
	if err != nil {
		return nil, fmt.Errorf("failed to get blocks: %w", err)
	}

	return blockCh, nil
}

func (b *BitSwapExchange) HasBlock(ctx context.Context, c cid.Cid) (bool, error) {
	return b.blockstore.Has(ctx, c)
}

func (b *BitSwapExchange) PutBlock(ctx context.Context, block blocks.Block) error {
	if err := b.blockstore.Put(ctx, block); err != nil {
		return fmt.Errorf("failed to put block in blockstore: %w", err)
	}

	logger.Debug("block stored",
		zap.String("cid", block.Cid().String()),
		zap.Int("size", len(block.RawData())))

	return nil
}

func (b *BitSwapExchange) PutBlocks(ctx context.Context, blks []blocks.Block) error {
	if len(blks) == 0 {
		return nil
	}

	if err := b.blockstore.PutMany(ctx, blks); err != nil {
		return fmt.Errorf("failed to put blocks in blockstore: %w", err)
	}

	logger.Info("blocks stored", zap.Int("count", len(blks)))

	return nil
}

func (b *BitSwapExchange) Close() error {
	logger.Info("closing bitswap exchange")
	return b.bitswap.Close()
}

type BitSwapStats struct {
	BlocksReceived  uint64
	DupBlksReceived uint64
	DupDataReceived uint64
}

func (b *BitSwapExchange) GetStats() *BitSwapStats {
	stat, err := b.bitswap.Stat()
	if err != nil {
		logger.Warn("failed to get bitswap stats", zap.Error(err))
		return &BitSwapStats{}
	}

	return &BitSwapStats{
		BlocksReceived:  stat.BlocksReceived,
		DupBlksReceived: stat.DupBlksReceived,
		DupDataReceived: stat.DupDataReceived,
	}
}
