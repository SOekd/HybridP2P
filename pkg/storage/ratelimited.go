package storage

import (
	"context"
	"sync"
	"time"

	"P2P-CDN/internal/logger"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

type RateLimitedBlockstore struct {
	*FileBlockstore
	mu      sync.RWMutex
	limiter *rate.Limiter
	onServe func(cid.Cid, int)
}

func NewRateLimitedBlockstore(bs *FileBlockstore) *RateLimitedBlockstore {
	return &RateLimitedBlockstore{FileBlockstore: bs}
}

func (r *RateLimitedBlockstore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	t0 := time.Now()
	blk, err := r.FileBlockstore.Get(ctx, c)
	if readMs := time.Since(t0).Milliseconds(); readMs > 10 {
		logger.Warn("slow blockstore read", zap.Int64("ms", readMs), zap.String("cid", c.String()))
	}
	if err != nil {
		return nil, err
	}

	r.mu.RLock()
	l := r.limiter
	cb := r.onServe
	r.mu.RUnlock()

	n := len(blk.RawData())

	if cb != nil && n > 0 {
		cb(c, n)
	}

	if l != nil && n > 0 {
		if err := l.WaitN(ctx, n); err != nil {
			return nil, err
		}
	}

	return blk, nil
}

func (r *RateLimitedBlockstore) SetServeCallback(fn func(cid.Cid, int)) {
	r.mu.Lock()
	r.onServe = fn
	r.mu.Unlock()
}

func (r *RateLimitedBlockstore) SetBandwidth(bps int64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if bps <= 0 {
		r.limiter = nil
		return
	}

	const minBurst = 2 * 1024 * 1024
	burst := int(bps)
	if burst < minBurst {
		burst = minBurst
	}

	r.limiter = rate.NewLimiter(rate.Limit(bps), burst)
}
