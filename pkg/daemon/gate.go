package daemon

import (
	"context"
	"sync"
)

type Gate struct {
	mu       sync.Mutex
	paused   bool
	resumeCh chan struct{}
}

func NewGate() *Gate {
	ch := make(chan struct{})
	close(ch)
	return &Gate{paused: false, resumeCh: ch}
}

func (g *Gate) Wait(ctx context.Context) error {
	g.mu.Lock()
	ch := g.resumeCh
	g.mu.Unlock()

	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (g *Gate) Pause() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.paused {
		g.paused = true
		g.resumeCh = make(chan struct{})
	}
}

func (g *Gate) Resume() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.paused {
		g.paused = false
		close(g.resumeCh)
	}
}

func (g *Gate) IsPaused() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.paused
}
