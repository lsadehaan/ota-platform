package smpp

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// PoolConfig configures the SMPP connection pool.
type PoolConfig struct {
	Connections    int           // number of SMPP connections (default 5)
	WindowSize     int           // max outstanding submits per connection (default 10)
	ReconnectDelay time.Duration // delay between reconnect attempts (default 5s)
}

// Pool manages multiple SMPP connections with windowed submits.
type Pool struct {
	config     Config
	poolConfig PoolConfig
	handler    DeliverHandler
	logger     *zap.Logger
	conns      []*poolConn
	nextConn   uint64 // round-robin counter
	mu         sync.RWMutex
	done       chan struct{}
}

type poolConn struct {
	client *Client
	window chan struct{} // buffered channel of size WindowSize
	index  int
}

// DefaultPoolConfig returns a PoolConfig with sensible defaults.
func DefaultPoolConfig() PoolConfig {
	return PoolConfig{
		Connections:    5,
		WindowSize:     10,
		ReconnectDelay: 5 * time.Second,
	}
}

// NewPool creates a new SMPP connection pool.
// If smppConfig is a single Config, it's replicated for all connections.
// The handler is shared across all connections (for DLR/MO dispatch).
func NewPool(smppConfig Config, poolConfig PoolConfig, handler DeliverHandler, logger *zap.Logger) *Pool {
	if poolConfig.Connections <= 0 {
		poolConfig.Connections = DefaultPoolConfig().Connections
	}
	if poolConfig.WindowSize <= 0 {
		poolConfig.WindowSize = DefaultPoolConfig().WindowSize
	}
	if poolConfig.ReconnectDelay <= 0 {
		poolConfig.ReconnectDelay = DefaultPoolConfig().ReconnectDelay
	}

	return &Pool{
		config:     smppConfig,
		poolConfig: poolConfig,
		handler:    handler,
		logger:     logger,
		done:       make(chan struct{}),
	}
}

// Connect creates poolConfig.Connections Client instances and connects each one
// with staggered delays to avoid thundering herd. It starts the background
// reconnect loop. Returns an error only if ALL connections fail.
func (p *Pool) Connect(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.conns = make([]*poolConn, p.poolConfig.Connections)
	successCount := 0

	for i := 0; i < p.poolConfig.Connections; i++ {
		// Stagger connection attempts to avoid thundering herd.
		if i > 0 {
			select {
			case <-ctx.Done():
				if successCount > 0 {
					go p.reconnectLoop()
					return nil
				}
				return ctx.Err()
			case <-time.After(200 * time.Millisecond):
			}
		}

		client := NewClient(p.config, p.handler, p.logger.With(zap.Int("conn_index", i)))
		pc := &poolConn{
			client: client,
			window: make(chan struct{}, p.poolConfig.WindowSize),
			index:  i,
		}
		p.conns[i] = pc

		if err := client.Connect(ctx); err != nil {
			p.logger.Warn("failed to connect pool member",
				zap.Int("index", i),
				zap.Error(err),
			)
			continue
		}

		successCount++
		p.logger.Info("pool connection established",
			zap.Int("index", i),
			zap.Int("total", successCount),
		)
	}

	if successCount == 0 {
		return fmt.Errorf("all %d pool connections failed", p.poolConfig.Connections)
	}

	go p.reconnectLoop()

	p.logger.Info("SMPP pool connected",
		zap.Int("active", successCount),
		zap.Int("total", p.poolConfig.Connections),
	)

	return nil
}

// Submit sends an SMS via a pool connection using round-robin selection with
// windowed back-pressure. It blocks if the selected connection's window is full.
func (p *Pool) Submit(req *SubmitRequest) (*SubmitResponse, error) {
	p.mu.RLock()
	conns := p.conns
	p.mu.RUnlock()

	if len(conns) == 0 {
		return nil, fmt.Errorf("pool not connected")
	}

	numConns := uint64(len(conns))

	deadline := time.Now().Add(5 * time.Second)
	for {
		// Try each connection starting from the round-robin position.
		start := atomic.AddUint64(&p.nextConn, 1)
		for i := uint64(0); i < numConns; i++ {
			idx := (start + i) % numConns
			conn := conns[idx]

			if !conn.client.IsBound() {
				continue
			}

			// Skip saturated connections instead of blocking on the first one,
			// otherwise a single full window creates head-of-line blocking.
			select {
			case conn.window <- struct{}{}:
				resp, err := conn.client.Submit(req)
				<-conn.window
				return resp, err
			default:
			}
		}

		if time.Now().After(deadline) {
			break
		}
		time.Sleep(1 * time.Millisecond)
	}

	return nil, fmt.Errorf("no bound connections with available window capacity")
}

// Close shuts down the pool: signals the reconnect loop to stop and closes
// all client connections. Returns the first error encountered.
func (p *Pool) Close() error {
	// Signal reconnect loop to stop.
	select {
	case <-p.done:
		// Already closed.
	default:
		close(p.done)
	}

	p.mu.RLock()
	conns := p.conns
	p.mu.RUnlock()

	var firstErr error
	for _, conn := range conns {
		if err := conn.client.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

// reconnectLoop runs in the background, periodically checking each connection
// and attempting to reconnect any that have become unbound.
func (p *Pool) reconnectLoop() {
	ticker := time.NewTicker(p.poolConfig.ReconnectDelay)
	defer ticker.Stop()

	for {
		select {
		case <-p.done:
			return
		case <-ticker.C:
			p.mu.RLock()
			conns := p.conns
			p.mu.RUnlock()

			for _, conn := range conns {
				if conn.client.IsBound() {
					continue
				}

				p.logger.Info("attempting to reconnect pool member",
					zap.Int("index", conn.index),
				)

				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				if err := conn.client.Connect(ctx); err != nil {
					p.logger.Warn("reconnect failed",
						zap.Int("index", conn.index),
						zap.Error(err),
					)
				} else {
					p.logger.Info("reconnect successful",
						zap.Int("index", conn.index),
					)
				}
				cancel()
			}
		}
	}
}

// ActiveConnections returns the count of currently bound connections in the pool.
func (p *Pool) ActiveConnections() int {
	p.mu.RLock()
	conns := p.conns
	p.mu.RUnlock()

	count := 0
	for _, conn := range conns {
		if conn.client.IsBound() {
			count++
		}
	}
	return count
}

// WindowUtilization returns the ratio of used window slots to total window
// capacity across all connections. Useful for observability and metrics.
func (p *Pool) WindowUtilization() float64 {
	p.mu.RLock()
	conns := p.conns
	p.mu.RUnlock()

	if len(conns) == 0 {
		return 0
	}

	totalCapacity := 0
	totalUsed := 0
	for _, conn := range conns {
		cap := cap(conn.window)
		totalCapacity += cap
		totalUsed += len(conn.window)
	}

	if totalCapacity == 0 {
		return 0
	}

	return float64(totalUsed) / float64(totalCapacity)
}
