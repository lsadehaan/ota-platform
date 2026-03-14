// Package main provides a Go-based SMPP load test for the SMSC Gateway.
//
// It connects multiple SMPP clients to the gateway in parallel, submits
// messages at maximum throughput, and measures submit TPS, DLR receipt
// rate, and end-to-end latency.
//
// Usage:
//
//	go run ./tests/smsc-gateway/loadtest \
//	  -host 127.0.0.1 -port 2776 \
//	  -conns 4 -messages 5000
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/idnteq/go-smsc/smpp"
)

func main() {
	host := flag.String("host", "127.0.0.1", "gateway host")
	port := flag.Int("port", 2776, "gateway SMPP port")
	conns := flag.Int("conns", 4, "number of parallel SMPP connections")
	messages := flag.Int("messages", 5000, "total messages to submit")
	windowSize := flag.Int("window", 100, "per-connection window size")
	systemID := flag.String("system-id", "loadtest", "SMPP system_id")
	password := flag.String("password", "password", "SMPP password")
	flag.Parse()

	logger, _ := zap.NewDevelopment()
	defer logger.Sync()

	fmt.Printf("\n╔══════════════════════════════════════════════════════╗\n")
	fmt.Printf("║            SMSC Gateway Load Test                    ║\n")
	fmt.Printf("╠══════════════════════════════════════════════════════╣\n")
	fmt.Printf("║  Target:      %s:%d                        ║\n", *host, *port)
	fmt.Printf("║  Connections: %-5d  Window: %-5d                   ║\n", *conns, *windowSize)
	fmt.Printf("║  Messages:    %-5d                                  ║\n", *messages)
	fmt.Printf("╚══════════════════════════════════════════════════════╝\n\n")

	// Counters.
	var submitted atomic.Int64
	var dlrReceived atomic.Int64
	var submitErrors atomic.Int64

	// DLR handler — just count them.
	dlrHandler := func(sourceAddr, destAddr string, esmClass byte, payload []byte) error {
		dlrReceived.Add(1)
		return nil
	}

	// Create pool.
	smppCfg := smpp.Config{
		Host:           *host,
		Port:           *port,
		SystemID:       *systemID,
		Password:       *password,
		SourceAddr:     "LOADTEST",
		SourceAddrTON:  0x05,
		SourceAddrNPI:  0x00,
		EnquireLinkSec: 30,
	}
	poolCfg := smpp.PoolConfig{
		Connections:      *conns,
		WindowSize:       *windowSize,
		DeliverWorkers:   16,
		DeliverQueueSize: 50000,
		SubmitTimeout:    30 * time.Second,
	}

	pool := smpp.NewPool(smppCfg, poolCfg, dlrHandler, logger.Named("pool"))
	ctx := context.Background()

	if err := pool.Connect(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect: %v\n", err)
		os.Exit(1)
	}
	defer pool.Close()

	fmt.Printf("Connected %d SMPP clients to %s:%d\n\n", pool.ActiveConnections(), *host, *port)

	// Submit phase.
	total := *messages
	perWorker := total / *conns
	remainder := total % *conns

	start := time.Now()
	var wg sync.WaitGroup

	for i := 0; i < *conns; i++ {
		count := perWorker
		if i < remainder {
			count++
		}
		wg.Add(1)
		go func(workerID, count int) {
			defer wg.Done()
			for j := 0; j < count; j++ {
				msisdn := fmt.Sprintf("+2783%03d%04d", workerID, j)
				req := &smpp.SubmitRequest{
					MSISDN:      msisdn,
					DestTON:     0x01,
					DestNPI:     0x01,
					DataCoding:  0x00,
					Payload:     []byte(fmt.Sprintf("Load test msg %d-%d", workerID, j)),
					RegisterDLR: true,
				}
				_, err := pool.Submit(req)
				if err != nil {
					submitErrors.Add(1)
				} else {
					submitted.Add(1)
				}
			}
		}(i, count)
	}

	// Progress reporter.
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				elapsed := time.Since(start).Seconds()
				sub := submitted.Load()
				dlr := dlrReceived.Load()
				tps := float64(sub) / elapsed
				fmt.Printf("  [%5.1fs] submitted=%d (%.0f TPS)  DLRs=%d  errors=%d\n",
					elapsed, sub, tps, dlr, submitErrors.Load())
			case <-done:
				return
			}
		}
	}()

	wg.Wait()
	submitElapsed := time.Since(start)
	close(done)

	submitTPS := float64(submitted.Load()) / submitElapsed.Seconds()
	fmt.Printf("\n── Submit Phase Complete ──────────────────────────────\n")
	fmt.Printf("  Submitted: %d in %s (%.0f TPS)\n", submitted.Load(), submitElapsed.Round(time.Millisecond), submitTPS)
	fmt.Printf("  Errors:    %d\n", submitErrors.Load())

	// Wait for DLRs (mock-smsc has 500ms DLR delay).
	fmt.Printf("\n── Waiting for DLRs ──────────────────────────────────\n")
	dlrDeadline := time.Now().Add(60 * time.Second)
	for dlrReceived.Load() < submitted.Load() && time.Now().Before(dlrDeadline) {
		time.Sleep(100 * time.Millisecond)
		if time.Now().Unix()%2 == 0 {
			fmt.Printf("  DLRs: %d / %d\n", dlrReceived.Load(), submitted.Load())
		}
	}

	totalElapsed := time.Since(start)
	endToEndTPS := float64(submitted.Load()) / totalElapsed.Seconds()

	fmt.Printf("\n╔══════════════════════════════════════════════════════╗\n")
	fmt.Printf("║                     RESULTS                          ║\n")
	fmt.Printf("╠══════════════════════════════════════════════════════╣\n")
	fmt.Printf("║  Messages submitted:   %-6d                        ║\n", submitted.Load())
	fmt.Printf("║  Submit errors:        %-6d                        ║\n", submitErrors.Load())
	fmt.Printf("║  DLRs received:        %-6d                        ║\n", dlrReceived.Load())
	fmt.Printf("║  Submit time:          %-10s                    ║\n", submitElapsed.Round(time.Millisecond))
	fmt.Printf("║  Total time (w/ DLR):  %-10s                    ║\n", totalElapsed.Round(time.Millisecond))
	fmt.Printf("║  Submit TPS:           %-6.0f                        ║\n", submitTPS)
	fmt.Printf("║  End-to-end TPS:       %-6.0f                        ║\n", endToEndTPS)
	fmt.Printf("║  DLR rate:             %-5.1f%%                       ║\n", float64(dlrReceived.Load())/float64(submitted.Load())*100)
	fmt.Printf("╚══════════════════════════════════════════════════════╝\n")
}
