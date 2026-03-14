package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"ota-platform/internal/bootstrap"
	"ota-platform/internal/config"
	"github.com/idnteq/go-smsc/smpp"
)

type loadgenConfig struct {
	TargetHost     string
	TargetPort     int
	SystemID       string
	Password       string
	SourceAddr     string
	Binds          int
	Window         int
	RateTPS        int
	Messages       int
	PayloadSize    int
	RegisterDLR    bool
	SettleTimeout  time.Duration
	ArtifactsDir   string
	DeliverWorkers int
	DeliverQueue   int
}

type latencyStats struct {
	mu   sync.Mutex
	vals []time.Duration
}

func (s *latencyStats) Add(v time.Duration) {
	s.mu.Lock()
	s.vals = append(s.vals, v)
	s.mu.Unlock()
}

func (s *latencyStats) Snapshot() []time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]time.Duration, len(s.vals))
	copy(out, s.vals)
	return out
}

type resultSummary struct {
	StartedAt          time.Time `json:"started_at"`
	FinishedAt         time.Time `json:"finished_at"`
	TargetHost         string    `json:"target_host"`
	TargetPort         int       `json:"target_port"`
	Connections        int       `json:"connections"`
	WindowSize         int       `json:"window_size"`
	RateTPS            int       `json:"rate_tps"`
	MessagesRequested  int       `json:"messages_requested"`
	PayloadSize        int       `json:"payload_size"`
	RegisterDLR        bool      `json:"register_dlr"`
	SubmitAttempts     int64     `json:"submit_attempts"`
	SubmitSuccess      int64     `json:"submit_success"`
	SubmitFailures     int64     `json:"submit_failures"`
	DLRReceived        int64     `json:"dlr_received"`
	DLRUnknown         int64     `json:"dlr_unknown"`
	MOReceived         int64     `json:"mo_received"`
	SubmitTPS          float64   `json:"submit_tps"`
	DLRTPS             float64   `json:"dlr_tps"`
	SubmitLatencyP50Ms float64   `json:"submit_latency_p50_ms"`
	SubmitLatencyP95Ms float64   `json:"submit_latency_p95_ms"`
	SubmitLatencyP99Ms float64   `json:"submit_latency_p99_ms"`
	SubmitLatencyMaxMs float64   `json:"submit_latency_max_ms"`
	DLRLatencyP50Ms    float64   `json:"dlr_latency_p50_ms"`
	DLRLatencyP95Ms    float64   `json:"dlr_latency_p95_ms"`
	DLRLatencyP99Ms    float64   `json:"dlr_latency_p99_ms"`
	DLRLatencyMaxMs    float64   `json:"dlr_latency_max_ms"`
	ArtifactsDir       string    `json:"artifacts_dir"`
}

type loadGenerator struct {
	cfg          loadgenConfig
	logger       *zap.Logger
	pool         *smpp.Pool
	startedAt    time.Time
	submitLat    latencyStats
	dlrLat       latencyStats
	pendingStart sync.Map // msgID -> time.Time
	attempts     atomic.Int64
	success      atomic.Int64
	failures     atomic.Int64
	dlrReceived  atomic.Int64
	dlrUnknown   atomic.Int64
	moReceived   atomic.Int64
}

func main() {
	logger := bootstrap.NewLogger()
	ctx, cancel := bootstrap.SignalContext()
	defer cancel()

	cfg := loadConfig()
	if err := os.MkdirAll(cfg.ArtifactsDir, 0o755); err != nil {
		logger.Fatal("create artifacts dir failed", zap.Error(err))
	}

	lg := &loadGenerator{
		cfg:    cfg,
		logger: logger.Named("smpp-loadgen"),
	}

	if err := lg.run(ctx); err != nil {
		logger.Fatal("loadgen failed", zap.Error(err))
	}
}

func loadConfig() loadgenConfig {
	return loadgenConfig{
		TargetHost:     config.GetEnv("LOADGEN_TARGET_HOST", "central-sut"),
		TargetPort:     config.GetEnvInt("LOADGEN_TARGET_PORT", 2776),
		SystemID:       config.GetEnv("LOADGEN_SYSTEM_ID", "loadgen"),
		Password:       config.GetEnv("LOADGEN_PASSWORD", "password"),
		SourceAddr:     config.GetEnv("LOADGEN_SOURCE_ADDR", "LOADGEN"),
		Binds:          config.GetEnvInt("LOADGEN_BINDS", 16),
		Window:         config.GetEnvInt("LOADGEN_WINDOW", 200),
		RateTPS:        config.GetEnvInt("LOADGEN_RATE_TPS", 1000),
		Messages:       config.GetEnvInt("LOADGEN_MESSAGES", 100000),
		PayloadSize:    config.GetEnvInt("LOADGEN_PAYLOAD_SIZE", 32),
		RegisterDLR:    config.GetEnv("LOADGEN_REGISTER_DLR", "true") != "false",
		SettleTimeout:  time.Duration(config.GetEnvInt("LOADGEN_SETTLE_TIMEOUT_SEC", 30)) * time.Second,
		ArtifactsDir:   config.GetEnv("LOADGEN_ARTIFACTS_DIR", "/artifacts"),
		DeliverWorkers: config.GetEnvInt("LOADGEN_DELIVER_WORKERS", 16),
		DeliverQueue:   config.GetEnvInt("LOADGEN_DELIVER_QUEUE_SIZE", 10000),
	}
}

func (lg *loadGenerator) run(ctx context.Context) error {
	smppCfg := smpp.Config{
		Host:           lg.cfg.TargetHost,
		Port:           lg.cfg.TargetPort,
		SystemID:       lg.cfg.SystemID,
		Password:       lg.cfg.Password,
		SourceAddr:     lg.cfg.SourceAddr,
		SourceAddrTON:  0x05,
		SourceAddrNPI:  0x00,
		EnquireLinkSec: 30,
	}
	poolCfg := smpp.PoolConfig{
		Connections:      lg.cfg.Binds,
		WindowSize:       lg.cfg.Window,
		DeliverWorkers:   lg.cfg.DeliverWorkers,
		DeliverQueueSize: lg.cfg.DeliverQueue,
		SubmitTimeout:    60 * time.Second,
	}

	lg.pool = smpp.NewPool(smppCfg, poolCfg, lg.handleDeliver, lg.logger.Named("pool"))
	if err := lg.pool.Connect(ctx); err != nil {
		return err
	}
	defer lg.pool.Close()

	lg.startedAt = time.Now()
	lg.logger.Info("starting benchmark",
		zap.String("target_host", lg.cfg.TargetHost),
		zap.Int("target_port", lg.cfg.TargetPort),
		zap.Int("binds", lg.cfg.Binds),
		zap.Int("window", lg.cfg.Window),
		zap.Int("rate_tps", lg.cfg.RateTPS),
		zap.Int("messages", lg.cfg.Messages),
		zap.Bool("register_dlr", lg.cfg.RegisterDLR),
	)

	if err := lg.runSubmits(ctx); err != nil {
		return err
	}

	if lg.cfg.RegisterDLR {
		lg.waitForDLRs()
	}

	return lg.writeArtifacts(lg.buildSummary())
}

func (lg *loadGenerator) runSubmits(ctx context.Context) error {
	var ticker *time.Ticker
	if lg.cfg.RateTPS > 0 {
		interval := time.Second / time.Duration(lg.cfg.RateTPS)
		if interval <= 0 {
			interval = time.Nanosecond
		}
		ticker = time.NewTicker(interval)
		defer ticker.Stop()
	}

	payload := makePayload(lg.cfg.PayloadSize)
	for i := 0; i < lg.cfg.Messages; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if ticker != nil {
			select {
			case <-ticker.C:
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		dest := fmt.Sprintf("+2783%07d", i)
		lg.attempts.Add(1)
		start := time.Now()
		resp, err := lg.pool.Submit(&smpp.SubmitRequest{
			MSISDN:      dest,
			DestTON:     0x01,
			DestNPI:     0x01,
			ESMClass:    0x00,
			ProtocolID:  0x00,
			DataCoding:  0x00,
			Payload:     payload,
			RegisterDLR: lg.cfg.RegisterDLR,
		})
		if err != nil || resp == nil || resp.Error != nil {
			lg.failures.Add(1)
			continue
		}

		lg.success.Add(1)
		lg.submitLat.Add(time.Since(start))
		if lg.cfg.RegisterDLR && resp.MessageID != "" {
			lg.pendingStart.Store(resp.MessageID, start)
		}
	}

	return nil
}

func (lg *loadGenerator) handleDeliver(sourceAddr, destAddr string, esmClass byte, payload []byte) error {
	if smpp.IsDLR(esmClass) {
		receipt := smpp.ParseDLRReceipt(string(payload))
		if receipt == nil {
			lg.dlrUnknown.Add(1)
			return nil
		}
		if startAny, ok := lg.pendingStart.Load(receipt.MessageID); ok {
			lg.pendingStart.Delete(receipt.MessageID)
			lg.dlrLat.Add(time.Since(startAny.(time.Time)))
			lg.dlrReceived.Add(1)
		} else {
			lg.dlrUnknown.Add(1)
		}
		return nil
	}

	lg.moReceived.Add(1)
	return nil
}

func (lg *loadGenerator) waitForDLRs() {
	deadline := time.Now().Add(lg.cfg.SettleTimeout)
	for time.Now().Before(deadline) {
		if lg.dlrReceived.Load()+lg.failures.Load() >= lg.success.Load() {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func (lg *loadGenerator) buildSummary() resultSummary {
	finished := time.Now()
	submitVals := lg.submitLat.Snapshot()
	dlrVals := lg.dlrLat.Snapshot()

	totalSecs := finished.Sub(lg.startedAt).Seconds()
	submitTPS := 0.0
	dlrTPS := 0.0
	if totalSecs > 0 {
		submitTPS = float64(lg.success.Load()) / totalSecs
		dlrTPS = float64(lg.dlrReceived.Load()) / totalSecs
	}

	return resultSummary{
		StartedAt:          lg.startedAt,
		FinishedAt:         finished,
		TargetHost:         lg.cfg.TargetHost,
		TargetPort:         lg.cfg.TargetPort,
		Connections:        lg.cfg.Binds,
		WindowSize:         lg.cfg.Window,
		RateTPS:            lg.cfg.RateTPS,
		MessagesRequested:  lg.cfg.Messages,
		PayloadSize:        lg.cfg.PayloadSize,
		RegisterDLR:        lg.cfg.RegisterDLR,
		SubmitAttempts:     lg.attempts.Load(),
		SubmitSuccess:      lg.success.Load(),
		SubmitFailures:     lg.failures.Load(),
		DLRReceived:        lg.dlrReceived.Load(),
		DLRUnknown:         lg.dlrUnknown.Load(),
		MOReceived:         lg.moReceived.Load(),
		SubmitTPS:          submitTPS,
		DLRTPS:             dlrTPS,
		SubmitLatencyP50Ms: percentileMillis(submitVals, 0.50),
		SubmitLatencyP95Ms: percentileMillis(submitVals, 0.95),
		SubmitLatencyP99Ms: percentileMillis(submitVals, 0.99),
		SubmitLatencyMaxMs: maxMillis(submitVals),
		DLRLatencyP50Ms:    percentileMillis(dlrVals, 0.50),
		DLRLatencyP95Ms:    percentileMillis(dlrVals, 0.95),
		DLRLatencyP99Ms:    percentileMillis(dlrVals, 0.99),
		DLRLatencyMaxMs:    maxMillis(dlrVals),
		ArtifactsDir:       lg.cfg.ArtifactsDir,
	}
}

func (lg *loadGenerator) writeArtifacts(summary resultSummary) error {
	stamp := summary.FinishedAt.UTC().Format("20060102T150405Z")

	summaryPath := filepath.Join(lg.cfg.ArtifactsDir, fmt.Sprintf("summary-%s.json", stamp))
	data, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(summaryPath, data, 0o644); err != nil {
		return err
	}

	csvPath := filepath.Join(lg.cfg.ArtifactsDir, fmt.Sprintf("summary-%s.csv", stamp))
	f, err := os.Create(csvPath)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	header := []string{
		"started_at", "finished_at", "target_host", "target_port", "connections", "window_size",
		"rate_tps", "messages_requested", "payload_size", "register_dlr", "submit_attempts",
		"submit_success", "submit_failures", "dlr_received", "dlr_unknown", "mo_received",
		"submit_tps", "dlr_tps", "submit_latency_p50_ms", "submit_latency_p95_ms",
		"submit_latency_p99_ms", "submit_latency_max_ms", "dlr_latency_p50_ms",
		"dlr_latency_p95_ms", "dlr_latency_p99_ms", "dlr_latency_max_ms",
	}
	row := []string{
		summary.StartedAt.UTC().Format(time.RFC3339),
		summary.FinishedAt.UTC().Format(time.RFC3339),
		summary.TargetHost,
		fmt.Sprintf("%d", summary.TargetPort),
		fmt.Sprintf("%d", summary.Connections),
		fmt.Sprintf("%d", summary.WindowSize),
		fmt.Sprintf("%d", summary.RateTPS),
		fmt.Sprintf("%d", summary.MessagesRequested),
		fmt.Sprintf("%d", summary.PayloadSize),
		fmt.Sprintf("%t", summary.RegisterDLR),
		fmt.Sprintf("%d", summary.SubmitAttempts),
		fmt.Sprintf("%d", summary.SubmitSuccess),
		fmt.Sprintf("%d", summary.SubmitFailures),
		fmt.Sprintf("%d", summary.DLRReceived),
		fmt.Sprintf("%d", summary.DLRUnknown),
		fmt.Sprintf("%d", summary.MOReceived),
		fmt.Sprintf("%.2f", summary.SubmitTPS),
		fmt.Sprintf("%.2f", summary.DLRTPS),
		fmt.Sprintf("%.3f", summary.SubmitLatencyP50Ms),
		fmt.Sprintf("%.3f", summary.SubmitLatencyP95Ms),
		fmt.Sprintf("%.3f", summary.SubmitLatencyP99Ms),
		fmt.Sprintf("%.3f", summary.SubmitLatencyMaxMs),
		fmt.Sprintf("%.3f", summary.DLRLatencyP50Ms),
		fmt.Sprintf("%.3f", summary.DLRLatencyP95Ms),
		fmt.Sprintf("%.3f", summary.DLRLatencyP99Ms),
		fmt.Sprintf("%.3f", summary.DLRLatencyMaxMs),
	}
	if err := w.Write(header); err != nil {
		return err
	}
	if err := w.Write(row); err != nil {
		return err
	}

	lg.logger.Info("benchmark complete",
		zap.String("summary_json", summaryPath),
		zap.String("summary_csv", csvPath),
		zap.Int64("submit_success", summary.SubmitSuccess),
		zap.Int64("submit_failures", summary.SubmitFailures),
		zap.Int64("dlr_received", summary.DLRReceived),
		zap.Float64("submit_tps", summary.SubmitTPS),
	)
	return nil
}

func makePayload(size int) []byte {
	if size <= 0 {
		size = 1
	}
	out := make([]byte, size)
	for i := range out {
		out[i] = byte('A' + (i % 26))
	}
	return out
}

func percentileMillis(vals []time.Duration, p float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	cp := make([]time.Duration, len(vals))
	copy(cp, vals)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	idx := int(float64(len(cp)-1) * p)
	return float64(cp[idx].Microseconds()) / 1000.0
}

func maxMillis(vals []time.Duration) float64 {
	if len(vals) == 0 {
		return 0
	}
	max := vals[0]
	for _, v := range vals[1:] {
		if v > max {
			max = v
		}
	}
	return float64(max.Microseconds()) / 1000.0
}
