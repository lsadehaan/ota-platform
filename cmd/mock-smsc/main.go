package main

import (
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"go.uber.org/zap"

	"ota-platform/internal/mocksmsc"
	"ota-platform/pkg/hexutil"
)

func main() {
	// 1. Initialize logger.
	logger, err := zap.NewDevelopment()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	// 2. Read config from environment.
	port := 2775
	if v := os.Getenv("SMSC_PORT"); v != "" {
		parsed, err := strconv.Atoi(v)
		if err != nil {
			logger.Fatal("invalid SMSC_PORT", zap.String("value", v), zap.Error(err))
		}
		port = parsed
	}

	dlrDelayMs := 1000
	if v := os.Getenv("DLR_DELAY_MS"); v != "" {
		parsed, err := strconv.Atoi(v)
		if err != nil {
			logger.Fatal("invalid DLR_DELAY_MS", zap.String("value", v), zap.Error(err))
		}
		dlrDelayMs = parsed
	}

	dlrSuccessRate := 0.95
	if v := os.Getenv("DLR_SUCCESS_RATE"); v != "" {
		parsed, err := strconv.ParseFloat(v, 64)
		if err != nil {
			logger.Fatal("invalid DLR_SUCCESS_RATE", zap.String("value", v), zap.Error(err))
		}
		dlrSuccessRate = parsed
	}

	enableMO := false
	if v := os.Getenv("ENABLE_MO"); v == "true" || v == "1" {
		enableMO = true
	}

	moDelayMs := 100
	if v := os.Getenv("MO_DELAY_MS"); v != "" {
		parsed, err := strconv.Atoi(v)
		if err != nil {
			logger.Fatal("invalid MO_DELAY_MS", zap.String("value", v), zap.Error(err))
		}
		moDelayMs = parsed
	}

	var moPayload []byte
	if v := os.Getenv("MO_PAYLOAD_HEX"); v != "" {
		decoded, err := hexutil.Decode(v)
		if err != nil {
			logger.Fatal("invalid MO_PAYLOAD_HEX", zap.String("value", v), zap.Error(err))
		}
		moPayload = decoded
	}

	config := mocksmsc.Config{
		Port:           port,
		DLRDelayMs:     dlrDelayMs,
		DLRSuccessRate: dlrSuccessRate,
		EnableMO:       enableMO,
		MODelayMs:      moDelayMs,
		MOPayload:      moPayload,
	}

	// 3. Create and start mock SMSC server.
	server := mocksmsc.NewServer(config, logger)
	if err := server.Start(); err != nil {
		logger.Fatal("failed to start mock SMSC server", zap.Error(err))
	}

	logger.Info("mock SMSC server started",
		zap.Int("port", port),
		zap.Int("dlr_delay_ms", dlrDelayMs),
		zap.Float64("dlr_success_rate", dlrSuccessRate),
		zap.Bool("enable_mo", enableMO),
		zap.Int("mo_delay_ms", moDelayMs),
		zap.Int("mo_payload_len", len(moPayload)),
	)

	// 4. Wait for shutdown signal.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	logger.Info("received shutdown signal", zap.String("signal", sig.String()))

	server.Stop()
	logger.Info("mock SMSC server shut down gracefully")
}
