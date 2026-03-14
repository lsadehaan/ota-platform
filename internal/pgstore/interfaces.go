package pgstore

import (
	"ota-platform/internal/controlplane"
	"ota-platform/internal/executor"
	"ota-platform/internal/transport"
)

// Compile-time interface satisfaction checks.
var (
	_ executor.CoordinationStore     = (*CoordinationStore)(nil)
	_ executor.CounterStore          = (*CounterStore)(nil)
	_ executor.ExecutionStore        = (*ExecutionStore)(nil)
	_ controlplane.QueryStore        = (*QueryStore)(nil)
	_ controlplane.CardKeyWriter     = (*CardKeyStore)(nil)
	_ controlplane.CounterReader     = (*CardKeyStore)(nil)
	_ controlplane.CoordinationStore = (*CoordinationStore)(nil)
	_ transport.CoordinationStore    = (*GatewayCoordinationStore)(nil)
	_ transport.MSISDNResolver       = (*MSISDNResolver)(nil)
)
