#!/usr/bin/env bash
#
# SMSC Gateway integration test runner.
#
# Usage:
#   ./run_tests.sh                     # Default: mock-smsc downstream
#   ./run_tests.sh smppsim             # Use SMPPSim downstream
#   ./run_tests.sh mocksmsc --keep     # Keep stack running after tests
#   ./run_tests.sh chaos               # mock-smsc + Toxiproxy fault injection
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_DIR="$REPO_ROOT/deployments/smsc-matrix"

PROFILE="${1:-mocksmsc}"
KEEP_STACK=false

# Parse flags
for arg in "$@"; do
    case "$arg" in
        --keep) KEEP_STACK=true ;;
    esac
done

echo "=== SMSC Gateway Integration Tests ==="
echo "Profile:  $PROFILE"
echo "Compose:  $COMPOSE_DIR"
echo ""

# Build compose file list.
COMPOSE_FILES="-f $COMPOSE_DIR/compose.base.yml"

case "$PROFILE" in
    mocksmsc)
        COMPOSE_FILES="$COMPOSE_FILES -f $COMPOSE_DIR/compose.mocksmsc.yml"
        ;;
    smppsim)
        COMPOSE_FILES="$COMPOSE_FILES -f $COMPOSE_DIR/compose.smppsim.yml"
        ;;
    chaos)
        COMPOSE_FILES="$COMPOSE_FILES -f $COMPOSE_DIR/compose.mocksmsc.yml"
        COMPOSE_FILES="$COMPOSE_FILES -f $COMPOSE_DIR/compose.chaos.yml"
        ;;
    *)
        echo "Unknown profile: $PROFILE"
        echo "Valid profiles: mocksmsc, smppsim, chaos"
        exit 1
        ;;
esac

PROJECT_NAME="smscgw-test-${PROFILE}"

cleanup() {
    if [ "$KEEP_STACK" = false ]; then
        echo ""
        echo "=== Tearing down stack ==="
        docker compose -p "$PROJECT_NAME" $COMPOSE_FILES down -v --remove-orphans 2>/dev/null || true
    else
        echo ""
        echo "=== Stack left running (--keep) ==="
        echo "To tear down: docker compose -p $PROJECT_NAME $COMPOSE_FILES down -v"
    fi
}
trap cleanup EXIT

# 1. Build and start the stack.
echo "=== Building and starting stack ==="
docker compose -p "$PROJECT_NAME" $COMPOSE_FILES build
docker compose -p "$PROJECT_NAME" $COMPOSE_FILES up -d

# 2. Wait for smsc-gateway to be healthy.
echo ""
echo "=== Waiting for smsc-gateway to be healthy ==="
RETRIES=0
MAX_RETRIES=30
until docker compose -p "$PROJECT_NAME" $COMPOSE_FILES ps smsc-gateway | grep -q "healthy"; do
    RETRIES=$((RETRIES + 1))
    if [ "$RETRIES" -ge "$MAX_RETRIES" ]; then
        echo "ERROR: smsc-gateway did not become healthy within ${MAX_RETRIES} attempts"
        echo ""
        echo "=== Gateway logs ==="
        docker compose -p "$PROJECT_NAME" $COMPOSE_FILES logs smsc-gateway
        exit 1
    fi
    echo "  Waiting... ($RETRIES/$MAX_RETRIES)"
    sleep 2
done
echo "  smsc-gateway is healthy"

# 3. Install Python dependencies.
echo ""
echo "=== Installing Python dependencies ==="
pip install -q -r "$SCRIPT_DIR/requirements.txt"

# 4. Run pytest.
echo ""
echo "=== Running tests ==="
GW_HOST=127.0.0.1 GW_PORT=2776 GW_PASSWORD=password \
    pytest "$SCRIPT_DIR" -v --tb=short -x; TEST_EXIT=$?

echo ""
if [ "$TEST_EXIT" -eq 0 ]; then
    echo "=== ALL TESTS PASSED ==="
else
    echo "=== TESTS FAILED (exit code: $TEST_EXIT) ==="
    echo ""
    echo "=== Gateway logs ==="
    docker compose -p "$PROJECT_NAME" $COMPOSE_FILES logs --tail=50 smsc-gateway
fi

exit "$TEST_EXIT"
