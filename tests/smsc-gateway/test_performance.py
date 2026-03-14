"""Performance / load tests (L01, L04).

These tests measure gateway throughput under sustained and burst load.
They are skipped unless the PERF_TEST environment variable is set.

Usage:
    PERF_TEST=1 GW_HOST=127.0.0.1 GW_PORT=2776 pytest tests/smsc-gateway/test_performance.py -v
"""

import os
import time
import pytest
from conftest import ESMEClient, GW_HOST, GW_PORT, GW_PASSWORD

skip_no_perf = pytest.mark.skipif(
    not os.environ.get("PERF_TEST"),
    reason="PERF_TEST env var not set — performance tests skipped",
)


@skip_no_perf
class TestSteadyRate:
    """L01: Sustained 100 TPS for 10 seconds."""

    def test_l01_100tps_steady(self, esme):
        """L01: Submit 1000 messages at ~100 TPS — all should get DLRs."""
        count = 1000
        target_tps = 100
        interval = 1.0 / target_tps

        msg_ids = []
        start = time.time()

        for i in range(count):
            iter_start = time.time()
            resp = esme.submit_sm(
                source_addr="GATEWAY",
                dest_addr=f"+2783{i:07d}",
                short_message=f"L01 msg {i}".encode(),
            )
            mid = resp.message_id.decode() if isinstance(resp.message_id, bytes) else resp.message_id
            assert mid.startswith("GW-"), f"Expected GW-* ID, got: {mid}"
            msg_ids.append(mid)

            # Pace to target TPS.
            elapsed = time.time() - iter_start
            if elapsed < interval:
                time.sleep(interval - elapsed)

        submit_elapsed = time.time() - start
        actual_tps = count / submit_elapsed
        print(f"\nL01: Submitted {count} messages in {submit_elapsed:.1f}s ({actual_tps:.0f} TPS)")

        # Wait for all DLRs. Allow generous timeout for downstream latency.
        dlrs = esme.wait_for_deliver(timeout=120, count=count)
        total_elapsed = time.time() - start

        print(f"L01: Received {len(dlrs)} DLRs in {total_elapsed:.1f}s total")
        assert len(dlrs) >= count, f"Expected {count} DLRs, got {len(dlrs)}"


@skip_no_perf
class TestBurstSubmit:
    """L04: Burst 500 submits as fast as possible."""

    def test_l04_burst_500(self, esme):
        """L04: Submit 500 messages without pacing — all should get DLRs."""
        count = 500
        msg_ids = []

        start = time.time()

        for i in range(count):
            resp = esme.submit_sm(
                source_addr="GATEWAY",
                dest_addr=f"+2784{i:07d}",
                short_message=f"L04 burst {i}".encode(),
            )
            mid = resp.message_id.decode() if isinstance(resp.message_id, bytes) else resp.message_id
            assert mid.startswith("GW-"), f"Expected GW-* ID, got: {mid}"
            msg_ids.append(mid)

        submit_elapsed = time.time() - start
        actual_tps = count / submit_elapsed if submit_elapsed > 0 else float("inf")
        print(f"\nL04: Burst-submitted {count} messages in {submit_elapsed:.2f}s ({actual_tps:.0f} TPS)")

        # Wait for all DLRs.
        dlrs = esme.wait_for_deliver(timeout=120, count=count)
        total_elapsed = time.time() - start

        print(f"L04: Received {len(dlrs)} DLRs in {total_elapsed:.1f}s total")
        assert len(dlrs) >= count, f"Expected {count} DLRs, got {len(dlrs)}"
