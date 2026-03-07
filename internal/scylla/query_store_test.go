package scylla

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestDayBucketsInclusive(t *testing.T) {
	from := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	to := time.Date(2026, 3, 3, 1, 0, 0, 0, time.UTC)

	got := dayBuckets(from, to)
	want := []string{"2026-03-01", "2026-03-02", "2026-03-03"}
	if len(got) != len(want) {
		t.Fatalf("bucket len mismatch: got %d want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("bucket[%d] = %q want %q", i, got[i], want[i])
		}
	}
}

func TestDayBucketsDesc(t *testing.T) {
	from := time.Date(2026, 3, 5, 10, 0, 0, 0, time.UTC)
	to := time.Date(2026, 3, 7, 1, 0, 0, 0, time.UTC)

	got := dayBucketsDesc(from, to)
	want := []string{"2026-03-07", "2026-03-06", "2026-03-05"}
	if len(got) != len(want) {
		t.Fatalf("expected %d days, got %d", len(want), len(got))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("day %d: expected %s, got %s", i, want[i], got[i])
		}
	}
}

func TestCountsFromMapSortsByCountThenKey(t *testing.T) {
	got := countsFromMap(map[string]int64{
		"b": 2,
		"a": 2,
		"c": 5,
	})
	if len(got) != 3 {
		t.Fatalf("unexpected count length: %d", len(got))
	}
	if got[0].Key != "c" || got[0].Count != 5 {
		t.Fatalf("unexpected first entry: %#v", got[0])
	}
	if got[1].Key != "a" || got[2].Key != "b" {
		t.Fatalf("expected tie-break by key ordering, got %#v", got)
	}
}

func TestFilterMessagesMatchesStatusOrDLRStatus(t *testing.T) {
	dlr := "UNDELIV"
	in := []MessageRecord{
		{ID: uuid.New(), Status: "sent", DLRStatus: &dlr, CreatedAt: time.Now().UTC()},
		{ID: uuid.New(), Status: "failed", CreatedAt: time.Now().UTC()},
	}
	got := filterMessages(in, MessageFilter{
		Status: "UNDELIV",
		From:   time.Now().UTC().Add(-time.Hour),
		To:     time.Now().UTC().Add(time.Hour),
	})
	if len(got) != 1 || got[0].DLRStatus == nil || *got[0].DLRStatus != "UNDELIV" {
		t.Fatalf("unexpected filtered result: %#v", got)
	}
}

func TestMessageCollectorKeepsNewestPageWindow(t *testing.T) {
	collector := newMessageCollector(4)
	base := time.Date(2026, 3, 7, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 6; i++ {
		collector.AddAll([]MessageRecord{{
			ID:        uuid.New(),
			CardID:    uuid.New(),
			Status:    "sent",
			Direction: "MT",
			CreatedAt: base.Add(time.Duration(i) * time.Minute),
			UpdatedAt: base.Add(time.Duration(i) * time.Minute),
		}}, MessageFilter{
			From: base.Add(-time.Hour),
			To:   base.Add(time.Hour),
		})
	}

	if collector.Total() != 6 {
		t.Fatalf("expected total 6, got %d", collector.Total())
	}

	page := collector.Page(2, 2)
	if len(page) != 2 {
		t.Fatalf("expected page length 2, got %d", len(page))
	}
	if !page[0].CreatedAt.Equal(base.Add(3*time.Minute)) || !page[1].CreatedAt.Equal(base.Add(2*time.Minute)) {
		t.Fatalf("unexpected page contents: %+v", page)
	}
}

func TestIsFailedDLR(t *testing.T) {
	if !IsFailedDLR("FAILED") {
		t.Fatal("expected FAILED to be treated as failed")
	}
	if IsFailedDLR("DELIVRD") {
		t.Fatal("did not expect DELIVRD to be treated as failed")
	}
}

func TestAggregateMessageMetrics(t *testing.T) {
	got := aggregateMessageMetrics([]hourlyMessageMetrics{
		{Total: 3, MT: 2, MO: 1, Delivered: 1, Undelivered: 1},
		{Total: 5, MT: 4, MO: 1, Delivered: 3, Undelivered: 0},
	})
	if got.Total != 8 || got.MT != 6 || got.MO != 2 || got.Delivered != 4 || got.Undelivered != 1 {
		t.Fatalf("unexpected aggregate metrics: %#v", got)
	}
}

func TestThroughputFromHourlyMetricsSortsAscending(t *testing.T) {
	later := time.Date(2026, 3, 7, 13, 0, 0, 0, time.UTC)
	earlier := later.Add(-time.Hour)
	points := throughputFromHourlyMetrics([]hourlyMessageMetrics{
		{Timestamp: later, MT: 7, Delivered: 6, Undelivered: 1},
		{Timestamp: earlier, MT: 4, Delivered: 3, Undelivered: 0},
	})
	if len(points) != 2 {
		t.Fatalf("unexpected throughput length: %d", len(points))
	}
	if !points[0].Timestamp.Equal(earlier) || !points[1].Timestamp.Equal(later) {
		t.Fatalf("unexpected throughput ordering: %#v", points)
	}
}
