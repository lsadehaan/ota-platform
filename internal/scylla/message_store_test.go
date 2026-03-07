package scylla

import (
	"encoding/json"
	"testing"
	"time"
)

func TestBuildMessageUpdateSetIncludesDLRStatusAndUpdatedAt(t *testing.T) {
	setClause, values := buildMessageUpdateSet(map[string]interface{}{
		"dlr_status": "DELIVRD",
		"status":     "sent",
	}, time.Unix(123, 0).UTC(), map[string]string{
		"dlr_status": "dlr_status",
		"status":     "status",
	})

	if setClause == "" {
		t.Fatal("expected non-empty set clause")
	}
	if len(values) != 3 {
		t.Fatalf("expected 3 values, got %d", len(values))
	}
	if setClause != "dlr_status = ?, status = ?, updated_at = ?" && setClause != "status = ?, dlr_status = ?, updated_at = ?" {
		t.Fatalf("unexpected set clause: %s", setClause)
	}
}

func TestBuildMessageUpdateSetDropsUnknownFields(t *testing.T) {
	setClause, values := buildMessageUpdateSet(map[string]interface{}{
		"ignored": "x",
	}, time.Now().UTC(), map[string]string{
		"status": "status",
	})

	if setClause != "" {
		t.Fatalf("expected empty set clause, got %q", setClause)
	}
	if values != nil {
		t.Fatalf("expected nil values, got %#v", values)
	}
}

func TestBuildMessageUpdateSetNormalizesPORStatusCode(t *testing.T) {
	setClause, values := buildMessageUpdateSet(map[string]interface{}{
		"por_status_code": json.Number("5"),
	}, time.Unix(123, 0).UTC(), map[string]string{
		"por_status_code": "por_status_code",
	})

	if setClause == "" {
		t.Fatal("expected non-empty set clause")
	}
	if len(values) != 2 {
		t.Fatalf("expected 2 values, got %d", len(values))
	}
	got, ok := values[0].(int16)
	if !ok {
		t.Fatalf("expected int16, got %T", values[0])
	}
	if got != 5 {
		t.Fatalf("unexpected por_status_code: got %d want 5", got)
	}
}

func TestErrorCounterDeltasTracksStatusDLRAndPORChanges(t *testing.T) {
	oldStatus := "send_failed"
	oldDLR := "UNDELIV"
	oldPOR := int16(5)
	newDLR := "DELIVRD"
	newPOR := int16(0)

	deltas := errorCounterDeltas(&oldStatus, "sent", &oldDLR, &newDLR, &oldPOR, &newPOR)
	got := map[string]int64{}
	for _, delta := range deltas {
		got[delta.Kind+":"+delta.Key] = delta.Delta
	}

	want := map[string]int64{
		"status:send_failed": -1,
		"dlr:UNDELIV":        -1,
		"por:5":              -1,
	}
	if len(got) != len(want) {
		t.Fatalf("unexpected delta count: got %#v want %#v", got, want)
	}
	for key, count := range want {
		if got[key] != count {
			t.Fatalf("delta %s = %d want %d", key, got[key], count)
		}
	}
}
