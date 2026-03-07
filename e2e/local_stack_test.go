//go:build e2e

package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"
)

type apiClient struct {
	baseURL string
	http    *http.Client
}

type profileEnvelope struct {
	Data struct {
		ID           string `json:"id"`
		Applications []struct {
			ID string `json:"id"`
		} `json:"applications"`
	} `json:"data"`
}

type cardEnvelope struct {
	Data struct {
		ID string `json:"id"`
	} `json:"data"`
}

type campaignCreateEnvelope struct {
	Data struct {
		ID string `json:"id"`
	} `json:"data"`
	StartError string `json:"start_error"`
}

type campaignDetail struct {
	ID           string  `json:"id"`
	Status       string  `json:"status"`
	TotalCards   int64   `json:"total_cards"`
	SuccessCards int64   `json:"success_cards"`
	FailedCards  int64   `json:"failed_cards"`
	PendingCards int64   `json:"pending_cards"`
	InProgress   int64   `json:"in_progress_cards"`
	ProgressPct  float64 `json:"progress_percent"`
	StartedAt    *string `json:"started_at"`
	CompletedAt  *string `json:"completed_at"`
}

type throughputEnvelope struct {
	Data []struct {
		Timestamp string `json:"timestamp"`
		Sent      int64  `json:"sent"`
		Delivered int64  `json:"delivered"`
		Failed    int64  `json:"failed"`
	} `json:"data"`
}

func TestLocalStackCampaignLifecycle(t *testing.T) {
	client := &apiClient{
		baseURL: getenv("E2E_BASE_URL", "http://localhost:8080"),
		http: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
	cardCount := getenvInt("E2E_CARD_COUNT", 50)
	if err := waitForHealthy(client, 2*time.Minute); err != nil {
		t.Fatalf("wait for stack health: %v", err)
	}

	prefix := fmt.Sprintf("e2e-%d", time.Now().UnixNano())
	profileID, appID := createProfileAndApplication(t, client, prefix)
	cardIDs := createCards(t, client, prefix, profileID, cardCount)
	campaignID := createCampaign(t, client, prefix, appID, cardIDs)

	wallStart := time.Now()
	campaign := waitForCampaignTerminal(t, client, campaignID, 2*time.Minute)
	wallElapsed := time.Since(wallStart)

	if campaign.FailedCards != 0 {
		t.Fatalf("campaign has failed cards: %+v", campaign)
	}
	if campaign.SuccessCards != int64(cardCount) {
		t.Fatalf("campaign success count mismatch: got %d want %d", campaign.SuccessCards, cardCount)
	}
	if campaign.Status != "completed" && campaign.Status != "completed_with_errors" {
		t.Fatalf("unexpected terminal campaign status: %s", campaign.Status)
	}

	throughput := getCampaignThroughput(t, client, campaignID, int64(cardCount))
	var sent, delivered, failed int64
	for _, point := range throughput.Data {
		sent += point.Sent
		delivered += point.Delivered
		failed += point.Failed
	}
	if sent < int64(cardCount) {
		t.Fatalf("campaign throughput sent count too low: got %d want >= %d", sent, cardCount)
	}
	if delivered < int64(cardCount) {
		t.Fatalf("campaign throughput delivered count too low: got %d want >= %d", delivered, cardCount)
	}
	if failed != 0 {
		t.Fatalf("campaign throughput failed count mismatch: got %d want 0", failed)
	}

	cardTPS := float64(cardCount) / wallElapsed.Seconds()
	smsPartTPS := cardTPS // this E2E uses one step and one SMS part per card

	t.Logf("E2E summary: cards=%d elapsed=%s card_tps=%.2f sms_part_tps=%.2f sent=%d delivered=%d failed=%d status=%s progress=%.2f%%",
		cardCount, wallElapsed, cardTPS, smsPartTPS, sent, delivered, failed, campaign.Status, campaign.ProgressPct)
}

func waitForHealthy(client *apiClient, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := client.http.Get(client.baseURL + "/api/v1/monitoring/health")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("timeout waiting for health")
}

func createProfileAndApplication(t *testing.T, client *apiClient, prefix string) (string, string) {
	t.Helper()
	body := map[string]any{
		"name":           prefix + "-profile",
		"max_concat_sms": 5,
		"buffer_size":    140,
		"applications": []map[string]any{{
			"name":               prefix + "-app",
			"tar":                "010203",
			"kic_algo":           "DES",
			"kic_mode":           "TRIPLE_DES_CBC_2_KEYS",
			"kic_keyset_id":      1,
			"kid_algo":           "DES",
			"kid_mode":           "TRIPLE_DES_CBC_2_KEYS",
			"kid_keyset_id":      1,
			"certification_mode": "CC",
			"ciphered":           true,
			"counter_mode":       "COUNTER_REPLAY_OR_CHECK",
			"por_mode":           "REPLY_ALWAYS",
			"por_protocol":       "SMS_SUBMIT",
			"por_ciphered":       false,
			"por_cert_mode":      "NO_SECURITY",
		}},
	}
	var resp profileEnvelope
	postJSON(t, client, "/api/v1/profiles", body, http.StatusCreated, &resp)
	if resp.Data.ID == "" || len(resp.Data.Applications) != 1 || resp.Data.Applications[0].ID == "" {
		t.Fatalf("unexpected profile create response: %+v", resp)
	}
	return resp.Data.ID, resp.Data.Applications[0].ID
}

func createCards(t *testing.T, client *apiClient, prefix, profileID string, count int) []string {
	t.Helper()
	ids := make([]string, 0, count)
	baseMSISDN := int(time.Now().UnixNano()%900000 + 100000)
	for i := 0; i < count; i++ {
		body := map[string]any{
			"iccid":      fmt.Sprintf("%s-iccid-%06d", prefix, i),
			"imsi":       fmt.Sprintf("%s-imsi-%06d", prefix, i),
			"msisdn":     fmt.Sprintf("447700%06d", (baseMSISDN+i)%1000000),
			"profile_id": profileID,
			"enc_key":    "404142434445464748494A4B4C4D4E4F",
			"auth_key":   "505152535455565758595A5B5C5D5E5F",
			"status":     "active",
		}
		var resp cardEnvelope
		postJSON(t, client, "/api/v1/cards", body, http.StatusCreated, &resp)
		if resp.Data.ID == "" {
			t.Fatalf("empty card id in create response for card %d", i)
		}
		ids = append(ids, resp.Data.ID)
	}
	return ids
}

func createCampaign(t *testing.T, client *apiClient, prefix, appID string, cardIDs []string) string {
	t.Helper()
	body := map[string]any{
		"name":              prefix + "-campaign",
		"campaign_type":     "script",
		"card_ids":          cardIDs,
		"max_retries":       1,
		"start_immediately": true,
		"commands": []map[string]any{{
			"application_id":  appID,
			"script":          "A0CA000000",
			"sequence":        1,
			"expect_response": true,
		}},
	}
	var resp campaignCreateEnvelope
	postJSON(t, client, "/api/v1/campaigns", body, http.StatusCreated, &resp)
	if resp.StartError != "" {
		t.Fatalf("campaign failed to start immediately: %s", resp.StartError)
	}
	if resp.Data.ID == "" {
		t.Fatalf("empty campaign id in create response: %+v", resp)
	}
	return resp.Data.ID
}

func waitForCampaignTerminal(t *testing.T, client *apiClient, campaignID string, timeout time.Duration) campaignDetail {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var campaign campaignDetail
		getJSON(t, client, "/api/v1/campaigns/"+campaignID, http.StatusOK, &campaign)
		if campaign.TotalCards > 0 && campaign.SuccessCards+campaign.FailedCards >= campaign.TotalCards && campaign.PendingCards == 0 && campaign.InProgress == 0 {
			return campaign
		}
		if campaign.Status == "failed" || campaign.Status == "completed" || campaign.Status == "completed_with_errors" {
			return campaign
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for campaign %s to complete", campaignID)
	return campaignDetail{}
}

func getCampaignThroughput(t *testing.T, client *apiClient, campaignID string, expectedMT int64) throughputEnvelope {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		var resp throughputEnvelope
		getJSON(t, client, "/api/v1/dashboard/sms-throughput?campaign_id="+campaignID, http.StatusOK, &resp)
		var sent, delivered, failed int64
		for _, point := range resp.Data {
			sent += point.Sent
			delivered += point.Delivered
			failed += point.Failed
		}
		if sent >= expectedMT && delivered+failed >= expectedMT {
			return resp
		}
		time.Sleep(500 * time.Millisecond)
	}
	var resp throughputEnvelope
	getJSON(t, client, "/api/v1/dashboard/sms-throughput?campaign_id="+campaignID, http.StatusOK, &resp)
	return resp
}

func postJSON(t *testing.T, client *apiClient, path string, body any, wantStatus int, out any) {
	t.Helper()
	payload, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal request %s: %v", path, err)
	}
	req, err := http.NewRequest(http.MethodPost, client.baseURL+path, bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("build request %s: %v", path, err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.http.Do(req)
	if err != nil {
		t.Fatalf("post %s: %v", path, err)
	}
	defer resp.Body.Close()
	decodeResponse(t, resp, wantStatus, out)
}

func getJSON(t *testing.T, client *apiClient, path string, wantStatus int, out any) {
	t.Helper()
	resp, err := client.http.Get(client.baseURL + path)
	if err != nil {
		t.Fatalf("get %s: %v", path, err)
	}
	defer resp.Body.Close()
	decodeResponse(t, resp, wantStatus, out)
}

func decodeResponse(t *testing.T, resp *http.Response, wantStatus int, out any) {
	t.Helper()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	if resp.StatusCode != wantStatus {
		t.Fatalf("unexpected status %d want %d body=%s", resp.StatusCode, wantStatus, string(body))
	}
	if out == nil {
		return
	}
	if err := json.Unmarshal(body, out); err != nil {
		t.Fatalf("decode response: %v body=%s", err, string(body))
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getenvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil && parsed > 0 {
			return parsed
		}
	}
	return fallback
}
