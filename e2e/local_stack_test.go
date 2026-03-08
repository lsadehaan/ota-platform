//go:build e2e

package e2e

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"sort"
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

type cardsListEnvelope struct {
	Data []struct {
		ID string `json:"id"`
	} `json:"data"`
	Total      int `json:"total"`
	Page       int `json:"page"`
	TotalPages int `json:"total_pages"`
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
	campaignTimeout := getenvDuration("E2E_CAMPAIGN_TIMEOUT", 2*time.Minute)
	if err := waitForHealthy(client, 2*time.Minute); err != nil {
		t.Fatalf("wait for stack health: %v", err)
	}

	prefix := fmt.Sprintf("e2e-%d", time.Now().UnixNano())
	profileID, appID := createProfileAndApplication(t, client, prefix)
	cardIDs := createCards(t, client, prefix, profileID, cardCount)
	campaignID := createCampaign(t, client, prefix, appID, cardIDs)

	wallStart := time.Now()
	campaign := waitForCampaignTerminal(t, client, campaignID, campaignTimeout)
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
	if failed != 0 {
		t.Fatalf("campaign throughput failed count mismatch: got %d want 0", failed)
	}
	if delivered < int64(cardCount) {
		t.Logf("throughput read model lagging behind campaign completion: delivered=%d expected>=%d", delivered, cardCount)
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
	if count >= 500 {
		profileName := prefix + "-profile"
		if err := importCardsCSV(t, client, prefix, profileName, count); err != nil {
			t.Fatalf("import cards: %v", err)
		}
		ids, err := listCardIDsByPrefix(t, client, prefix, count)
		if err != nil {
			t.Fatalf("list imported cards: %v", err)
		}
		return ids
	}
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

func importCardsCSV(t *testing.T, client *apiClient, prefix, profileName string, count int) error {
	t.Helper()

	var csvBuf bytes.Buffer
	w := csv.NewWriter(&csvBuf)
	if err := w.Write([]string{"iccid", "imsi", "msisdn", "profile_name", "enc_key", "auth_key"}); err != nil {
		return err
	}

	baseMSISDN := int(time.Now().UnixNano()%900000 + 100000)
	for i := 0; i < count; i++ {
		if err := w.Write([]string{
			fmt.Sprintf("%s-iccid-%06d", prefix, i),
			fmt.Sprintf("%s-imsi-%06d", prefix, i),
			fmt.Sprintf("447700%06d", (baseMSISDN+i)%1000000),
			profileName,
			"404142434445464748494A4B4C4D4E4F",
			"505152535455565758595A5B5C5D5E5F",
		}); err != nil {
			return err
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return err
	}

	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	fw, err := mw.CreateFormFile("file", "cards.csv")
	if err != nil {
		return err
	}
	if _, err := fw.Write(csvBuf.Bytes()); err != nil {
		return err
	}
	if err := mw.Close(); err != nil {
		return err
	}

	req, err := http.NewRequest(http.MethodPost, client.baseURL+"/api/v1/cards/import", &body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())

	resp, err := client.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("import cards status %d body=%s", resp.StatusCode, string(b))
	}
	return nil
}

func listCardIDsByPrefix(t *testing.T, client *apiClient, prefix string, expected int) ([]string, error) {
	t.Helper()

	page := 1
	ids := make([]string, 0, expected)
	for {
		var resp cardsListEnvelope
		path := fmt.Sprintf("/api/v1/cards?q=%s&page=%d&page_size=100", url.QueryEscape(prefix), page)
		if err := getJSONE(client, path, http.StatusOK, &resp); err != nil {
			return nil, err
		}
		for _, card := range resp.Data {
			ids = append(ids, card.ID)
		}
		if page >= resp.TotalPages || len(resp.Data) == 0 {
			break
		}
		page++
	}

	sort.Strings(ids)
	if len(ids) < expected {
		return nil, fmt.Errorf("listed %d cards, expected at least %d", len(ids), expected)
	}
	return ids[:expected], nil
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
		if err := getJSONE(client, "/api/v1/campaigns/"+campaignID, http.StatusOK, &campaign); err != nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}
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
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		var resp throughputEnvelope
		if err := getJSONE(client, "/api/v1/dashboard/sms-throughput?campaign_id="+campaignID, http.StatusOK, &resp); err != nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}
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
	if err := getJSONE(client, path, wantStatus, out); err != nil {
		t.Fatalf("get %s: %v", path, err)
	}
}

func getJSONE(client *apiClient, path string, wantStatus int, out any) error {
	resp, err := client.http.Get(client.baseURL + path)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return decodeResponseE(resp, wantStatus, out)
}

func decodeResponse(t *testing.T, resp *http.Response, wantStatus int, out any) {
	t.Helper()
	if err := decodeResponseE(resp, wantStatus, out); err != nil {
		t.Fatal(err)
	}
}

func decodeResponseE(resp *http.Response, wantStatus int, out any) error {
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response body: %w", err)
	}
	if resp.StatusCode != wantStatus {
		return fmt.Errorf("unexpected status %d want %d body=%s", resp.StatusCode, wantStatus, string(body))
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("decode response: %w body=%s", err, string(body))
	}
	return nil
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

func getenvDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if parsed, err := time.ParseDuration(v); err == nil && parsed > 0 {
			return parsed
		}
	}
	return fallback
}
