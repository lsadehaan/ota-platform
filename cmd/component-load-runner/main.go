package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	kafkapkg "ota-platform/internal/kafka"
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

type promQueryResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Value []interface{} `json:"value"`
		} `json:"result"`
	} `json:"data"`
}

type result struct {
	Component   string
	InputCount  int
	OutputCount int
	Elapsed     time.Duration
	TPS         float64
	Notes       []string
}

func main() {
	var (
		component  = flag.String("component", "planner", "planner|executor|gateway|projector")
		baseURL    = flag.String("base-url", getenv("COMPONENT_BASE_URL", "http://ota-api:8080"), "control-plane API base URL")
		brokersCSV = flag.String("kafka-brokers", getenv("COMPONENT_KAFKA_BROKERS", "kafka:9092"), "comma-separated kafka brokers")
		promURL    = flag.String("prom-url", getenv("COMPONENT_PROM_URL", "http://prometheus:9090"), "Prometheus base URL")
		cards      = flag.Int("cards", getenvInt("COMPONENT_CARD_COUNT", 5000), "card count for planner/executor")
		messages   = flag.Int("messages", getenvInt("COMPONENT_MESSAGE_COUNT", 10000), "message count for gateway/projector")
		timeout    = flag.Duration("timeout", getenvDuration("COMPONENT_TIMEOUT", 10*time.Minute), "component timeout")
		output     = flag.String("output", "", "optional markdown report path")
	)
	flag.Parse()

	client := &apiClient{baseURL: *baseURL, http: &http.Client{Timeout: 60 * time.Second}}
	if err := waitForHealthy(client, 2*time.Minute); err != nil {
		fatalf("wait for api health: %v", err)
	}

	brokers := splitCSV(*brokersCSV)
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	var res result
	var err error
	switch *component {
	case "planner":
		res, err = runPlanner(ctx, client, brokers, *cards)
	case "executor":
		res, err = runExecutor(ctx, client, brokers, *cards)
	case "gateway":
		res, err = runGateway(ctx, brokers, *messages)
	case "projector":
		res, err = runProjector(ctx, brokers, *promURL, *messages)
	default:
		fatalf("unknown component %q", *component)
	}
	if err != nil {
		fatalf("run %s: %v", *component, err)
	}

	report := renderReport(res)
	if *output != "" {
		if err := os.WriteFile(*output, []byte(report), 0o644); err != nil {
			fatalf("write report: %v", err)
		}
	}
	fmt.Print(report)
}

func runPlanner(ctx context.Context, client *apiClient, brokers []string, count int) (result, error) {
	prefix := fmt.Sprintf("planner-%d", time.Now().UnixNano())
	profileID, appID, err := createProfileAndApplication(client, prefix)
	if err != nil {
		return result{}, err
	}
	if err := importCards(client, prefix, profileID, count); err != nil {
		return result{}, err
	}
	cardIDs, err := listCardIDsByPrefix(client, prefix, count)
	if err != nil {
		return result{}, err
	}
	campaignID, err := createCampaign(client, prefix, appID, cardIDs, false)
	if err != nil {
		return result{}, err
	}

	counter := newKafkaCounter(brokers, "card-events", "planner-counter-"+uuid.New().String(), func(key, value []byte) bool {
		var ev kafkapkg.CardEvent
		if err := json.Unmarshal(value, &ev); err != nil {
			return false
		}
		return ev.Type == "card.activate" && ev.CampaignID == campaignID
	})
	defer counter.Close()
	if err := counter.Start(ctx); err != nil {
		return result{}, err
	}

	start := time.Now()
	if err := postEmpty(client, "/api/v1/campaigns/"+campaignID+"/start", http.StatusOK); err != nil {
		return result{}, err
	}
	if err := counter.WaitFor(ctx, count); err != nil {
		return result{}, err
	}
	elapsed := time.Since(start)
	return result{Component: "planner", InputCount: count, OutputCount: counter.Count(), Elapsed: elapsed, TPS: float64(count) / elapsed.Seconds(), Notes: []string{"measured from start endpoint to card.activate publication"}}, nil
}

func runExecutor(ctx context.Context, client *apiClient, brokers []string, count int) (result, error) {
	prefix := fmt.Sprintf("executor-%d", time.Now().UnixNano())
	profileID, appID, err := createProfileAndApplication(client, prefix)
	if err != nil {
		return result{}, err
	}
	if err := importCards(client, prefix, profileID, count); err != nil {
		return result{}, err
	}
	cardIDs, err := listCardIDsByPrefix(client, prefix, count)
	if err != nil {
		return result{}, err
	}
	campaignID, err := createCampaign(client, prefix, appID, cardIDs, false)
	if err != nil {
		return result{}, err
	}
	if err := postEmpty(client, "/api/v1/campaigns/"+campaignID+"/start", http.StatusOK); err != nil {
		return result{}, err
	}

	counter := newKafkaCounter(brokers, "send-sms", "executor-counter-"+uuid.New().String(), func(key, value []byte) bool {
		var msg kafkapkg.SendSMSMessage
		if err := json.Unmarshal(value, &msg); err != nil {
			return false
		}
		return msg.CampaignID == campaignID
	})
	defer counter.Close()
	if err := counter.Start(ctx); err != nil {
		return result{}, err
	}

	producer := kafkapkg.NewProducer(brokers, "card-events", zap.NewNop())
	defer producer.Close()

	items := make([]kafkapkg.BatchItem, 0, 1000)
	start := time.Now()
	for _, cardID := range cardIDs {
		items = append(items, kafkapkg.BatchItem{Key: cardID, Value: kafkapkg.CardEvent{Type: "card.activate", EventID: uuid.New().String(), CardID: cardID, CampaignID: campaignID, Step: 1, RetryCount: 0, Timestamp: time.Now()}})
		if len(items) == 1000 {
			if err := producer.PublishBatch(ctx, items); err != nil {
				return result{}, err
			}
			items = items[:0]
		}
	}
	if len(items) > 0 {
		if err := producer.PublishBatch(ctx, items); err != nil {
			return result{}, err
		}
	}
	if err := counter.WaitFor(ctx, count); err != nil {
		return result{}, err
	}
	elapsed := time.Since(start)
	return result{Component: "executor", InputCount: count, OutputCount: counter.Count(), Elapsed: elapsed, TPS: float64(count) / elapsed.Seconds(), Notes: []string{"measured from card.activate publish to send-sms output"}}, nil
}

func runGateway(ctx context.Context, brokers []string, count int) (result, error) {
	campaignID := uuid.New().String()
	counter := newKafkaCounter(brokers, "card-events", "gateway-counter-"+uuid.New().String(), func(key, value []byte) bool {
		var ev kafkapkg.CardEvent
		if err := json.Unmarshal(value, &ev); err != nil {
			return false
		}
		return ev.Type == "card.dlr_received" && ev.CampaignID == campaignID
	})
	defer counter.Close()
	if err := counter.Start(ctx); err != nil {
		return result{}, err
	}

	producer := kafkapkg.NewProducer(brokers, "send-sms", zap.NewNop())
	defer producer.Close()

	payload := "A0CA000000"
	items := make([]kafkapkg.BatchItem, 0, 1000)
	start := time.Now()
	for i := 0; i < count; i++ {
		cardID := uuid.New().String()
		msgID := uuid.New().String()
		msisdn := fmt.Sprintf("447700%06d", i%1000000)
		items = append(items, kafkapkg.BatchItem{Key: cardID, Value: kafkapkg.SendSMSMessage{
			MsgID: msgID, CampaignID: campaignID, CardID: cardID, MSISDN: msisdn,
			TON: 1, NPI: 1, DataCoding: 0xF6, ProtocolID: 0x7F, ESMClass: 0x40,
			Parts: []kafkapkg.SMSPart{{Sequence: 1, Total: 1, RefNum: 0, Payload: payload}},
		}})
		if len(items) == 1000 {
			if err := producer.PublishBatch(ctx, items); err != nil {
				return result{}, err
			}
			items = items[:0]
		}
	}
	if len(items) > 0 {
		if err := producer.PublishBatch(ctx, items); err != nil {
			return result{}, err
		}
	}
	if err := counter.WaitFor(ctx, count); err != nil {
		return result{}, err
	}
	elapsed := time.Since(start)
	return result{Component: "gateway", InputCount: count, OutputCount: counter.Count(), Elapsed: elapsed, TPS: float64(count) / elapsed.Seconds(), Notes: []string{"measured from send-sms publish to card.dlr_received output"}}, nil
}

func runProjector(ctx context.Context, brokers []string, promURL string, count int) (result, error) {
	before, err := promQueryValue(promURL, `sum(ota_projector_actions_processed_total)`)
	if err != nil {
		return result{}, err
	}

	producer := kafkapkg.NewProducer(brokers, "message-log", zap.NewNop())
	defer producer.Close()

	campaignID := uuid.New().String()
	start := time.Now()
	items := make([]kafkapkg.BatchItem, 0, 1000)
	expectedActions := count * 2
	for i := 0; i < count; i++ {
		id := uuid.New().String()
		cardID := uuid.New().String()
		createdAt := time.Now().UTC().Format(time.RFC3339Nano)
		items = append(items,
			kafkapkg.BatchItem{Key: cardID, Value: kafkapkg.MessageLogAction{Action: "create", Log: &kafkapkg.MessageLogEntry{ID: id, CampaignID: campaignID, CardID: cardID, Direction: "MT", CreatedAt: createdAt, UpdatedAt: createdAt, RawPayload: "", Status: "sent"}}},
			kafkapkg.BatchItem{Key: cardID, Value: kafkapkg.MessageLogAction{Action: "update", ID: id, Updates: map[string]interface{}{"dlr_status": "DELIVRD", "updated_at": time.Now().UTC().Format(time.RFC3339Nano)}}},
		)
		if len(items) >= 1000 {
			if err := producer.PublishBatch(ctx, items); err != nil {
				return result{}, err
			}
			items = items[:0]
		}
	}
	if len(items) > 0 {
		if err := producer.PublishBatch(ctx, items); err != nil {
			return result{}, err
		}
	}

	if err := waitForPromDelta(ctx, promURL, `sum(ota_projector_actions_processed_total)`, before, float64(expectedActions)); err != nil {
		return result{}, err
	}
	elapsed := time.Since(start)
	return result{Component: "projector", InputCount: expectedActions, OutputCount: expectedActions, Elapsed: elapsed, TPS: float64(expectedActions) / elapsed.Seconds(), Notes: []string{"measured from message-log publish to projector processed-actions counter delta"}}, nil
}

type kafkaCounter struct {
	brokers []string
	topic   string
	readers []*kafka.Reader
	match   func(key, value []byte) bool
	mu      sync.Mutex
	count   int
	errMu   sync.Mutex
	err     error
}

func newKafkaCounter(brokers []string, topic, groupID string, match func(key, value []byte) bool) *kafkaCounter {
	_ = groupID
	return &kafkaCounter{
		brokers: brokers,
		topic:   topic,
		match:   match,
	}
}

func (c *kafkaCounter) Start(ctx context.Context) error {
	conn, err := kafka.DialContext(ctx, "tcp", c.brokers[0])
	if err != nil {
		return err
	}
	partitions, err := conn.ReadPartitions()
	_ = conn.Close()
	if err != nil {
		return err
	}
	for _, p := range partitions {
		if p.Topic != c.topic {
			continue
		}
		leader, err := kafka.DialLeader(ctx, "tcp", c.brokers[0], p.Topic, p.ID)
		if err != nil {
			return err
		}
		_, last, err := leader.ReadOffsets()
		_ = leader.Close()
		if err != nil {
			return err
		}
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:   c.brokers,
			Topic:     p.Topic,
			Partition: p.ID,
			MinBytes:  1,
			MaxBytes:  10e6,
			MaxWait:   100 * time.Millisecond,
		})
		if err := reader.SetOffset(last); err != nil {
			_ = reader.Close()
			return err
		}
		c.readers = append(c.readers, reader)
		go c.readLoop(ctx, reader)
	}
	time.Sleep(250 * time.Millisecond)
	return nil
}

func (c *kafkaCounter) WaitFor(ctx context.Context, expected int) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := c.Err(); err != nil {
			return err
		}
		if c.Count() >= expected {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for %d messages, got %d", expected, c.Count())
		case <-ticker.C:
		}
	}
}

func (c *kafkaCounter) Count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.count
}

func (c *kafkaCounter) Err() error {
	c.errMu.Lock()
	defer c.errMu.Unlock()
	return c.err
}

func (c *kafkaCounter) readLoop(ctx context.Context, reader *kafka.Reader) {
	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			c.errMu.Lock()
			if c.err == nil {
				c.err = err
			}
			c.errMu.Unlock()
			return
		}
		if c.match(msg.Key, msg.Value) {
			c.mu.Lock()
			c.count++
			c.mu.Unlock()
		}
	}
}

func (c *kafkaCounter) Close() error {
	var firstErr error
	for _, reader := range c.readers {
		if err := reader.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
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

func createProfileAndApplication(client *apiClient, prefix string) (string, string, error) {
	body := map[string]any{"name": prefix + "-profile", "max_concat_sms": 5, "buffer_size": 140, "applications": []map[string]any{{"name": prefix + "-app", "tar": "010203", "kic_algo": "DES", "kic_mode": "TRIPLE_DES_CBC_2_KEYS", "kic_keyset_id": 1, "kid_algo": "DES", "kid_mode": "TRIPLE_DES_CBC_2_KEYS", "kid_keyset_id": 1, "certification_mode": "CC", "ciphered": true, "counter_mode": "COUNTER_REPLAY_OR_CHECK", "por_mode": "REPLY_ALWAYS", "por_protocol": "SMS_SUBMIT", "por_ciphered": false, "por_cert_mode": "NO_SECURITY"}}}
	var resp profileEnvelope
	if err := postJSON(client, "/api/v1/profiles", body, http.StatusCreated, &resp); err != nil {
		return "", "", err
	}
	if resp.Data.ID == "" || len(resp.Data.Applications) != 1 || resp.Data.Applications[0].ID == "" {
		return "", "", fmt.Errorf("unexpected profile response")
	}
	return resp.Data.ID, resp.Data.Applications[0].ID, nil
}

func importCards(client *apiClient, prefix, profileID string, count int) error {
	profileName := prefix + "-profile"
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	_ = w.Write([]string{"iccid", "imsi", "msisdn", "profile_name", "enc_key", "auth_key"})
	base := int(time.Now().UnixNano()%900000 + 100000)
	for i := 0; i < count; i++ {
		_ = w.Write([]string{
			fmt.Sprintf("%s-iccid-%06d", prefix, i),
			fmt.Sprintf("%s-imsi-%06d", prefix, i),
			fmt.Sprintf("447700%06d", (base+i)%1000000),
			profileName,
			"404142434445464748494A4B4C4D4E4F",
			"505152535455565758595A5B5C5D5E5F",
		})
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
	if _, err := fw.Write(buf.Bytes()); err != nil {
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
	_ = profileID // for symmetry; cards import uses profile name
	return nil
}

func listCardIDsByPrefix(client *apiClient, prefix string, expected int) ([]string, error) {
	page := 1
	ids := make([]string, 0, expected)
	for {
		var resp cardsListEnvelope
		path := fmt.Sprintf("/api/v1/cards?q=%s&page=%d&page_size=100", url.QueryEscape(prefix), page)
		if err := getJSON(client, path, http.StatusOK, &resp); err != nil {
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

func createCampaign(client *apiClient, prefix, appID string, cardIDs []string, startImmediately bool) (string, error) {
	body := map[string]any{"name": prefix + "-campaign", "campaign_type": "script", "card_ids": cardIDs, "max_retries": 1, "start_immediately": startImmediately, "commands": []map[string]any{{"application_id": appID, "script": "A0CA000000", "sequence": 1, "expect_response": true}}}
	var resp campaignCreateEnvelope
	if err := postJSON(client, "/api/v1/campaigns", body, http.StatusCreated, &resp); err != nil {
		return "", err
	}
	if resp.StartError != "" {
		return "", fmt.Errorf("start error: %s", resp.StartError)
	}
	if resp.Data.ID == "" {
		return "", fmt.Errorf("empty campaign id")
	}
	return resp.Data.ID, nil
}

func postEmpty(client *apiClient, path string, wantStatus int) error {
	req, err := http.NewRequest(http.MethodPost, client.baseURL+path, bytes.NewReader([]byte(`{}`)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != wantStatus {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("unexpected status %d want %d body=%s", resp.StatusCode, wantStatus, string(b))
	}
	return nil
}

func postJSON(client *apiClient, path string, body any, wantStatus int, out any) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, client.baseURL+path, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return decodeResponse(resp, wantStatus, out)
}

func getJSON(client *apiClient, path string, wantStatus int, out any) error {
	resp, err := client.http.Get(client.baseURL + path)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return decodeResponse(resp, wantStatus, out)
}

func decodeResponse(resp *http.Response, wantStatus int, out any) error {
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
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

func promQueryValue(baseURL, expr string) (float64, error) {
	u := strings.TrimRight(baseURL, "/") + "/api/v1/query?query=" + url.QueryEscape(expr)
	resp, err := http.Get(u)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	var parsed promQueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return 0, err
	}
	if parsed.Status != "success" {
		return 0, fmt.Errorf("unexpected prom response for %s", expr)
	}
	if len(parsed.Data.Result) == 0 {
		return 0, nil
	}
	if len(parsed.Data.Result[0].Value) < 2 {
		return 0, fmt.Errorf("unexpected prom response for %s", expr)
	}
	raw := fmt.Sprint(parsed.Data.Result[0].Value[1])
	return strconv.ParseFloat(raw, 64)
}

func waitForPromDelta(ctx context.Context, promURL, expr string, before, delta float64) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	target := before + delta
	for {
		current, err := promQueryValue(promURL, expr)
		if err == nil && current >= target {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for prom delta %.0f on %s", delta, expr)
		case <-ticker.C:
		}
	}
}

func renderReport(res result) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# Component Load Report\n\n")
	fmt.Fprintf(&b, "- Component: `%s`\n", res.Component)
	fmt.Fprintf(&b, "- Input count: `%d`\n", res.InputCount)
	fmt.Fprintf(&b, "- Output count: `%d`\n", res.OutputCount)
	fmt.Fprintf(&b, "- Elapsed: `%s`\n", res.Elapsed)
	fmt.Fprintf(&b, "- TPS: `%.2f`\n", res.TPS)
	if len(res.Notes) > 0 {
		fmt.Fprintf(&b, "\nNotes:\n")
		for _, note := range res.Notes {
			fmt.Fprintf(&b, "- %s\n", note)
		}
	}
	return b.String()
}

func splitCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getenvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func getenvDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}

func fatalf(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
