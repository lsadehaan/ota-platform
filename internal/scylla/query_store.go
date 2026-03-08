package scylla

import (
	"container/heap"
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/google/uuid"
)

type CampaignStats struct {
	Total      int64
	Pending    int64
	InProgress int64
	Completed  int64
	Failed     int64
	Skipped    int64
}

type CampaignCardView struct {
	CardID      string
	Status      string
	CurrentStep int
	RetryCount  int
	LastMsgID   string
	LastError   string
	UpdatedAt   time.Time
}

type MessageRecord struct {
	ID             uuid.UUID
	CampaignID     *uuid.UUID
	CardID         uuid.UUID
	Direction      string
	Status         string
	SMPPMessageID  *string
	DLRStatus      *string
	CounterHex     *string
	PORStatusCode  *int16
	RawPayload     []byte
	SecuredPayload []byte
	PORData        []byte
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

type MessageFilter struct {
	CampaignID *uuid.UUID
	CardID     *uuid.UUID
	Direction  string
	Status     string
	From       time.Time
	To         time.Time
	Page       int
	PageSize   int
}

type ErrorCount struct {
	Key   string
	Count int64
}

type ThroughputPoint struct {
	Timestamp time.Time `json:"timestamp"`
	Sent      float64   `json:"sent"`
	Delivered float64   `json:"delivered"`
	Failed    float64   `json:"failed"`
}

type hourlyMessageMetrics struct {
	Timestamp   time.Time
	Total       int64
	MT          int64
	MO          int64
	Delivered   int64
	Undelivered int64
}

type MessageMetrics struct {
	Total       int64
	MT          int64
	MO          int64
	Delivered   int64
	Undelivered int64
}

type QueryStore struct {
	client *Client
}

func NewQueryStore(client *Client) *QueryStore {
	return &QueryStore{client: client}
}

func (s *QueryStore) InitializeCampaign(ctx context.Context, campaignID uuid.UUID, cardIDs []uuid.UUID, initialStep int, updatedAt time.Time) error {
	byBucket := make(map[int]int64)
	batch := s.client.Session().NewBatch(gocql.UnloggedBatch).WithContext(ctx)
	flushBatch := func() error {
		if len(batch.Entries) == 0 {
			return nil
		}
		if err := s.client.Session().ExecuteBatch(batch); err != nil {
			return err
		}
		batch = s.client.Session().NewBatch(gocql.UnloggedBatch).WithContext(ctx)
		return nil
	}
	for _, cardID := range cardIDs {
		cardStr := cardID.String()
		cardBucket := s.client.CardBucket(cardStr)
		campaignBucket := s.client.CampaignBucket(campaignID.String(), cardStr)
		byBucket[campaignBucket]++

		batch.Query(`INSERT INTO card_state_by_card (
			card_bucket, card_id, campaign_id, status, current_step, retry_count,
			last_msg_id, last_smpp_message_id, last_error_code, last_error_text, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			cardBucket,
			toGocqlUUID(cardID),
			toGocqlUUID(campaignID),
			"pending",
			initialStep,
			0,
			nil,
			"",
			"",
			"",
			updatedAt.UTC(),
		)

		batch.Query(`INSERT INTO campaign_card_status_by_bucket (
			campaign_id, campaign_bucket, status, updated_at, card_id, current_step, retry_count, last_msg_id, last_error_text
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			toGocqlUUID(campaignID), campaignBucket, "pending", updatedAt.UTC(), toGocqlUUID(cardID), initialStep, 0, nil, "",
		)
		if len(batch.Entries) >= 200 {
			if err := flushBatch(); err != nil {
				return fmt.Errorf("initialize campaign batch: %w", err)
			}
		}
	}
	if err := flushBatch(); err != nil {
		return fmt.Errorf("initialize campaign batch: %w", err)
	}

	for bucket, count := range byBucket {
		if err := s.client.Session().Query(
			`UPDATE campaign_progress_by_bucket SET pending = pending + ? WHERE campaign_id = ? AND campaign_bucket = ?`,
			count, toGocqlUUID(campaignID), bucket,
		).WithContext(ctx).Exec(); err != nil {
			return fmt.Errorf("init campaign progress bucket: %w", err)
		}
	}

	return nil
}

func (s *QueryStore) CampaignStats(ctx context.Context, campaignID uuid.UUID) (CampaignStats, error) {
	iter := s.client.Session().Query(
		`SELECT pending, in_progress, completed, failed, skipped FROM campaign_progress_by_bucket WHERE campaign_id = ?`,
		toGocqlUUID(campaignID),
	).WithContext(ctx).Iter()

	var stats CampaignStats
	var pending, inProgress, completed, failed, skipped int64
	for iter.Scan(&pending, &inProgress, &completed, &failed, &skipped) {
		stats.Pending += pending
		stats.InProgress += inProgress
		stats.Completed += completed
		stats.Failed += failed
		stats.Skipped += skipped
	}
	if err := iter.Close(); err != nil {
		return CampaignStats{}, err
	}
	stats.Total = stats.Pending + stats.InProgress + stats.Completed + stats.Failed + stats.Skipped
	return normalizeCampaignStats(stats), nil
}

func (s *QueryStore) CampaignStatsBatch(ctx context.Context, campaignIDs []uuid.UUID) (map[uuid.UUID]CampaignStats, error) {
	out := make(map[uuid.UUID]CampaignStats, len(campaignIDs))
	if len(campaignIDs) == 0 {
		return out, nil
	}

	sem := make(chan struct{}, 8)
	var mu sync.Mutex
	var wg sync.WaitGroup
	var firstErr error

	for _, id := range campaignIDs {
		wg.Add(1)
		go func(campaignID uuid.UUID) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				mu.Lock()
				if firstErr == nil {
					firstErr = ctx.Err()
				}
				mu.Unlock()
				return
			}
			defer func() { <-sem }()

			stats, err := s.CampaignStats(ctx, campaignID)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
				return
			}
			out[campaignID] = stats
		}(id)
	}
	wg.Wait()
	if firstErr != nil {
		return nil, firstErr
	}
	return out, nil
}

func normalizeCampaignStats(stats CampaignStats) CampaignStats {
	terminal := stats.Completed + stats.Failed + stats.Skipped
	if stats.Pending < 0 {
		stats.Pending = 0
	}
	if stats.InProgress < 0 {
		stats.InProgress = 0
	}
	if stats.Total < terminal {
		stats.Total = terminal
	}
	if terminal >= stats.Total && stats.Total > 0 {
		stats.Pending = 0
		stats.InProgress = 0
		stats.Total = terminal
	}
	return stats
}

func (s *QueryStore) ListCampaignCards(ctx context.Context, campaignID uuid.UUID, statusFilter string) ([]CampaignCardView, error) {
	var out []CampaignCardView
	for bucket := 0; bucket < s.client.BucketCount(); bucket++ {
		var query string
		var args []interface{}
		if statusFilter != "" {
			query = `SELECT status, updated_at, card_id, current_step, retry_count, last_msg_id, last_error_text
				FROM campaign_card_status_by_bucket
				WHERE campaign_id = ? AND campaign_bucket = ? AND status = ?`
			args = []interface{}{toGocqlUUID(campaignID), bucket, statusFilter}
		} else {
			query = `SELECT status, updated_at, card_id, current_step, retry_count, last_msg_id, last_error_text
				FROM campaign_card_status_by_bucket
				WHERE campaign_id = ? AND campaign_bucket = ?`
			args = []interface{}{toGocqlUUID(campaignID), bucket}
		}

		iter := s.client.Session().Query(query, args...).WithContext(ctx).Iter()
		rows, err := scanCampaignCardViews(iter)
		if err != nil {
			return nil, err
		}
		out = append(out, rows...)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].UpdatedAt.After(out[j].UpdatedAt)
	})
	return out, nil
}

func (s *QueryStore) ListFailedCardIDs(ctx context.Context, campaignID uuid.UUID) ([]uuid.UUID, error) {
	var out []uuid.UUID
	for bucket := 0; bucket < s.client.BucketCount(); bucket++ {
		iter := s.client.Session().Query(
			`SELECT card_id FROM failed_cards_by_campaign_bucket WHERE campaign_id = ? AND campaign_bucket = ?`,
			toGocqlUUID(campaignID), bucket,
		).WithContext(ctx).Iter()
		var cardID gocql.UUID
		for iter.Scan(&cardID) {
			out = append(out, uuid.UUID(cardID))
		}
		if err := iter.Close(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (s *QueryStore) ResetFailedCards(ctx context.Context, campaignID uuid.UUID, cardIDs []uuid.UUID, step int, updatedAt time.Time) error {
	for _, cardID := range cardIDs {
		cardStr := cardID.String()
		cardBucket := s.client.CardBucket(cardStr)
		campaignBucket := s.client.CampaignBucket(campaignID.String(), cardStr)

		var oldUpdatedAt time.Time
		var oldStatus string
		if err := s.client.Session().Query(
			`SELECT status, updated_at FROM card_state_by_card WHERE card_bucket = ? AND card_id = ?`,
			cardBucket, toGocqlUUID(cardID),
		).WithContext(ctx).Scan(&oldStatus, &oldUpdatedAt); err != nil && err != gocql.ErrNotFound {
			return err
		}

		if err := s.client.Session().Query(`INSERT INTO card_state_by_card (
			card_bucket, card_id, campaign_id, status, current_step, retry_count,
			last_msg_id, last_smpp_message_id, last_error_code, last_error_text, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			cardBucket, toGocqlUUID(cardID), toGocqlUUID(campaignID), "pending", step, 0, nil, "", "", "", updatedAt.UTC(),
		).WithContext(ctx).Exec(); err != nil {
			return err
		}

		if oldStatus != "" {
			_ = s.client.Session().Query(
				`DELETE FROM campaign_card_status_by_bucket WHERE campaign_id = ? AND campaign_bucket = ? AND status = ? AND updated_at = ? AND card_id = ?`,
				toGocqlUUID(campaignID), campaignBucket, oldStatus, oldUpdatedAt.UTC(), toGocqlUUID(cardID),
			).WithContext(ctx).Exec()
		}
		if err := s.client.Session().Query(
			`INSERT INTO campaign_card_status_by_bucket (campaign_id, campaign_bucket, status, updated_at, card_id, current_step, retry_count, last_msg_id, last_error_text)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			toGocqlUUID(campaignID), campaignBucket, "pending", updatedAt.UTC(), toGocqlUUID(cardID), step, 0, nil, "",
		).WithContext(ctx).Exec(); err != nil {
			return err
		}
		_ = s.client.Session().Query(
			`DELETE FROM failed_cards_by_campaign_bucket WHERE campaign_id = ? AND campaign_bucket = ? AND card_id = ?`,
			toGocqlUUID(campaignID), campaignBucket, toGocqlUUID(cardID),
		).WithContext(ctx).Exec()

		_ = s.client.Session().Query(
			`UPDATE campaign_progress_by_bucket SET failed = failed - 1, pending = pending + 1 WHERE campaign_id = ? AND campaign_bucket = ?`,
			toGocqlUUID(campaignID), campaignBucket,
		).WithContext(ctx).Exec()
	}
	return nil
}

func (s *QueryStore) AbortCampaign(ctx context.Context, campaignID uuid.UUID, updatedAt time.Time) (int, error) {
	skipped := 0
	batch := s.client.Session().NewBatch(gocql.UnloggedBatch).WithContext(ctx)
	type counterUpdate struct {
		campaignBucket int
		fromColumn     string
	}
	counterUpdates := make([]counterUpdate, 0, 128)
	flushBatch := func() error {
		if len(batch.Entries) == 0 {
			if len(counterUpdates) == 0 {
				return nil
			}
		} else {
			if err := s.client.Session().ExecuteBatch(batch); err != nil {
				return err
			}
			batch = s.client.Session().NewBatch(gocql.UnloggedBatch).WithContext(ctx)
		}
		for _, update := range counterUpdates {
			if err := s.client.Session().Query(
				fmt.Sprintf(`UPDATE campaign_progress_by_bucket SET %s = %s + ?, skipped = skipped + ? WHERE campaign_id = ? AND campaign_bucket = ?`,
					update.fromColumn, update.fromColumn),
				int64(-1), int64(1), toGocqlUUID(campaignID), update.campaignBucket,
			).WithContext(ctx).Exec(); err != nil {
				return err
			}
		}
		counterUpdates = counterUpdates[:0]
		return nil
	}
	for bucket := 0; bucket < s.client.BucketCount(); bucket++ {
		iter := s.client.Session().Query(
			`SELECT status, updated_at, card_id, current_step, retry_count, last_msg_id, last_error_text
			 FROM campaign_card_status_by_bucket
			 WHERE campaign_id = ? AND campaign_bucket = ?`,
			toGocqlUUID(campaignID), bucket,
		).WithContext(ctx).Iter()

		var (
			status       string
			rowUpdatedAt time.Time
			cardID       gocql.UUID
			currentStep  int
			retryCount   int
			lastMsgID    *gocql.UUID
			lastError    string
		)
		for iter.Scan(&status, &rowUpdatedAt, &cardID, &currentStep, &retryCount, &lastMsgID, &lastError) {
			if status == "completed" || status == "failed" || status == "skipped" {
				continue
			}

			cardUUID := uuid.UUID(cardID)
			cardIDStr := cardUUID.String()
			cardBucket := s.client.CardBucket(cardIDStr)
			campaignBucket := s.client.CampaignBucket(campaignID.String(), cardIDStr)

			batch.Query(`INSERT INTO card_state_by_card (
				card_bucket, card_id, campaign_id, status, current_step, retry_count,
				last_msg_id, last_smpp_message_id, last_error_code, last_error_text, updated_at
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				cardBucket, cardID, toGocqlUUID(campaignID), "skipped", currentStep, retryCount,
				lastMsgID, "", "", lastError, updatedAt.UTC(),
			)

			batch.Query(
				`INSERT INTO campaign_card_status_by_bucket (campaign_id, campaign_bucket, status, updated_at, card_id, current_step, retry_count, last_msg_id, last_error_text)
				 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				toGocqlUUID(campaignID), campaignBucket, "skipped", updatedAt.UTC(), cardID, currentStep, retryCount, lastMsgID, lastError,
			)
			batch.Query(
				`DELETE FROM campaign_card_status_by_bucket WHERE campaign_id = ? AND campaign_bucket = ? AND status = ? AND updated_at = ? AND card_id = ?`,
				toGocqlUUID(campaignID), campaignBucket, status, rowUpdatedAt.UTC(), cardID,
			)
			batch.Query(
				`DELETE FROM failed_cards_by_campaign_bucket WHERE campaign_id = ? AND campaign_bucket = ? AND card_id = ?`,
				toGocqlUUID(campaignID), campaignBucket, cardID,
			)

			fromColumn := counterColumn(status)
			if fromColumn != counterColumn("skipped") {
				counterUpdates = append(counterUpdates, counterUpdate{
					campaignBucket: campaignBucket,
					fromColumn:     fromColumn,
				})
			}
			if len(batch.Entries) >= 200 || len(counterUpdates) >= 200 {
				if err := flushBatch(); err != nil {
					return skipped, err
				}
			}
			skipped++
		}
		if err := iter.Close(); err != nil {
			return skipped, err
		}
	}
	if err := flushBatch(); err != nil {
		return skipped, err
	}

	return skipped, nil
}

func toGocqlUUID(id uuid.UUID) gocql.UUID {
	return gocql.UUID(id)
}

func (s *QueryStore) GetMessage(ctx context.Context, msgID uuid.UUID) (*MessageRecord, error) {
	var (
		cardID         gocql.UUID
		campaignID     *gocql.UUID
		createdAt      time.Time
		updatedAt      time.Time
		direction      string
		status         string
		smppMessageID  *string
		dlrStatus      *string
		counterHex     *string
		porStatusCode  *int16
		rawPayload     []byte
		securedPayload []byte
		porData        []byte
	)

	err := s.client.Session().Query(
		`SELECT card_id, campaign_id, created_at, updated_at, direction, status, smpp_message_id, dlr_status, counter_hex, por_status_code, raw_payload, secured_payload, por_data
		 FROM message_by_id WHERE msg_id = ?`,
		toGocqlUUID(msgID),
	).WithContext(ctx).Scan(&cardID, &campaignID, &createdAt, &updatedAt, &direction, &status, &smppMessageID, &dlrStatus, &counterHex, &porStatusCode, &rawPayload, &securedPayload, &porData)
	if err == gocql.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	record := &MessageRecord{
		ID:             msgID,
		CardID:         uuid.UUID(cardID),
		Direction:      direction,
		Status:         status,
		SMPPMessageID:  smppMessageID,
		DLRStatus:      dlrStatus,
		CounterHex:     counterHex,
		PORStatusCode:  porStatusCode,
		RawPayload:     rawPayload,
		SecuredPayload: securedPayload,
		PORData:        porData,
		CreatedAt:      createdAt.UTC(),
		UpdatedAt:      updatedAt.UTC(),
	}
	if campaignID != nil {
		cid := uuid.UUID(*campaignID)
		record.CampaignID = &cid
	}
	return record, nil
}

func (s *QueryStore) ListCardMessages(ctx context.Context, cardID uuid.UUID, limit int) ([]MessageRecord, error) {
	if limit <= 0 {
		limit = 10
	}
	return s.listRecentCardMessages(ctx, cardID, time.Now().UTC().Add(-30*24*time.Hour), time.Now().UTC(), limit)
}

func (s *QueryStore) ListMessages(ctx context.Context, filter MessageFilter) ([]MessageRecord, int64, error) {
	if filter.Page <= 0 {
		filter.Page = 1
	}
	if filter.PageSize <= 0 {
		filter.PageSize = 20
	}
	if filter.To.IsZero() {
		filter.To = time.Now().UTC()
	}
	if filter.From.IsZero() {
		filter.From = filter.To.Add(-24 * time.Hour)
	}

	maxKeep := filter.Page * filter.PageSize
	collector := newMessageCollector(maxKeep)
	var err error
	switch {
	case filter.CardID != nil:
		err = s.queryByCard(ctx, *filter.CardID, filter, collector)
	case filter.CampaignID != nil:
		err = s.queryByCampaign(ctx, *filter.CampaignID, filter, collector)
	default:
		err = s.queryGlobal(ctx, filter, collector)
	}
	if err != nil {
		return nil, 0, err
	}
	return collector.Page(filter.Page, filter.PageSize), collector.Total(), nil
}

func (s *QueryStore) RecentActivity(ctx context.Context, limit int) ([]MessageRecord, error) {
	if limit <= 0 {
		limit = 50
	}
	return s.listRecentGlobalMessages(ctx, time.Now().UTC().Add(-24*time.Hour), time.Now().UTC(), limit)
}

func (s *QueryStore) MessageMetrics(ctx context.Context, since time.Time) (MessageMetrics, error) {
	points, err := s.loadHourlyMetrics(ctx, since.UTC(), time.Now().UTC())
	if err != nil {
		return MessageMetrics{}, err
	}
	return aggregateMessageMetrics(points), nil
}

func (s *QueryStore) CampaignMessageMetrics(ctx context.Context, campaignID uuid.UUID, since time.Time) (MessageMetrics, error) {
	points, err := s.loadCampaignHourlyMetrics(ctx, campaignID, since.UTC(), time.Now().UTC())
	if err != nil {
		return MessageMetrics{}, err
	}
	return aggregateMessageMetrics(points), nil
}

func aggregateMessageMetrics(points []hourlyMessageMetrics) MessageMetrics {
	var out MessageMetrics
	for _, point := range points {
		out.Total += point.Total
		out.MT += point.MT
		out.MO += point.MO
		out.Delivered += point.Delivered
		out.Undelivered += point.Undelivered
	}
	return out
}

func (s *QueryStore) Throughput(ctx context.Context, since time.Time) ([]ThroughputPoint, error) {
	metrics, err := s.loadHourlyMetrics(ctx, since.UTC(), time.Now().UTC())
	if err != nil {
		return nil, err
	}
	return throughputFromHourlyMetrics(metrics), nil
}

func (s *QueryStore) CampaignThroughput(ctx context.Context, campaignID uuid.UUID, since time.Time) ([]ThroughputPoint, error) {
	metrics, err := s.loadCampaignHourlyMetrics(ctx, campaignID, since.UTC(), time.Now().UTC())
	if err != nil {
		return nil, err
	}
	return throughputFromHourlyMetrics(metrics), nil
}

func throughputFromHourlyMetrics(metrics []hourlyMessageMetrics) []ThroughputPoint {
	points := make([]ThroughputPoint, 0, len(metrics))
	for _, metric := range metrics {
		points = append(points, ThroughputPoint{
			Timestamp: metric.Timestamp,
			Sent:      float64(metric.MT),
			Delivered: float64(metric.Delivered),
			Failed:    float64(metric.Undelivered),
		})
	}
	sort.Slice(points, func(i, j int) bool { return points[i].Timestamp.Before(points[j].Timestamp) })
	return points
}

// MinuteThroughput returns per-minute average TPS for the last hour.
func (s *QueryStore) MinuteThroughput(ctx context.Context) ([]ThroughputPoint, error) {
	now := time.Now().UTC()
	since := now.Add(-1 * time.Hour)
	metrics, err := s.loadMinuteMetrics(ctx, since, now)
	if err != nil {
		return nil, err
	}
	points := make([]ThroughputPoint, 0, len(metrics))
	for _, m := range metrics {
		points = append(points, ThroughputPoint{
			Timestamp: m.Timestamp,
			Sent:      float64(m.MT) / 60.0,
			Delivered: float64(m.Delivered) / 60.0,
			Failed:    float64(m.Undelivered) / 60.0,
		})
	}
	sort.Slice(points, func(i, j int) bool { return points[i].Timestamp.Before(points[j].Timestamp) })
	return points, nil
}

func (s *QueryStore) loadMinuteMetrics(ctx context.Context, since, until time.Time) ([]hourlyMessageMetrics, error) {
	start := since.UTC().Truncate(time.Minute)
	end := until.UTC().Truncate(time.Minute)
	out := make([]hourlyMessageMetrics, 0, int(end.Sub(start)/time.Minute)+1)
	for bucket := start; !bucket.After(end); bucket = bucket.Add(time.Minute) {
		var total, mt, mo, delivered, undelivered int64
		err := s.client.Session().Query(
			`SELECT total_messages, mt_messages, mo_messages, delivered_messages, undelivered_messages
			 FROM message_metrics_by_minute WHERE minute_bucket = ?`,
			bucket,
		).WithContext(ctx).Consistency(gocql.One).Scan(&total, &mt, &mo, &delivered, &undelivered)
		if err == gocql.ErrNotFound {
			out = append(out, hourlyMessageMetrics{Timestamp: bucket})
			continue
		}
		if err != nil {
			return nil, err
		}
		out = append(out, hourlyMessageMetrics{
			Timestamp:   bucket,
			Total:       total,
			MT:          mt,
			MO:          mo,
			Delivered:   delivered,
			Undelivered: undelivered,
		})
	}
	return out, nil
}

func (s *QueryStore) ErrorSummary(ctx context.Context, since time.Time) (status []ErrorCount, dlr []ErrorCount, por []ErrorCount, err error) {
	return s.loadGlobalErrorSummary(ctx, since.UTC(), time.Now().UTC())
}

func (s *QueryStore) CampaignErrorSummary(ctx context.Context, campaignID uuid.UUID, since time.Time) (status []ErrorCount, dlr []ErrorCount, por []ErrorCount, err error) {
	return s.loadCampaignErrorSummary(ctx, campaignID, since.UTC(), time.Now().UTC())
}

func (s *QueryStore) queryByCard(ctx context.Context, cardID uuid.UUID, filter MessageFilter, collector *messageCollector) error {
	cardBucket := s.client.CardBucket(cardID.String())
	for _, day := range dayBuckets(filter.From, filter.To) {
		iter := s.client.Session().Query(
			`SELECT created_at, msg_id, campaign_id, updated_at, direction, status, smpp_message_id, dlr_status, counter_hex, por_status_code
			 FROM message_by_card_time WHERE card_bucket = ? AND card_id = ? AND time_bucket = ?`,
			cardBucket, toGocqlUUID(cardID), day,
		).WithContext(ctx).Iter()
		rows, err := scanMessageRowsByCard(iter, cardID)
		if err != nil {
			return err
		}
		collector.AddAll(rows, filter)
	}
	return nil
}

func (s *QueryStore) queryByCampaign(ctx context.Context, campaignID uuid.UUID, filter MessageFilter, collector *messageCollector) error {
	rows, err := s.parallelMessageScan(ctx, dayBuckets(filter.From, filter.To), func(ctx context.Context, day string, bucket int) ([]MessageRecord, error) {
		iter := s.client.Session().Query(
			`SELECT created_at, card_id, msg_id, updated_at, direction, status, smpp_message_id, dlr_status, por_status_code
			 FROM message_by_campaign_bucket_time WHERE campaign_id = ? AND campaign_bucket = ? AND time_bucket = ?`,
			toGocqlUUID(campaignID), bucket, day,
		).WithContext(ctx).Iter()
		return scanMessageRowsByCampaign(iter, campaignID)
	})
	if err != nil {
		return err
	}
	collector.AddAll(rows, filter)
	return nil
}

func (s *QueryStore) queryGlobal(ctx context.Context, filter MessageFilter, collector *messageCollector) error {
	if filter.Status != "" {
		return s.queryGlobalByStatus(ctx, filter, collector)
	}
	if filter.Direction != "" {
		return s.queryGlobalByDirection(ctx, filter, collector)
	}

	rows, err := s.parallelMessageScan(ctx, dayBuckets(filter.From, filter.To), func(ctx context.Context, day string, bucket int) ([]MessageRecord, error) {
		iter := s.client.Session().Query(
			`SELECT created_at, msg_id, card_id, campaign_id, updated_at, direction, status, smpp_message_id, dlr_status, por_status_code
			 FROM message_by_time_bucket WHERE time_bucket = ? AND global_bucket = ?`,
			day, bucket,
		).WithContext(ctx).Iter()
		return scanMessageRowsGlobal(iter)
	})
	if err != nil {
		return err
	}
	collector.AddAll(rows, filter)
	return nil
}

func (s *QueryStore) queryGlobalByDirection(ctx context.Context, filter MessageFilter, collector *messageCollector) error {
	rows, err := s.parallelMessageScan(ctx, dayBuckets(filter.From, filter.To), func(ctx context.Context, day string, bucket int) ([]MessageRecord, error) {
		iter := s.client.Session().Query(
			`SELECT created_at, msg_id, card_id, campaign_id, updated_at, direction, status, smpp_message_id, dlr_status, por_status_code
			 FROM message_by_direction_time_bucket WHERE direction = ? AND time_bucket = ? AND global_bucket = ?`,
			filter.Direction, day, bucket,
		).WithContext(ctx).Iter()
		return scanMessageRowsGlobal(iter)
	})
	if err != nil {
		return err
	}
	collector.AddAll(rows, filter)
	return nil
}

func (s *QueryStore) queryGlobalByStatus(ctx context.Context, filter MessageFilter, collector *messageCollector) error {
	statusRows, err := s.parallelMessageScan(ctx, dayBuckets(filter.From, filter.To), func(ctx context.Context, day string, bucket int) ([]MessageRecord, error) {
		iter := s.client.Session().Query(
			`SELECT created_at, msg_id, card_id, campaign_id, updated_at, direction, status, smpp_message_id, dlr_status, por_status_code
			 FROM message_by_status_time_bucket WHERE status = ? AND time_bucket = ? AND global_bucket = ?`,
			filter.Status, day, bucket,
		).WithContext(ctx).Iter()
		return scanMessageRowsGlobal(iter)
	})
	if err != nil {
		return err
	}
	collector.AddAll(statusRows, filter)

	dlrRows, err := s.parallelMessageScan(ctx, dayBuckets(filter.From, filter.To), func(ctx context.Context, day string, bucket int) ([]MessageRecord, error) {
		iter := s.client.Session().Query(
			`SELECT created_at, msg_id, card_id, campaign_id, updated_at, direction, status, smpp_message_id, dlr_status, por_status_code
			 FROM message_by_dlr_status_time_bucket WHERE dlr_status = ? AND time_bucket = ? AND global_bucket = ?`,
			filter.Status, day, bucket,
		).WithContext(ctx).Iter()
		return scanMessageRowsGlobal(iter)
	})
	if err != nil {
		return err
	}
	collector.AddAll(dlrRows, filter)
	return nil
}

func scanMessageRowsByCard(iter *gocql.Iter, cardID uuid.UUID) ([]MessageRecord, error) {
	out := make([]MessageRecord, 0)
	var (
		createdAt     time.Time
		msgID         gocql.UUID
		campaignID    *gocql.UUID
		updatedAt     time.Time
		direction     string
		status        string
		smppMessageID *string
		dlrStatus     *string
		counterHex    *string
		porStatusCode *int16
	)
	for iter.Scan(&createdAt, &msgID, &campaignID, &updatedAt, &direction, &status, &smppMessageID, &dlrStatus, &counterHex, &porStatusCode) {
		record := MessageRecord{
			ID:            uuid.UUID(msgID),
			CardID:        cardID,
			Direction:     direction,
			Status:        status,
			SMPPMessageID: smppMessageID,
			DLRStatus:     dlrStatus,
			CounterHex:    counterHex,
			PORStatusCode: porStatusCode,
			CreatedAt:     createdAt.UTC(),
			UpdatedAt:     updatedAt.UTC(),
		}
		if campaignID != nil {
			cid := uuid.UUID(*campaignID)
			record.CampaignID = &cid
		}
		out = append(out, record)
	}
	if err := iter.Close(); err != nil {
		return nil, err
	}
	return out, nil
}

func scanMessageRowsByCampaign(iter *gocql.Iter, campaignID uuid.UUID) ([]MessageRecord, error) {
	out := make([]MessageRecord, 0)
	var (
		createdAt     time.Time
		cardID        gocql.UUID
		msgID         gocql.UUID
		updatedAt     time.Time
		direction     string
		status        string
		smppMessageID *string
		dlrStatus     *string
		porStatusCode *int16
	)
	for iter.Scan(&createdAt, &cardID, &msgID, &updatedAt, &direction, &status, &smppMessageID, &dlrStatus, &porStatusCode) {
		cid := campaignID
		out = append(out, MessageRecord{
			ID:            uuid.UUID(msgID),
			CampaignID:    &cid,
			CardID:        uuid.UUID(cardID),
			Direction:     direction,
			Status:        status,
			SMPPMessageID: smppMessageID,
			DLRStatus:     dlrStatus,
			PORStatusCode: porStatusCode,
			CreatedAt:     createdAt.UTC(),
			UpdatedAt:     updatedAt.UTC(),
		})
	}
	if err := iter.Close(); err != nil {
		return nil, err
	}
	return out, nil
}

func scanMessageRowsGlobal(iter *gocql.Iter) ([]MessageRecord, error) {
	out := make([]MessageRecord, 0)
	var (
		createdAt     time.Time
		msgID         gocql.UUID
		cardID        gocql.UUID
		campaignID    *gocql.UUID
		updatedAt     time.Time
		direction     string
		status        string
		smppMessageID *string
		dlrStatus     *string
		porStatusCode *int16
	)
	for iter.Scan(&createdAt, &msgID, &cardID, &campaignID, &updatedAt, &direction, &status, &smppMessageID, &dlrStatus, &porStatusCode) {
		record := MessageRecord{
			ID:            uuid.UUID(msgID),
			CardID:        uuid.UUID(cardID),
			Direction:     direction,
			Status:        status,
			SMPPMessageID: smppMessageID,
			DLRStatus:     dlrStatus,
			PORStatusCode: porStatusCode,
			CreatedAt:     createdAt.UTC(),
			UpdatedAt:     updatedAt.UTC(),
		}
		if campaignID != nil {
			cid := uuid.UUID(*campaignID)
			record.CampaignID = &cid
		}
		out = append(out, record)
	}
	if err := iter.Close(); err != nil {
		return nil, err
	}
	return out, nil
}

func filterMessages(in []MessageRecord, filter MessageFilter) []MessageRecord {
	out := make([]MessageRecord, 0, len(in))
	for _, record := range in {
		if !record.CreatedAt.Before(filter.From) && !record.CreatedAt.After(filter.To) {
			if filter.Direction != "" && record.Direction != filter.Direction {
				continue
			}
			if filter.Status != "" && record.Status != filter.Status {
				if record.DLRStatus == nil || *record.DLRStatus != filter.Status {
					continue
				}
			}
			out = append(out, record)
		}
	}
	return out
}

type messageCollector struct {
	limit int
	total int64
	heap  messageMinHeap
}

func newMessageCollector(limit int) *messageCollector {
	if limit <= 0 {
		limit = 1
	}
	h := make(messageMinHeap, 0, limit)
	heap.Init(&h)
	return &messageCollector{limit: limit, heap: h}
}

func (c *messageCollector) AddAll(records []MessageRecord, filter MessageFilter) {
	for _, record := range records {
		if !matchesMessageFilter(record, filter) {
			continue
		}
		c.total++
		if c.heap.Len() < c.limit {
			heap.Push(&c.heap, record)
			continue
		}
		if record.CreatedAt.After(c.heap[0].CreatedAt) {
			heap.Pop(&c.heap)
			heap.Push(&c.heap, record)
		}
	}
}

func (c *messageCollector) Total() int64 {
	return c.total
}

func (c *messageCollector) Page(page, pageSize int) []MessageRecord {
	out := make([]MessageRecord, c.heap.Len())
	for i := len(out) - 1; i >= 0; i-- {
		out[i] = heap.Pop(&c.heap).(MessageRecord)
	}
	start := (page - 1) * pageSize
	if start >= len(out) {
		return []MessageRecord{}
	}
	end := start + pageSize
	if end > len(out) {
		end = len(out)
	}
	return out[start:end]
}

type messageMinHeap []MessageRecord

func (h messageMinHeap) Len() int           { return len(h) }
func (h messageMinHeap) Less(i, j int) bool { return h[i].CreatedAt.Before(h[j].CreatedAt) }
func (h messageMinHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *messageMinHeap) Push(x interface{}) {
	*h = append(*h, x.(MessageRecord))
}

func (h *messageMinHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

func matchesMessageFilter(record MessageRecord, filter MessageFilter) bool {
	if record.CreatedAt.Before(filter.From) || record.CreatedAt.After(filter.To) {
		return false
	}
	if filter.Direction != "" && record.Direction != filter.Direction {
		return false
	}
	if filter.Status != "" && record.Status != filter.Status {
		if record.DLRStatus == nil || *record.DLRStatus != filter.Status {
			return false
		}
	}
	return true
}

func scanCampaignCardViews(iter *gocql.Iter) ([]CampaignCardView, error) {
	out := make([]CampaignCardView, 0)
	var (
		status      string
		updatedAt   time.Time
		cardID      gocql.UUID
		currentStep int
		retryCount  int
		lastMsgID   *gocql.UUID
		lastError   string
	)
	for iter.Scan(&status, &updatedAt, &cardID, &currentStep, &retryCount, &lastMsgID, &lastError) {
		view := CampaignCardView{
			CardID:      uuid.UUID(cardID).String(),
			Status:      status,
			CurrentStep: currentStep,
			RetryCount:  retryCount,
			LastError:   lastError,
			UpdatedAt:   updatedAt.UTC(),
		}
		if lastMsgID != nil {
			view.LastMsgID = uuid.UUID(*lastMsgID).String()
		}
		out = append(out, view)
	}
	if err := iter.Close(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *QueryStore) parallelMessageScan(ctx context.Context, days []string, fn func(context.Context, string, int) ([]MessageRecord, error)) ([]MessageRecord, error) {
	type result struct {
		rows []MessageRecord
		err  error
	}

	const maxConcurrent = 16
	sem := make(chan struct{}, maxConcurrent)
	results := make(chan result, len(days)*s.client.BucketCount())
	var wg sync.WaitGroup

	for _, day := range days {
		for bucket := 0; bucket < s.client.BucketCount(); bucket++ {
			day := day
			bucket := bucket
			wg.Add(1)
			go func() {
				defer wg.Done()
				select {
				case sem <- struct{}{}:
				case <-ctx.Done():
					results <- result{err: ctx.Err()}
					return
				}
				defer func() { <-sem }()

				rows, err := fn(ctx, day, bucket)
				results <- result{rows: rows, err: err}
			}()
		}
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	out := make([]MessageRecord, 0)
	for res := range results {
		if res.err != nil {
			return nil, res.err
		}
		out = append(out, res.rows...)
	}
	return out, nil
}

func countsFromMap(in map[string]int64) []ErrorCount {
	out := make([]ErrorCount, 0, len(in))
	for key, count := range in {
		out = append(out, ErrorCount{Key: key, Count: count})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count == out[j].Count {
			return out[i].Key < out[j].Key
		}
		return out[i].Count > out[j].Count
	})
	return out
}

func dayBuckets(from, to time.Time) []string {
	from = from.UTC()
	to = to.UTC()
	start := time.Date(from.Year(), from.Month(), from.Day(), 0, 0, 0, 0, time.UTC)
	end := time.Date(to.Year(), to.Month(), to.Day(), 0, 0, 0, 0, time.UTC)
	var days []string
	for day := start; !day.After(end); day = day.Add(24 * time.Hour) {
		days = append(days, day.Format("2006-01-02"))
	}
	return days
}

func dayBucketsDesc(from, to time.Time) []string {
	days := dayBuckets(from, to)
	for i, j := 0, len(days)-1; i < j; i, j = i+1, j-1 {
		days[i], days[j] = days[j], days[i]
	}
	return days
}

func (s *QueryStore) listRecentCardMessages(ctx context.Context, cardID uuid.UUID, from, to time.Time, limit int) ([]MessageRecord, error) {
	if limit <= 0 {
		return []MessageRecord{}, nil
	}

	cardBucket := s.client.CardBucket(cardID.String())
	out := make([]MessageRecord, 0, limit)
	for _, day := range dayBucketsDesc(from, to) {
		iter := s.client.Session().Query(
			`SELECT created_at, msg_id, campaign_id, updated_at, direction, status, smpp_message_id, dlr_status, counter_hex, por_status_code
			 FROM message_by_card_time WHERE card_bucket = ? AND card_id = ? AND time_bucket = ? LIMIT ?`,
			cardBucket, toGocqlUUID(cardID), day, limit,
		).WithContext(ctx).Iter()
		rows, err := scanMessageRowsByCard(iter, cardID)
		if err != nil {
			return nil, err
		}
		out = append(out, rows...)
		if len(out) >= limit {
			break
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *QueryStore) listRecentGlobalMessages(ctx context.Context, from, to time.Time, limit int) ([]MessageRecord, error) {
	if limit <= 0 {
		return []MessageRecord{}, nil
	}

	out := make([]MessageRecord, 0, limit*s.client.BucketCount())
	for _, day := range dayBucketsDesc(from, to) {
		rows, err := s.parallelMessageScan(ctx, []string{day}, func(ctx context.Context, day string, bucket int) ([]MessageRecord, error) {
			iter := s.client.Session().Query(
				`SELECT created_at, msg_id, card_id, campaign_id, updated_at, direction, status, smpp_message_id, dlr_status, por_status_code
				 FROM message_by_time_bucket WHERE time_bucket = ? AND global_bucket = ? LIMIT ?`,
				day, bucket, limit,
			).WithContext(ctx).Iter()
			return scanMessageRowsGlobal(iter)
		})
		if err != nil {
			return nil, err
		}
		out = append(out, rows...)
		if len(out) >= limit {
			break
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func IsFailedDLR(status string) bool {
	switch status {
	case "FAILED", "UNDELIV", "EXPIRED", "DELETED", "REJECTD":
		return true
	default:
		return false
	}
}

func (s *QueryStore) loadHourlyMetrics(ctx context.Context, since, until time.Time) ([]hourlyMessageMetrics, error) {
	start := since.UTC().Truncate(time.Hour)
	end := until.UTC().Truncate(time.Hour)
	out := make([]hourlyMessageMetrics, 0)
	for bucket := start; !bucket.After(end); bucket = bucket.Add(time.Hour) {
		var total, mt, mo, delivered, undelivered int64
		err := s.client.Session().Query(
			`SELECT total_messages, mt_messages, mo_messages, delivered_messages, undelivered_messages
			 FROM message_metrics_by_hour WHERE hour_bucket = ?`,
			bucket,
		).WithContext(ctx).Consistency(gocql.One).Scan(&total, &mt, &mo, &delivered, &undelivered)
		if err == gocql.ErrNotFound {
			out = append(out, hourlyMessageMetrics{Timestamp: bucket})
			continue
		}
		if err != nil {
			return nil, err
		}
		out = append(out, hourlyMessageMetrics{
			Timestamp:   bucket,
			Total:       total,
			MT:          mt,
			MO:          mo,
			Delivered:   delivered,
			Undelivered: undelivered,
		})
	}
	return out, nil
}

func (s *QueryStore) loadCampaignHourlyMetrics(ctx context.Context, campaignID uuid.UUID, since, until time.Time) ([]hourlyMessageMetrics, error) {
	start := since.UTC().Truncate(time.Hour)
	end := until.UTC().Truncate(time.Hour)
	out := make([]hourlyMessageMetrics, 0, int(end.Sub(start)/time.Hour)+1)
	for bucket := start; !bucket.After(end); bucket = bucket.Add(time.Hour) {
		var total, mt, mo, delivered, undelivered int64
		for campaignBucket := 0; campaignBucket < s.client.BucketCount(); campaignBucket++ {
			var rowTotal, rowMT, rowMO, rowDelivered, rowUndelivered int64
			err := s.client.Session().Query(
				`SELECT total_messages, mt_messages, mo_messages, delivered_messages, undelivered_messages
				 FROM campaign_message_metrics_by_hour WHERE campaign_id = ? AND campaign_bucket = ? AND hour_bucket = ?`,
				toGocqlUUID(campaignID), campaignBucket, bucket,
			).WithContext(ctx).Consistency(gocql.One).Scan(&rowTotal, &rowMT, &rowMO, &rowDelivered, &rowUndelivered)
			if err == gocql.ErrNotFound {
				continue
			}
			if err != nil {
				return nil, err
			}
			total += rowTotal
			mt += rowMT
			mo += rowMO
			delivered += rowDelivered
			undelivered += rowUndelivered
		}
		out = append(out, hourlyMessageMetrics{
			Timestamp:   bucket,
			Total:       total,
			MT:          mt,
			MO:          mo,
			Delivered:   delivered,
			Undelivered: undelivered,
		})
	}
	return out, nil
}

func (s *QueryStore) loadGlobalErrorSummary(ctx context.Context, since, until time.Time) ([]ErrorCount, []ErrorCount, []ErrorCount, error) {
	statusMap := map[string]int64{}
	dlrMap := map[string]int64{}
	porMap := map[string]int64{}
	for bucket := since.UTC().Truncate(time.Hour); !bucket.After(until.UTC().Truncate(time.Hour)); bucket = bucket.Add(time.Hour) {
		iter := s.client.Session().Query(
			`SELECT error_kind, error_key, error_count FROM message_error_counts_by_hour WHERE hour_bucket = ?`,
			bucket,
		).WithContext(ctx).Iter()

		var kind, key string
		var count int64
		for iter.Scan(&kind, &key, &count) {
			accumulateErrorCount(kind, key, count, statusMap, dlrMap, porMap)
		}
		if err := iter.Close(); err != nil {
			return nil, nil, nil, err
		}
	}
	return countsFromMap(statusMap), countsFromMap(dlrMap), countsFromMap(porMap), nil
}

func (s *QueryStore) loadCampaignErrorSummary(ctx context.Context, campaignID uuid.UUID, since, until time.Time) ([]ErrorCount, []ErrorCount, []ErrorCount, error) {
	statusMap := map[string]int64{}
	dlrMap := map[string]int64{}
	porMap := map[string]int64{}
	for hourBucket := since.UTC().Truncate(time.Hour); !hourBucket.After(until.UTC().Truncate(time.Hour)); hourBucket = hourBucket.Add(time.Hour) {
		for campaignBucket := 0; campaignBucket < s.client.BucketCount(); campaignBucket++ {
			iter := s.client.Session().Query(
				`SELECT error_kind, error_key, error_count
				 FROM campaign_message_error_counts_by_hour
				 WHERE campaign_id = ? AND campaign_bucket = ? AND hour_bucket = ?`,
				toGocqlUUID(campaignID), campaignBucket, hourBucket,
			).WithContext(ctx).Iter()

			var kind, key string
			var count int64
			for iter.Scan(&kind, &key, &count) {
				accumulateErrorCount(kind, key, count, statusMap, dlrMap, porMap)
			}
			if err := iter.Close(); err != nil {
				return nil, nil, nil, err
			}
		}
	}
	return countsFromMap(statusMap), countsFromMap(dlrMap), countsFromMap(porMap), nil
}

func accumulateErrorCount(kind, key string, count int64, statusMap, dlrMap, porMap map[string]int64) {
	switch kind {
	case "status":
		statusMap[key] += count
	case "dlr":
		dlrMap[key] += count
	case "por":
		porMap[key] += count
	}
}
