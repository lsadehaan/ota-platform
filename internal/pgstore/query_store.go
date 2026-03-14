package pgstore

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"ota-platform/internal/db"
	"ota-platform/internal/store"
)

// QueryStore implements controlplane.QueryStore backed by Postgres/TimescaleDB.
type QueryStore struct {
	db *gorm.DB
}

// NewQueryStore creates a new Postgres-backed QueryStore.
func NewQueryStore(database *gorm.DB) *QueryStore {
	return &QueryStore{db: database}
}

// CampaignStats reads aggregate counters from the campaign_stats table (O(1)).
func (s *QueryStore) CampaignStats(ctx context.Context, campaignID uuid.UUID) (store.CampaignStats, error) {
	var row db.CampaignStatsRow
	if err := s.db.WithContext(ctx).Where("campaign_id = ?", campaignID).First(&row).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return store.CampaignStats{}, nil
		}
		return store.CampaignStats{}, fmt.Errorf("read campaign stats: %w", err)
	}
	return store.CampaignStats{
		Total:      row.Total,
		Pending:    row.Pending,
		InProgress: row.InProgress,
		Completed:  row.Completed,
		Failed:     row.Failed,
		Skipped:    row.Skipped,
	}, nil
}

// CampaignStatsBatch reads stats for multiple campaigns in a single query.
func (s *QueryStore) CampaignStatsBatch(ctx context.Context, campaignIDs []uuid.UUID) (map[uuid.UUID]store.CampaignStats, error) {
	out := make(map[uuid.UUID]store.CampaignStats, len(campaignIDs))
	if len(campaignIDs) == 0 {
		return out, nil
	}
	var rows []db.CampaignStatsRow
	if err := s.db.WithContext(ctx).Where("campaign_id IN ?", campaignIDs).Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("batch read campaign stats: %w", err)
	}
	for _, row := range rows {
		out[row.CampaignID] = store.CampaignStats{
			Total:      row.Total,
			Pending:    row.Pending,
			InProgress: row.InProgress,
			Completed:  row.Completed,
			Failed:     row.Failed,
			Skipped:    row.Skipped,
		}
	}
	return out, nil
}

// ListCampaignCards returns the latest card execution state per card for a campaign,
// optionally filtered by status. Uses DISTINCT ON to read from append-only table.
func (s *QueryStore) ListCampaignCards(ctx context.Context, campaignID uuid.UUID, statusFilter string) ([]store.CampaignCardView, error) {
	// Subquery: get latest row per card_id (highest transition_seq).
	subquery := `
		SELECT DISTINCT ON (card_id) *
		FROM card_execution_states
		WHERE campaign_id = ?
		ORDER BY card_id, transition_seq DESC`

	query := s.db.WithContext(ctx).
		Table("(?) AS latest", gorm.Expr(subquery, campaignID))
	if statusFilter != "" {
		query = query.Where("status = ?", statusFilter)
	}
	query = query.Order("updated_at DESC")

	var rows []db.CardExecutionState
	if err := query.Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("list campaign cards: %w", err)
	}

	views := make([]store.CampaignCardView, 0, len(rows))
	for _, r := range rows {
		var lastMsgID string
		if r.LastMsgID != nil {
			lastMsgID = r.LastMsgID.String()
		}
		views = append(views, store.CampaignCardView{
			CardID:      r.CardID.String(),
			Status:      r.Status,
			CurrentStep: r.CurrentStep,
			RetryCount:  r.RetryCount,
			LastMsgID:   lastMsgID,
			LastError:   r.LastError,
			UpdatedAt:   r.UpdatedAt,
		})
	}
	return views, nil
}

// ListFailedCardIDs returns IDs of all cards whose latest state is 'failed'.
func (s *QueryStore) ListFailedCardIDs(ctx context.Context, campaignID uuid.UUID) ([]uuid.UUID, error) {
	var cardIDs []uuid.UUID
	if err := s.db.WithContext(ctx).Raw(`
		SELECT card_id FROM (
			SELECT DISTINCT ON (card_id) card_id, status
			FROM card_execution_states
			WHERE campaign_id = ?
			ORDER BY card_id, transition_seq DESC
		) latest WHERE status = 'failed'`, campaignID).Pluck("card_id", &cardIDs).Error; err != nil {
		return nil, fmt.Errorf("list failed card IDs: %w", err)
	}
	return cardIDs, nil
}

// ResetFailedCards appends new 'pending' rows for failed cards and updates campaign_stats.
func (s *QueryStore) ResetFailedCards(ctx context.Context, campaignID uuid.UUID, cardIDs []uuid.UUID, step int, updatedAt time.Time) error {
	if len(cardIDs) == 0 {
		return nil
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Append-only: INSERT a new row with status='pending' and transition_seq+1
		// for each card whose latest state is 'failed'.
		result := tx.Exec(`
			INSERT INTO card_execution_states
				(card_id, campaign_id, status, current_step, retry_count, last_error, transition_seq, updated_at)
			SELECT card_id, campaign_id, 'pending', ?, 0, '', transition_seq + 1, ?
			FROM (
				SELECT DISTINCT ON (card_id) card_id, campaign_id, transition_seq
				FROM card_execution_states
				WHERE campaign_id = ? AND card_id IN ?
				ORDER BY card_id, transition_seq DESC
			) latest`,
			step, updatedAt, campaignID, cardIDs,
		)
		if result.Error != nil {
			return fmt.Errorf("reset card states: %w", result.Error)
		}
		resetCount := result.RowsAffected
		if resetCount == 0 {
			return nil
		}
		// Adjust campaign_stats: failed -= N, pending += N
		if err := tx.Exec(
			`UPDATE campaign_stats SET failed = failed - ?, pending = pending + ? WHERE campaign_id = ?`,
			resetCount, resetCount, campaignID,
		).Error; err != nil {
			return fmt.Errorf("update campaign stats for reset: %w", err)
		}
		return nil
	})
}

// InitializeCampaign batch-inserts card_execution_states rows and creates a campaign_stats row.
func (s *QueryStore) InitializeCampaign(ctx context.Context, campaignID uuid.UUID, cardIDs []uuid.UUID, initialStep int, updatedAt time.Time) error {
	if len(cardIDs) == 0 {
		return nil
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		const batchSize = 500
		for i := 0; i < len(cardIDs); i += batchSize {
			end := i + batchSize
			if end > len(cardIDs) {
				end = len(cardIDs)
			}
			// Append-only: INSERT initial pending rows with transition_seq=0.
		// ON CONFLICT DO NOTHING handles duplicate inserts safely (e.g. retry after crash).
			chunk := cardIDs[i:end]
			valueSQL := strings.Builder{}
			args := make([]interface{}, 0, len(chunk)*6)
			for j, cardID := range chunk {
				if j > 0 {
					valueSQL.WriteString(",")
				}
				valueSQL.WriteString("(?,?,?,?,?,?)")
				args = append(args, cardID, campaignID, "pending", initialStep, 0, updatedAt)
			}
			if err := tx.Exec(
				"INSERT INTO card_execution_states (card_id, campaign_id, status, current_step, transition_seq, updated_at) VALUES "+
					valueSQL.String()+" ON CONFLICT (card_id, campaign_id, transition_seq) DO NOTHING",
				args...,
			).Error; err != nil {
				return fmt.Errorf("batch insert card execution states: %w", err)
			}
		}
		// Upsert campaign_stats: count actual pending rows to stay idempotent on retry.
		if err := tx.Exec(`
			INSERT INTO campaign_stats (campaign_id, total, pending)
			SELECT ?, COUNT(*), COUNT(*)
			FROM card_execution_states
			WHERE campaign_id = ? AND transition_seq = 0
			ON CONFLICT (campaign_id) DO UPDATE SET
				total = EXCLUDED.total,
				pending = EXCLUDED.pending`,
			campaignID, campaignID,
		).Error; err != nil {
			return fmt.Errorf("upsert campaign stats: %w", err)
		}
		return nil
	})
}

// AbortCampaign appends 'skipped' rows for all non-terminal cards and updates campaign_stats.
// Returns the number of cards that were skipped.
func (s *QueryStore) AbortCampaign(ctx context.Context, campaignID uuid.UUID, updatedAt time.Time) (int, error) {
	var skipped int64
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Append-only: INSERT new rows with status='skipped' for non-terminal cards.
		result := tx.Exec(`
			INSERT INTO card_execution_states
				(card_id, campaign_id, status, current_step, retry_count, transition_seq, updated_at)
			SELECT card_id, campaign_id, 'skipped', current_step, retry_count, transition_seq + 1, ?
			FROM (
				SELECT DISTINCT ON (card_id) card_id, campaign_id, current_step, retry_count, transition_seq, status
				FROM card_execution_states
				WHERE campaign_id = ?
				ORDER BY card_id, transition_seq DESC
			) latest
			WHERE status NOT IN ('completed', 'failed', 'skipped')`,
			updatedAt, campaignID,
		)
		if result.Error != nil {
			return fmt.Errorf("abort card states: %w", result.Error)
		}
		skipped = result.RowsAffected
		if skipped == 0 {
			return nil
		}
		// Set pending=0, in_progress=0, skipped += skipped count.
		if err := tx.Exec(
			`UPDATE campaign_stats SET skipped = skipped + pending + in_progress, pending = 0, in_progress = 0 WHERE campaign_id = ?`,
			campaignID,
		).Error; err != nil {
			return fmt.Errorf("update campaign stats for abort: %w", err)
		}
		return nil
	})
	return int(skipped), err
}

// ---------- Message queries (message_history TimescaleDB hypertable) ----------

// messageHistoryRow is an internal scan target for message_history queries.
type messageHistoryRow struct {
	ID             uuid.UUID  `gorm:"column:id"`
	CampaignID     *uuid.UUID `gorm:"column:campaign_id"`
	CardID         uuid.UUID  `gorm:"column:card_id"`
	Direction      string     `gorm:"column:direction"`
	Status         string     `gorm:"column:status"`
	SMPPMessageID  *string    `gorm:"column:smpp_message_id"`
	DLRStatus      *string    `gorm:"column:dlr_status"`
	CounterHex     *string    `gorm:"column:counter_hex"`
	PORStatusCode  *int16     `gorm:"column:por_status_code"`
	RawPayload     []byte     `gorm:"column:raw_payload"`
	SecuredPayload []byte     `gorm:"column:secured_payload"`
	PORData        []byte     `gorm:"column:por_data"`
	CreatedAt      time.Time  `gorm:"column:created_at"`
	UpdatedAt      time.Time  `gorm:"column:updated_at"`
}

func (messageHistoryRow) TableName() string { return "message_history" }

func toMessageRecord(r messageHistoryRow) store.MessageRecord {
	return store.MessageRecord{
		ID:             r.ID,
		CampaignID:     r.CampaignID,
		CardID:         r.CardID,
		Direction:      r.Direction,
		Status:         r.Status,
		SMPPMessageID:  r.SMPPMessageID,
		DLRStatus:      r.DLRStatus,
		CounterHex:     r.CounterHex,
		PORStatusCode:  r.PORStatusCode,
		RawPayload:     r.RawPayload,
		SecuredPayload: r.SecuredPayload,
		PORData:        r.PORData,
		CreatedAt:      r.CreatedAt,
		UpdatedAt:      r.UpdatedAt,
	}
}

// GetMessage retrieves a single message by ID.
func (s *QueryStore) GetMessage(ctx context.Context, msgID uuid.UUID) (*store.MessageRecord, error) {
	var row messageHistoryRow
	if err := s.db.WithContext(ctx).Where("id = ?", msgID).First(&row).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("get message: %w", err)
	}
	rec := toMessageRecord(row)
	return &rec, nil
}

// GetCardState reads the latest execution state for a card (highest transition_seq).
func (s *QueryStore) GetCardState(ctx context.Context, cardID uuid.UUID) (*store.CardStateRecord, error) {
	var row db.CardExecutionState
	if err := s.db.WithContext(ctx).
		Where("card_id = ?", cardID).
		Order("transition_seq DESC").
		First(&row).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("get card state: %w", err)
	}
	state := &store.CardStateRecord{
		CardID:      row.CardID,
		CampaignID:  &row.CampaignID,
		Status:      row.Status,
		CurrentStep: row.CurrentStep,
		RetryCount:  row.RetryCount,
		LastSMPPMessageID: row.LastSMPPID,
		LastErrorText:     row.LastError,
		UpdatedAt:         row.UpdatedAt,
	}
	if row.LastMsgID != nil {
		state.LastMsgID = row.LastMsgID
	}
	return state, nil
}

// ListCardMessages returns recent messages for a specific card.
func (s *QueryStore) ListCardMessages(ctx context.Context, cardID uuid.UUID, limit int) ([]store.MessageRecord, error) {
	if limit <= 0 {
		limit = 10
	}
	var rows []messageHistoryRow
	if err := s.db.WithContext(ctx).
		Where("card_id = ?", cardID).
		Order("created_at DESC").
		Limit(limit).
		Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("list card messages: %w", err)
	}
	out := make([]store.MessageRecord, 0, len(rows))
	for _, r := range rows {
		out = append(out, toMessageRecord(r))
	}
	return out, nil
}

// ListMessages returns paginated messages matching the given filter.
func (s *QueryStore) ListMessages(ctx context.Context, filter store.MessageFilter) ([]store.MessageRecord, int64, error) {
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

	query := s.db.WithContext(ctx).Table("message_history").
		Where("created_at >= ? AND created_at <= ?", filter.From, filter.To)

	if filter.CampaignID != nil {
		query = query.Where("campaign_id = ?", *filter.CampaignID)
	}
	if filter.CardID != nil {
		query = query.Where("card_id = ?", *filter.CardID)
	}
	if filter.Direction != "" {
		query = query.Where("direction = ?", filter.Direction)
	}
	if filter.Status != "" {
		query = query.Where("status = ?", filter.Status)
	}

	var total int64
	if err := query.Count(&total).Error; err != nil {
		return nil, 0, fmt.Errorf("count messages: %w", err)
	}

	offset := (filter.Page - 1) * filter.PageSize
	var rows []messageHistoryRow
	if err := query.
		Order("created_at DESC").
		Offset(offset).
		Limit(filter.PageSize).
		Find(&rows).Error; err != nil {
		return nil, 0, fmt.Errorf("list messages: %w", err)
	}

	out := make([]store.MessageRecord, 0, len(rows))
	for _, r := range rows {
		out = append(out, toMessageRecord(r))
	}
	return out, total, nil
}

// RecentActivity returns the most recent messages across all campaigns.
func (s *QueryStore) RecentActivity(ctx context.Context, limit int) ([]store.MessageRecord, error) {
	if limit <= 0 {
		limit = 50
	}
	var rows []messageHistoryRow
	if err := s.db.WithContext(ctx).
		Order("created_at DESC").
		Limit(limit).
		Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("recent activity: %w", err)
	}
	out := make([]store.MessageRecord, 0, len(rows))
	for _, r := range rows {
		out = append(out, toMessageRecord(r))
	}
	return out, nil
}

// MessageMetrics returns aggregate message counts since the given time.
func (s *QueryStore) MessageMetrics(ctx context.Context, since time.Time) (store.MessageMetrics, error) {
	return s.messageMetrics(ctx, since, nil)
}

// CampaignMessageMetrics returns aggregate message counts for a specific campaign.
func (s *QueryStore) CampaignMessageMetrics(ctx context.Context, campaignID uuid.UUID, since time.Time) (store.MessageMetrics, error) {
	return s.messageMetrics(ctx, since, &campaignID)
}

func (s *QueryStore) messageMetrics(ctx context.Context, since time.Time, campaignID *uuid.UUID) (store.MessageMetrics, error) {
	query := s.db.WithContext(ctx).Table("message_history").
		Where("created_at >= ?", since)
	if campaignID != nil {
		query = query.Where("campaign_id = ?", *campaignID)
	}

	var metrics store.MessageMetrics
	row := query.Select(`
		COUNT(*) AS total,
		COUNT(*) FILTER (WHERE direction = 'MT') AS mt,
		COUNT(*) FILTER (WHERE direction = 'MO') AS mo,
		COUNT(*) FILTER (WHERE dlr_status = 'DELIVRD' OR status = 'delivered') AS delivered,
		COUNT(*) FILTER (WHERE dlr_status IS NOT NULL AND dlr_status != 'DELIVRD' AND dlr_status != '') AS undelivered
	`).Row()
	if err := row.Scan(&metrics.Total, &metrics.MT, &metrics.MO, &metrics.Delivered, &metrics.Undelivered); err != nil {
		if err == sql.ErrNoRows {
			return store.MessageMetrics{}, nil
		}
		return store.MessageMetrics{}, fmt.Errorf("message metrics: %w", err)
	}
	return metrics, nil
}

// Throughput returns hourly throughput points since the given time.
func (s *QueryStore) Throughput(ctx context.Context, since time.Time) ([]store.ThroughputPoint, error) {
	return s.throughput(ctx, since, nil, "1 hour")
}

// CampaignThroughput returns hourly throughput for a specific campaign.
func (s *QueryStore) CampaignThroughput(ctx context.Context, campaignID uuid.UUID, since time.Time) ([]store.ThroughputPoint, error) {
	return s.throughput(ctx, since, &campaignID, "1 hour")
}

// MinuteThroughput returns per-minute average TPS for the last hour.
func (s *QueryStore) MinuteThroughput(ctx context.Context) ([]store.ThroughputPoint, error) {
	since := time.Now().UTC().Add(-1 * time.Hour)
	points, err := s.throughput(ctx, since, nil, "1 minute")
	if err != nil {
		return nil, err
	}
	// Convert counts to per-second rates (divide by 60).
	for i := range points {
		points[i].Sent /= 60.0
		points[i].Delivered /= 60.0
		points[i].Failed /= 60.0
	}
	return points, nil
}

func (s *QueryStore) throughput(ctx context.Context, since time.Time, campaignID *uuid.UUID, bucket string) ([]store.ThroughputPoint, error) {
	query := fmt.Sprintf(`
		SELECT
			time_bucket('%s', created_at) AS bucket,
			COUNT(*) FILTER (WHERE direction = 'MT') AS sent,
			COUNT(*) FILTER (WHERE dlr_status = 'DELIVRD' OR status = 'delivered') AS delivered,
			COUNT(*) FILTER (WHERE dlr_status IS NOT NULL AND dlr_status != 'DELIVRD' AND dlr_status != '') AS failed
		FROM message_history
		WHERE created_at >= @since
	`, bucket)
	args := map[string]interface{}{"since": since}

	if campaignID != nil {
		query += " AND campaign_id = @campaign_id"
		args["campaign_id"] = *campaignID
	}

	query += fmt.Sprintf(" GROUP BY time_bucket('%s', created_at) ORDER BY bucket ASC", bucket)

	rows, err := s.db.WithContext(ctx).Raw(query, args).Rows()
	if err != nil {
		return nil, fmt.Errorf("throughput query: %w", err)
	}
	defer rows.Close()

	var points []store.ThroughputPoint
	for rows.Next() {
		var p store.ThroughputPoint
		if err := rows.Scan(&p.Timestamp, &p.Sent, &p.Delivered, &p.Failed); err != nil {
			return nil, fmt.Errorf("scan throughput row: %w", err)
		}
		points = append(points, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("throughput rows: %w", err)
	}
	return points, nil
}

// ErrorSummary returns error counts grouped by status, DLR status, and POR status.
func (s *QueryStore) ErrorSummary(ctx context.Context, since time.Time) ([]store.ErrorCount, []store.ErrorCount, []store.ErrorCount, error) {
	return s.errorSummary(ctx, since, nil)
}

// CampaignErrorSummary returns error counts for a specific campaign.
func (s *QueryStore) CampaignErrorSummary(ctx context.Context, campaignID uuid.UUID, since time.Time) ([]store.ErrorCount, []store.ErrorCount, []store.ErrorCount, error) {
	return s.errorSummary(ctx, since, &campaignID)
}

func (s *QueryStore) errorSummary(ctx context.Context, since time.Time, campaignID *uuid.UUID) ([]store.ErrorCount, []store.ErrorCount, []store.ErrorCount, error) {
	baseWhere := "created_at >= ?"
	baseArgs := []interface{}{since}
	if campaignID != nil {
		baseWhere += " AND campaign_id = ?"
		baseArgs = append(baseArgs, *campaignID)
	}

	// Status errors.
	statusErrors, err := s.groupedErrorCounts(ctx, "status", baseWhere, baseArgs,
		"status NOT IN ('created', 'delivered', 'submitted')")
	if err != nil {
		return nil, nil, nil, fmt.Errorf("status error summary: %w", err)
	}

	// DLR status errors.
	dlrErrors, err := s.groupedErrorCounts(ctx, "dlr_status", baseWhere, baseArgs,
		"dlr_status IS NOT NULL AND dlr_status != '' AND dlr_status != 'DELIVRD'")
	if err != nil {
		return nil, nil, nil, fmt.Errorf("DLR error summary: %w", err)
	}

	// POR status errors.
	porErrors, err := s.groupedPORErrorCounts(ctx, baseWhere, baseArgs)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("POR error summary: %w", err)
	}

	return statusErrors, dlrErrors, porErrors, nil
}

func (s *QueryStore) groupedErrorCounts(ctx context.Context, column, baseWhere string, baseArgs []interface{}, extraWhere string) ([]store.ErrorCount, error) {
	query := fmt.Sprintf(
		`SELECT %s AS key, COUNT(*) AS count FROM message_history WHERE %s AND %s GROUP BY %s ORDER BY count DESC`,
		column, baseWhere, extraWhere, column,
	)
	rows, err := s.db.WithContext(ctx).Raw(query, baseArgs...).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []store.ErrorCount
	for rows.Next() {
		var ec store.ErrorCount
		if err := rows.Scan(&ec.Key, &ec.Count); err != nil {
			return nil, err
		}
		out = append(out, ec)
	}
	return out, rows.Err()
}

func (s *QueryStore) groupedPORErrorCounts(ctx context.Context, baseWhere string, baseArgs []interface{}) ([]store.ErrorCount, error) {
	query := fmt.Sprintf(
		`SELECT por_status_code::text AS key, COUNT(*) AS count FROM message_history WHERE %s AND por_status_code IS NOT NULL AND por_status_code != 0 GROUP BY por_status_code ORDER BY count DESC`,
		baseWhere,
	)
	rows, err := s.db.WithContext(ctx).Raw(query, baseArgs...).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []store.ErrorCount
	for rows.Next() {
		var ec store.ErrorCount
		if err := rows.Scan(&ec.Key, &ec.Count); err != nil {
			return nil, err
		}
		out = append(out, ec)
	}
	return out, rows.Err()
}

// Compile-time interface check.
var _ interface {
	CampaignStats(context.Context, uuid.UUID) (store.CampaignStats, error)
	CampaignStatsBatch(context.Context, []uuid.UUID) (map[uuid.UUID]store.CampaignStats, error)
	ListCampaignCards(context.Context, uuid.UUID, string) ([]store.CampaignCardView, error)
	ListFailedCardIDs(context.Context, uuid.UUID) ([]uuid.UUID, error)
	ResetFailedCards(context.Context, uuid.UUID, []uuid.UUID, int, time.Time) error
	InitializeCampaign(context.Context, uuid.UUID, []uuid.UUID, int, time.Time) error
	AbortCampaign(context.Context, uuid.UUID, time.Time) (int, error)
	GetMessage(context.Context, uuid.UUID) (*store.MessageRecord, error)
	GetCardState(context.Context, uuid.UUID) (*store.CardStateRecord, error)
	ListCardMessages(context.Context, uuid.UUID, int) ([]store.MessageRecord, error)
	ListMessages(context.Context, store.MessageFilter) ([]store.MessageRecord, int64, error)
	RecentActivity(context.Context, int) ([]store.MessageRecord, error)
	MessageMetrics(context.Context, time.Time) (store.MessageMetrics, error)
	CampaignMessageMetrics(context.Context, uuid.UUID, time.Time) (store.MessageMetrics, error)
	Throughput(context.Context, time.Time) ([]store.ThroughputPoint, error)
	CampaignThroughput(context.Context, uuid.UUID, time.Time) ([]store.ThroughputPoint, error)
	MinuteThroughput(context.Context) ([]store.ThroughputPoint, error)
	ErrorSummary(context.Context, time.Time) ([]store.ErrorCount, []store.ErrorCount, []store.ErrorCount, error)
	CampaignErrorSummary(context.Context, uuid.UUID, time.Time) ([]store.ErrorCount, []store.ErrorCount, []store.ErrorCount, error)
} = (*QueryStore)(nil)

