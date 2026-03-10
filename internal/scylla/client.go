package scylla

import (
	"fmt"
	"hash/fnv"
	"regexp"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"go.uber.org/zap"
)

type Config struct {
	Hosts             []string
	Keyspace          string
	Username          string
	Password          string
	BucketCount       int
	ReplicationClass  string
	ReplicationFactor int
}

type Client struct {
	session     *gocql.Session
	keyspace    string
	bucketCount int
	logger      *zap.Logger
}

func Open(cfg Config, logger *zap.Logger) (*Client, error) {
	if len(cfg.Hosts) == 0 || strings.TrimSpace(cfg.Hosts[0]) == "" {
		return nil, fmt.Errorf("scylla hosts required")
	}
	if cfg.Keyspace == "" {
		return nil, fmt.Errorf("scylla keyspace required")
	}
	if cfg.BucketCount <= 0 {
		cfg.BucketCount = 128
	}
	if cfg.ReplicationClass == "" {
		cfg.ReplicationClass = "SimpleStrategy"
	}
	if cfg.ReplicationFactor <= 0 {
		cfg.ReplicationFactor = 1
	}
	if !isValidIdentifier(cfg.Keyspace) {
		return nil, fmt.Errorf("invalid scylla keyspace name: %q", cfg.Keyspace)
	}

	cluster := gocql.NewCluster(cfg.Hosts...)
	cluster.Timeout = 10 * time.Second
	cluster.ConnectTimeout = 10 * time.Second
	cluster.Consistency = gocql.Quorum
	cluster.ProtoVersion = 4
	cluster.DisableInitialHostLookup = false
	cluster.RetryPolicy = &gocql.ExponentialBackoffRetryPolicy{
		NumRetries: 5,
		Min:        100 * time.Millisecond,
		Max:        2 * time.Second,
	}
	if cfg.Username != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: cfg.Username,
			Password: cfg.Password,
		}
	}

	sysSession, err := cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("connect scylla system session: %w", err)
	}
	defer sysSession.Close()

	if err := ensureKeyspace(sysSession, cfg.Keyspace, cfg.ReplicationClass, cfg.ReplicationFactor); err != nil {
		return nil, err
	}

	cluster.Keyspace = cfg.Keyspace
	session, err := cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("connect scylla keyspace session: %w", err)
	}

	client := &Client{
		session:     session,
		keyspace:    cfg.Keyspace,
		bucketCount: cfg.BucketCount,
		logger:      logger,
	}
	if cfg.ReplicationFactor == 1 {
		logger.Warn("scylla replication factor is 1; this is not production-safe",
			zap.String("keyspace", cfg.Keyspace),
		)
	}
	if err := client.ensureSchema(); err != nil {
		session.Close()
		return nil, err
	}
	return client, nil
}

func (c *Client) Close() {
	if c.session != nil {
		c.session.Close()
	}
}

func (c *Client) Session() *gocql.Session {
	return c.session
}

func (c *Client) BucketCount() int {
	return c.bucketCount
}

func (c *Client) CardBucket(cardID string) int {
	return hashBucket(cardID, c.bucketCount)
}

func (c *Client) CampaignBucket(campaignID, cardID string) int {
	return hashBucket(campaignID+":"+cardID, c.bucketCount)
}

func (c *Client) GlobalBucket(key string) int {
	return hashBucket("global:"+key, c.bucketCount)
}

func (c *Client) TimeBucket(ts time.Time) string {
	return ts.UTC().Format("2006-01-02")
}

func hashBucket(key string, bucketCount int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(bucketCount))
}

var identifierPattern = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_]*$`)
var allowedReplicationClasses = map[string]struct{}{
	"SimpleStrategy":          {},
	"NetworkTopologyStrategy": {},
}

func isValidIdentifier(v string) bool {
	return identifierPattern.MatchString(v)
}

func ensureKeyspace(session *gocql.Session, keyspace, replicationClass string, replicationFactor int) error {
	if _, ok := allowedReplicationClasses[replicationClass]; !ok {
		return fmt.Errorf("invalid scylla replication class: %q", replicationClass)
	}
	stmt := fmt.Sprintf(`
		CREATE KEYSPACE IF NOT EXISTS %s
		WITH replication = {'class': '%s', 'replication_factor': %d}
	`, keyspace, replicationClass, replicationFactor)
	if err := session.Query(stmt).Exec(); err != nil {
		return fmt.Errorf("create scylla keyspace %s: %w", keyspace, err)
	}
	return nil
}

func (c *Client) ensureSchema() error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS card_state_by_card (
			card_bucket int,
			card_id uuid,
			campaign_id uuid,
			status text,
			current_step int,
			retry_count int,
			last_msg_id uuid,
			last_smpp_message_id text,
			last_error_code text,
			last_error_text text,
			updated_at timestamp,
			PRIMARY KEY ((card_bucket, card_id))
		)`,
		`CREATE TABLE IF NOT EXISTS message_by_id (
			msg_id uuid PRIMARY KEY,
			card_id uuid,
			card_bucket int,
			campaign_id uuid,
			campaign_bucket int,
			time_bucket text,
			created_at timestamp,
			updated_at timestamp,
			direction text,
			status text,
			smpp_message_id text,
			dlr_status text,
			counter_hex text,
			por_status_code smallint,
			error_code text,
			raw_payload blob,
			secured_payload blob,
			por_data blob
		)`,
		`CREATE TABLE IF NOT EXISTS message_by_card_time (
			card_bucket int,
			card_id uuid,
			time_bucket text,
			created_at timestamp,
			msg_id uuid,
			campaign_id uuid,
			updated_at timestamp,
			direction text,
			status text,
			smpp_message_id text,
			dlr_status text,
			counter_hex text,
			por_status_code smallint,
			error_code text,
			raw_payload blob,
			secured_payload blob,
			por_data blob,
			PRIMARY KEY ((card_bucket, card_id, time_bucket), created_at, msg_id)
		) WITH CLUSTERING ORDER BY (created_at DESC, msg_id ASC)`,
		`CREATE TABLE IF NOT EXISTS message_by_campaign_bucket_time (
			campaign_id uuid,
			campaign_bucket int,
			time_bucket text,
			created_at timestamp,
			card_id uuid,
			msg_id uuid,
			updated_at timestamp,
			direction text,
			status text,
			smpp_message_id text,
			dlr_status text,
			por_status_code smallint,
			error_code text,
			PRIMARY KEY ((campaign_id, campaign_bucket, time_bucket), created_at, card_id, msg_id)
		) WITH CLUSTERING ORDER BY (created_at DESC, card_id ASC, msg_id ASC)`,
		`CREATE TABLE IF NOT EXISTS message_by_time_bucket (
			time_bucket text,
			global_bucket int,
			created_at timestamp,
			msg_id uuid,
			card_id uuid,
			campaign_id uuid,
			updated_at timestamp,
			direction text,
			status text,
			smpp_message_id text,
			dlr_status text,
			por_status_code smallint,
			error_code text,
			PRIMARY KEY ((time_bucket, global_bucket), created_at, msg_id)
		) WITH CLUSTERING ORDER BY (created_at DESC, msg_id ASC)`,
		`CREATE TABLE IF NOT EXISTS message_by_direction_time_bucket (
			direction text,
			time_bucket text,
			global_bucket int,
			created_at timestamp,
			msg_id uuid,
			card_id uuid,
			campaign_id uuid,
			updated_at timestamp,
			status text,
			smpp_message_id text,
			dlr_status text,
			por_status_code smallint,
			error_code text,
			PRIMARY KEY ((direction, time_bucket, global_bucket), created_at, msg_id)
		) WITH CLUSTERING ORDER BY (created_at DESC, msg_id ASC)`,
		`CREATE TABLE IF NOT EXISTS message_by_status_time_bucket (
			status text,
			time_bucket text,
			global_bucket int,
			created_at timestamp,
			msg_id uuid,
			card_id uuid,
			campaign_id uuid,
			updated_at timestamp,
			direction text,
			smpp_message_id text,
			dlr_status text,
			por_status_code smallint,
			error_code text,
			PRIMARY KEY ((status, time_bucket, global_bucket), created_at, msg_id)
		) WITH CLUSTERING ORDER BY (created_at DESC, msg_id ASC)`,
		`CREATE TABLE IF NOT EXISTS message_by_dlr_status_time_bucket (
			dlr_status text,
			time_bucket text,
			global_bucket int,
			created_at timestamp,
			msg_id uuid,
			card_id uuid,
			campaign_id uuid,
			updated_at timestamp,
			direction text,
			status text,
			smpp_message_id text,
			por_status_code smallint,
			error_code text,
			PRIMARY KEY ((dlr_status, time_bucket, global_bucket), created_at, msg_id)
		) WITH CLUSTERING ORDER BY (created_at DESC, msg_id ASC)`,
		`CREATE TABLE IF NOT EXISTS message_metrics_by_hour (
			hour_bucket timestamp PRIMARY KEY,
			total_messages counter,
			mt_messages counter,
			mo_messages counter,
			delivered_messages counter,
			undelivered_messages counter
		)`,
		`CREATE TABLE IF NOT EXISTS message_metrics_by_minute (
			minute_bucket timestamp PRIMARY KEY,
			total_messages counter,
			mt_messages counter,
			mo_messages counter,
			delivered_messages counter,
			undelivered_messages counter
		)`,
		`CREATE TABLE IF NOT EXISTS campaign_message_metrics_by_hour (
			campaign_id uuid,
			campaign_bucket int,
			hour_bucket timestamp,
			total_messages counter,
			mt_messages counter,
			mo_messages counter,
			delivered_messages counter,
			undelivered_messages counter,
			PRIMARY KEY ((campaign_id, campaign_bucket), hour_bucket)
		)`,
		`CREATE TABLE IF NOT EXISTS message_error_counts_by_hour (
			hour_bucket timestamp,
			error_kind text,
			error_key text,
			error_count counter,
			PRIMARY KEY ((hour_bucket), error_kind, error_key)
		)`,
		`CREATE TABLE IF NOT EXISTS campaign_message_error_counts_by_hour (
			campaign_id uuid,
			campaign_bucket int,
			hour_bucket timestamp,
			error_kind text,
			error_key text,
			error_count counter,
			PRIMARY KEY ((campaign_id, campaign_bucket, hour_bucket), error_kind, error_key)
		)`,
		`CREATE TABLE IF NOT EXISTS campaign_card_status_by_bucket (
			campaign_id uuid,
			campaign_bucket int,
			status text,
			updated_at timestamp,
			card_id uuid,
			current_step int,
			retry_count int,
			last_msg_id uuid,
			last_error_text text,
			PRIMARY KEY ((campaign_id, campaign_bucket), status, updated_at, card_id)
		) WITH CLUSTERING ORDER BY (status ASC, updated_at DESC, card_id ASC)`,
		`CREATE TABLE IF NOT EXISTS failed_cards_by_campaign_bucket (
			campaign_id uuid,
			campaign_bucket int,
			card_id uuid,
			failed_at timestamp,
			current_step int,
			retry_count int,
			last_msg_id uuid,
			last_error_code text,
			last_error_text text,
			PRIMARY KEY ((campaign_id, campaign_bucket), card_id)
		)`,
		`CREATE TABLE IF NOT EXISTS campaign_progress_by_bucket (
			campaign_id uuid,
			campaign_bucket int,
			pending counter,
			in_progress counter,
			completed counter,
			failed counter,
			skipped counter,
			PRIMARY KEY ((campaign_id), campaign_bucket)
		)`,
		`CREATE TABLE IF NOT EXISTS card_keys (
			card_id uuid PRIMARY KEY,
			enc_key blob,
			auth_key blob,
			kek blob,
			profile_id uuid,
			msisdn text
		)`,
		`CREATE TABLE IF NOT EXISTS card_counters (
			card_id uuid,
			application_id uuid,
			counter_value counter,
			PRIMARY KEY (card_id, application_id)
		)`,
	}

	for _, stmt := range stmts {
		if err := c.session.Query(stmt).Exec(); err != nil {
			return fmt.Errorf("ensure scylla schema: %w", err)
		}
	}
	columnAdds := []struct {
		table  string
		column string
		ctype  string
	}{
		{"message_by_id", "updated_at", "timestamp"},
		{"message_by_id", "dlr_status", "text"},
		{"message_by_card_time", "updated_at", "timestamp"},
		{"message_by_card_time", "dlr_status", "text"},
		{"message_by_campaign_bucket_time", "updated_at", "timestamp"},
		{"message_by_campaign_bucket_time", "dlr_status", "text"},
	}
	for _, add := range columnAdds {
		if err := c.ensureColumn(add.table, add.column, add.ctype); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) ensureColumn(table, column, ctype string) error {
	if !isValidIdentifier(table) || !isValidIdentifier(column) || !isValidIdentifier(ctype) {
		return fmt.Errorf("invalid identifier in schema alteration: %s.%s %s", table, column, ctype)
	}
	var name string
	err := c.session.Query(
		`SELECT column_name FROM system_schema.columns WHERE keyspace_name = ? AND table_name = ? AND column_name = ?`,
		c.keyspace, table, column,
	).Consistency(gocql.One).Scan(&name)
	if err == nil {
		return nil
	}
	if err != gocql.ErrNotFound {
		return fmt.Errorf("check scylla column %s.%s: %w", table, column, err)
	}
	if err := c.session.Query(fmt.Sprintf("ALTER TABLE %s ADD %s %s", table, column, ctype)).Exec(); err != nil {
		return fmt.Errorf("alter scylla table %s add %s: %w", table, column, err)
	}
	return nil
}
