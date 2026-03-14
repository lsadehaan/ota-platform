-- Phase 0: Simplified architecture tables.
-- These tables coexist with the existing schema; no breaking changes.

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Card execution states: append-only. Each state transition inserts a new row.
-- Latest state per card determined by highest transition_seq.
CREATE TABLE IF NOT EXISTS card_execution_states (
    card_id        UUID NOT NULL,
    campaign_id    UUID NOT NULL,
    status         TEXT NOT NULL DEFAULT 'pending',
    current_step   INT NOT NULL DEFAULT 0,
    retry_count    INT NOT NULL DEFAULT 0,
    last_msg_id    UUID,
    last_smpp_id   TEXT DEFAULT '',
    last_error     TEXT DEFAULT '',
    transition_seq BIGINT NOT NULL DEFAULT 0,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (card_id, campaign_id, transition_seq)
);
-- Index for DISTINCT ON queries: latest state per card in a campaign.
CREATE INDEX IF NOT EXISTS idx_ces_campaign_card_seq ON card_execution_states (campaign_id, card_id, transition_seq DESC);

-- Campaign stats: updated transactionally with card state transitions.
-- Avoids COUNT(*) scans over card_execution_states for stats queries.
CREATE TABLE IF NOT EXISTS campaign_stats (
    campaign_id UUID PRIMARY KEY,
    total       BIGINT NOT NULL DEFAULT 0,
    pending     BIGINT NOT NULL DEFAULT 0,
    in_progress BIGINT NOT NULL DEFAULT 0,
    completed   BIGINT NOT NULL DEFAULT 0,
    failed      BIGINT NOT NULL DEFAULT 0,
    skipped     BIGINT NOT NULL DEFAULT 0
);

-- Submission ledger: prevents duplicate SMPP submits after crash recovery.
CREATE TABLE IF NOT EXISTS sms_submission_ledger (
    msg_id     TEXT NOT NULL,
    part       INT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (msg_id, part)
);

-- Message history: TimescaleDB hypertable for time-series message data.
CREATE TABLE IF NOT EXISTS message_history (
    id              UUID NOT NULL DEFAULT gen_random_uuid(),
    campaign_id     UUID,
    card_id         UUID NOT NULL,
    direction       TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'created',
    raw_payload     BYTEA,
    secured_payload BYTEA,
    counter_hex     TEXT,
    smpp_message_id TEXT,
    dlr_status      TEXT,
    por_status_code SMALLINT,
    por_data        BYTEA,
    error_code      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id, created_at)
);
SELECT create_hypertable('message_history', 'created_at', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_mh_card_time ON message_history (card_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_mh_campaign_time ON message_history (campaign_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_mh_smpp ON message_history (smpp_message_id) WHERE smpp_message_id IS NOT NULL;
