-- Drop FK constraints on card_counters to eliminate per-row FK validation
-- during bulk INSERT (was causing 209ms per 1000-row shard due to
-- SELECT 1 FROM cards FOR KEY SHARE lookups).
-- Data integrity is maintained by the application layer: counters are only
-- inserted for cards that exist (preloader reads cards first).
ALTER TABLE card_counters DROP CONSTRAINT IF EXISTS card_counters_card_id_fkey;
ALTER TABLE card_counters DROP CONSTRAINT IF EXISTS card_counters_application_id_fkey;
