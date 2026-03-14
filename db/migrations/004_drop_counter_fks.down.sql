-- Re-add FK constraints on card_counters.
ALTER TABLE card_counters
    ADD CONSTRAINT card_counters_card_id_fkey
    FOREIGN KEY (card_id) REFERENCES cards(id);

ALTER TABLE card_counters
    ADD CONSTRAINT card_counters_application_id_fkey
    FOREIGN KEY (application_id) REFERENCES applications(id);
