-- Ledger: one debit and one refund per message (Inbox is not the only guard).
CREATE UNIQUE INDEX IF NOT EXISTS idx_ledger_entries_message_debit
    ON ledger_entries (message_id) WHERE message_id IS NOT NULL AND type = 'debit';
CREATE UNIQUE INDEX IF NOT EXISTS idx_ledger_entries_message_refund
    ON ledger_entries (message_id) WHERE message_id IS NOT NULL AND type = 'refund';

-- Status history: retries must not duplicate the same transition.
-- Demo DBs may already have duplicate (message_id, status) from pre-fix sinks.
DELETE FROM message_status_events a
    USING message_status_events b
WHERE a.message_id = b.message_id
  AND a.status = b.status
  AND a.id > b.id;

CREATE UNIQUE INDEX IF NOT EXISTS idx_message_status_events_message_status
    ON message_status_events (message_id, status);

-- Campaign expansion cursor (last successfully published recipient index; -1 = none).
ALTER TABLE campaigns
    ADD COLUMN IF NOT EXISTS expanded_through_index integer NOT NULL DEFAULT -1;
