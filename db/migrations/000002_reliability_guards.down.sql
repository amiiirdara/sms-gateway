ALTER TABLE campaigns DROP COLUMN IF EXISTS expanded_through_index;
DROP INDEX IF EXISTS idx_message_status_events_message_status;
DROP INDEX IF EXISTS idx_ledger_entries_message_refund;
DROP INDEX IF EXISTS idx_ledger_entries_message_debit;
