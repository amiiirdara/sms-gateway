CREATE TABLE ledger_entries (
    id             uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id     uuid NOT NULL REFERENCES accounts(id),
    type           text NOT NULL CHECK (type IN ('topup', 'debit', 'refund')),
    amount         bigint NOT NULL,
    message_id     uuid,
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX idx_ledger_entries_account_id ON ledger_entries (account_id, created_at);
CREATE UNIQUE INDEX IF NOT EXISTS idx_ledger_entries_message_debit
    ON ledger_entries (message_id) WHERE message_id IS NOT NULL AND type = 'debit';
CREATE UNIQUE INDEX IF NOT EXISTS idx_ledger_entries_message_refund
    ON ledger_entries (message_id) WHERE message_id IS NOT NULL AND type = 'refund';

COMMENT ON COLUMN accounts.balance IS 'cached projection of SUM(ledger_entries.amount); ledger is ground truth';
