-- Durable balance is the mutated accounts.balance column, not SUM(ledger_entries).
-- Fail the migration if the cache has drifted so we never drop history while money is wrong.

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM accounts a
        WHERE a.balance IS DISTINCT FROM (
            SELECT COALESCE(SUM(e.amount), 0)::bigint
            FROM ledger_entries e
            WHERE e.account_id = a.id
        )
    ) THEN
        RAISE EXCEPTION 'accounts.balance does not match SUM(ledger_entries); refuse to drop ledger';
    END IF;
END $$;

DROP TABLE IF EXISTS ledger_entries;

COMMENT ON COLUMN accounts.balance IS 'durable prepaid balance; mutated on topup/debit/refund, never derived from a log';
