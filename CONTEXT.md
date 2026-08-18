# SMS Gateway

Prepaid multi-tenant outbound SMS: an Account spends credit to send messages, never below zero.

## Language

**Account**:
A tenant that holds prepaid credit and sends messages.
_Avoid_: user, customer, client, tenant (in API language)

**Live balance**:
The prepaid amount that may be spent right now. Accept and `GET /v1/balance` use this number.
_Avoid_: Redis balance, hot-path balance, cached balance

**Durable balance**:
The prepaid amount that has been durably applied. Heal and cold-start trust this number. It is mutated; it is never the sum of a log.
_Avoid_: accounts.balance, ledger sum, cached projection, ground truth (when used of a log)

**Credit log**:
Append-only history of topup, debit, and refund. It is not a balance. Nothing heals, seeds, or spends by reading it.
_Avoid_: ledger, ledger_entries, event-sourced balance

**Topup**:
A credit-log entry and a durable-balance increase from purchased credit.

**Debit**:
A credit-log entry and a durable-balance decrease that catches up a live-balance spend on a message. At most one debit per message.

**Refund**:
A credit-log entry and a durable-balance increase that returns a message debit after failed send or Express SLA miss. Live balance is credited only after the durable refund has committed. At most one refund per message.

**Campaign reservation**:
An all-or-nothing live-balance debit for every recipient in a campaign. Durable debit and refund stay per-message; there is no campaign-level credit-log row.

**Cost**:
The prepaid amount charged for one message. Billing owns it; accept receives it as an argument.
_Avoid_: price, tariff, billing.CostPerMessage as a cross-package import

**Heal**:
Setting live balance down to durable balance when live is higher. Heal never grants credit.
_Avoid_: reconcile up, align from the credit log

**Seed**:
Copying durable balance into live balance only when live has no value yet. Not used when live is present and lower than durable.
