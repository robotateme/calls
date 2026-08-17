# ADR-0002: Workers Claim Rows with Row Locks and SKIP LOCKED

Status: Accepted

## Context

Multiple processes read the same working tables:

- `operators`;
- `telephony_outbox`;
- `calls` for retries and recovery.

Without locking, two workers can claim the same row. Without `SKIP LOCKED`, a
worker can wait on a locked row while other ready work exists.

## Decision

When a worker claims work, it locks rows.

For PostgreSQL/MySQL:

```sql
FOR UPDATE SKIP LOCKED
```

This is used for:

- batches of calls for retry/recovery;
- `telephony_outbox` publish/requeue;
- operator selection inside a transaction.

For PostgreSQL, publish claim in `telephony_outbox` uses atomic
`UPDATE ... RETURNING`: one SQL statement selects due records through
`FOR UPDATE SKIP LOCKED`, moves them to `processing`, increments `attempts`, and
returns updated rows for publishing. SQLite tests and non-PostgreSQL drivers use
the portable fallback.

The production branch is covered by
`tests/Feature/PostgreSQLTelephonyOutboxRepositoryTest.php`. It checks
post-claim `attempts` and concurrent claim by two workers for one row.

SQLite tests keep normal Laravel locking.

## Worker Rule

1. Take a small batch.
2. Lock rows.
3. Immediately mark ownership: `processing`, reservation, or another marker.
4. Commit the transaction.
5. Work only on rows owned by this worker.

Claim first, external side effect later.

Otherwise two processes can decide the same row belongs to both of them. For
outbox this can duplicate sends; for operators it can duplicate reservations.

## Do Not

- Read `pending` and decide ownership without a lock.
- Do external action before lock/claim.
- Hold a transaction during external calls.
- Assume one worker removes all race conditions forever.

## Indexes

Indexes are needed for:

- operator search;
- due outbox;
- stale outbox;
- retry scans.

Without indexes, locking does not fix slow queries.

## Tradeoffs

- Need to monitor lock wait and slow queries.
- Operator selection can still wait on a specific row lock.
- If operator claim becomes a bottleneck, add PostgreSQL-specific optimization
  similar to outbox publish claim.

## Rejected

- Single worker for everything.
- Advisory locks.
- `UPDATE ... RETURNING` for every driver, because SQLite tests and MySQL
  compatibility still matter.
