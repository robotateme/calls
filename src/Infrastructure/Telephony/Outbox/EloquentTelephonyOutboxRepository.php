<?php

declare(strict_types=1);

namespace Infrastructure\Telephony\Outbox;

use Application\Telephony\Ports\TelephonyOutboxWriteRepository;
use Domain\Telephony\TelephonyOutboxMessage;
use Illuminate\Database\Query\Builder;
use Illuminate\Support\Facades\DB;

final readonly class EloquentTelephonyOutboxRepository implements TelephonyOutboxWriteRepository
{
    public function __construct(private readonly EloquentTelephonyOutboxMapper $mapper) {}

    public function claimDue(int $limit): array
    {
        return DB::transaction(function () use ($limit): array {
            if (DB::connection()->getDriverName() === 'pgsql') {
                return $this->claimDueWithReturning($limit);
            }

            return $this->claimDuePortably($limit);
        });
    }

    /**
     * PostgreSQL claims due records and returns their post-claim state in one
     * round trip, so TelephonyOutboxMessage receives the incremented attempts.
     *
     * @return list<TelephonyOutboxMessage>
     */
    private function claimDueWithReturning(int $limit): array
    {
        $now = now()->toDateTimeString();
        $bindings = [
            'pending',
            $now,
            $limit,
            'processing',
            $now,
            $now,
        ];

        $records = DB::select(<<<'SQL'
            WITH due AS (
                SELECT id
                FROM telephony_outbox
                WHERE status = ?
                  AND canceled_at IS NULL
                  AND (available_at IS NULL OR available_at <= ?)
                ORDER BY id
                LIMIT ?
                FOR UPDATE SKIP LOCKED
            ),
            claimed AS (
                UPDATE telephony_outbox AS t
                SET status = ?,
                    attempts = t.attempts + 1,
                    processing_started_at = ?,
                    updated_at = ?
                FROM due
                WHERE t.id = due.id
                RETURNING t.*
            )
            SELECT *
            FROM claimed
            ORDER BY id
            SQL, $bindings);

        return $this->recordsToMessages($records);
    }

    /**
     * Portable fallback for SQLite tests and non-PostgreSQL deployments.
     *
     * @return list<TelephonyOutboxMessage>
     */
    private function claimDuePortably(int $limit): array
    {
        $records = DB::table('telephony_outbox')
            ->where('status', 'pending')
            ->whereNull('canceled_at')
            ->where(function (Builder $query): void {
                $query
                    ->whereNull('available_at')
                    ->orWhere('available_at', '<=', now());
            })
            ->orderBy('id')
            ->limit($limit)
            ->lock($this->forUpdateLock())
            ->get();

        $ids = $records->pluck('id')->map(static fn (mixed $id): int => (int) $id)->all();

        if ($ids !== []) {
            $now = now();

            DB::table('telephony_outbox')
                ->whereIn('id', $ids)
                ->update([
                    'status' => 'processing',
                    'attempts' => DB::raw('attempts + 1'),
                    'processing_started_at' => $now,
                    'updated_at' => $now,
                ]);
        }

        $claimedRecords = $ids === []
            ? collect()
            : DB::table('telephony_outbox')
                ->whereIn('id', $ids)
                ->orderBy('id')
                ->get();

        $messages = $claimedRecords
            ->map(fn (object $record): TelephonyOutboxMessage => $this->mapper->toDomain((array) $record))
            ->values()
            ->all();

        return array_values($messages);
    }

    public function markPublished(int $id): void
    {
        DB::table('telephony_outbox')
            ->where('id', $id)
            ->update([
                'status' => 'published',
                'published_at' => now(),
                'processing_started_at' => null,
                'last_error' => null,
                'updated_at' => now(),
            ]);
    }

    public function requeueStaleProcessing(int $olderThanSeconds, int $limit): array
    {
        $processingTimeoutSeconds = max(0, $olderThanSeconds);
        $requeueLimit = max(1, $limit);

        return DB::transaction(function () use ($processingTimeoutSeconds, $requeueLimit): array {
            $records = DB::table('telephony_outbox')
                ->where('status', 'processing')
                ->whereNull('canceled_at')
                ->where('processing_started_at', '<=', now()->subSeconds($processingTimeoutSeconds))
                ->orderBy('processing_started_at')
                ->orderBy('id')
                ->limit($requeueLimit)
                ->lock($this->forUpdateLock())
                ->get();

            $ids = $records->pluck('id')->map(static fn (mixed $id): int => (int) $id)->all();

            if ($ids !== []) {
                DB::table('telephony_outbox')
                    ->whereIn('id', $ids)
                    ->update([
                        'status' => 'pending',
                        'available_at' => now(),
                        'processing_started_at' => null,
                        'last_error' => 'Processing timeout: publisher did not finish claimed record.',
                        'updated_at' => now(),
                    ]);
            }

            $messages = $records
                ->map(fn (object $record): TelephonyOutboxMessage => $this->mapper->toDomain((array) $record))
                ->all();

            return array_values($messages);
        });
    }

    public function markFailed(int $id, string $error, int $retryDelaySeconds, int $maxAttempts): void
    {
        $attempts = (int) DB::table('telephony_outbox')
            ->where('id', $id)
            ->value('attempts');

        DB::table('telephony_outbox')
            ->where('id', $id)
            ->update([
                'status' => $attempts >= $maxAttempts ? 'failed' : 'pending',
                'available_at' => $attempts >= $maxAttempts ? null : now()->addSeconds($retryDelaySeconds),
                'processing_started_at' => null,
                'last_error' => mb_substr($error, 0, 4000),
                'updated_at' => now(),
            ]);
    }

    private function forUpdateLock(): string|bool
    {
        return in_array(DB::connection()->getDriverName(), ['mysql', 'pgsql'], true)
            ? 'FOR UPDATE SKIP LOCKED'
            : true;
    }

    /**
     * @param  array<int, object>  $records
     * @return list<TelephonyOutboxMessage>
     */
    private function recordsToMessages(array $records): array
    {
        return array_values(array_map(
            fn (object $record): TelephonyOutboxMessage => $this->mapper->toDomain((array) $record),
            $records,
        ));
    }
}
