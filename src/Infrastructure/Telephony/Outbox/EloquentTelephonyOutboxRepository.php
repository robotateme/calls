<?php

declare(strict_types=1);

namespace Infrastructure\Telephony\Outbox;

use Application\Telephony\Ports\TelephonyOutboxWriteRepository;
use Domain\Telephony\TelephonyOutboxMessage;
use Illuminate\Support\Facades\DB;

final readonly class EloquentTelephonyOutboxRepository implements TelephonyOutboxWriteRepository
{
    public function __construct(private readonly EloquentTelephonyOutboxMapper $mapper) {}

    public function claimDue(int $limit): array
    {
        return DB::transaction(function () use ($limit): array {
            $records = DB::table('telephony_outbox')
                ->where('status', 'pending')
                ->whereNull('canceled_at')
                ->where(function ($query): void {
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
                DB::table('telephony_outbox')
                    ->whereIn('id', $ids)
                    ->update([
                        'status' => 'processing',
                        'attempts' => DB::raw('attempts + 1'),
                        'processing_started_at' => now(),
                        'updated_at' => now(),
                    ]);
            }

            $messages = $records
                ->map(fn (object $record): TelephonyOutboxMessage => $this->mapper->toDomain((array) $record))
                ->all();

            return array_values($messages);
        });
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
        return DB::transaction(function () use ($olderThanSeconds, $limit): array {
            $records = DB::table('telephony_outbox')
                ->where('status', 'processing')
                ->whereNull('canceled_at')
                ->where('processing_started_at', '<=', now()->subSeconds(max(0, $olderThanSeconds)))
                ->orderBy('processing_started_at')
                ->orderBy('id')
                ->limit(max(1, $limit))
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
                ->map(fn (object $record): TelephonyOutboxMessage => $this->mapper->toDomain((array) $record, attemptOffset: 0))
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
}
