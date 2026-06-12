<?php

declare(strict_types=1);

namespace Infrastructure\Telephony\Outbox;

use Application\Telephony\Ports\TelephonyCommandOutboxReader;
use Application\Telephony\Ports\TelephonyCommandOutboxWriter;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Str;

final readonly class EloquentTelephonyCommandOutbox implements TelephonyCommandOutboxReader, TelephonyCommandOutboxWriter
{
    public function recordCallAssignmentRequested(string $externalCallId, int $operatorId, int $attempt): void
    {
        $this->record(
            type: 'call_assignment_requested',
            externalCallId: $externalCallId,
            idempotencyKey: sprintf('%s:call_assignment_requested:%d', $externalCallId, $attempt),
            payload: [
                'external_call_id' => $externalCallId,
                'operator_id' => $operatorId,
                'assignment_attempt' => $attempt,
            ],
        );
    }

    public function recordCallAssignmentCanceled(string $externalCallId, int $operatorId, int $attempt, string $reason): void
    {
        $this->record(
            type: 'call_assignment_canceled',
            externalCallId: $externalCallId,
            idempotencyKey: sprintf('%s:call_assignment_canceled:%d', $externalCallId, $attempt),
            payload: [
                'external_call_id' => $externalCallId,
                'operator_id' => $operatorId,
                'assignment_attempt' => $attempt,
                'reason' => $reason,
            ],
        );
    }

    public function recordOperatorSearchRetryScheduled(string $externalCallId, int $attempt, int $retryDelaySeconds): void
    {
        $this->record(
            type: 'operator_search_retry_scheduled',
            externalCallId: $externalCallId,
            idempotencyKey: sprintf('%s:operator_search_retry_scheduled:%d', $externalCallId, $attempt),
            payload: [
                'external_call_id' => $externalCallId,
                'attempt' => $attempt,
                'retry_delay_seconds' => $retryDelaySeconds,
            ],
        );
    }

    public function recordOperatorSearchExhausted(string $externalCallId, int $attempt, string $finalStatus): void
    {
        $this->record(
            type: 'operator_search_exhausted',
            externalCallId: $externalCallId,
            idempotencyKey: sprintf('%s:operator_search_exhausted:%d', $externalCallId, $attempt),
            payload: [
                'external_call_id' => $externalCallId,
                'attempt' => $attempt,
                'final_status' => $finalStatus,
            ],
        );
    }

    public function cancelPendingAssignmentRequests(string $externalCallId, string $reason): void
    {
        DB::table('telephony_outbox')
            ->where('external_call_id', $externalCallId)
            ->where('type', 'call_assignment_requested')
            ->where('status', 'pending')
            ->whereNull('published_at')
            ->whereNull('canceled_at')
            ->update([
                'canceled_at' => now(),
                'cancel_reason' => mb_substr($reason, 0, 128),
                'updated_at' => now(),
            ]);
    }

    public function hasPublishedAssignmentRequest(string $externalCallId): bool
    {
        return DB::table('telephony_outbox')
            ->where('external_call_id', $externalCallId)
            ->where('type', 'call_assignment_requested')
            ->where('status', 'published')
            ->whereNotNull('published_at')
            ->exists();
    }

    /**
     * @param  array<string, int|string>  $payload
     */
    private function record(string $type, string $externalCallId, string $idempotencyKey, array $payload): void
    {
        DB::table('telephony_outbox')->insertOrIgnore([
            'command_id' => (string) Str::uuid(),
            'idempotency_key' => $idempotencyKey,
            'type' => $type,
            'external_call_id' => $externalCallId,
            'payload' => json_encode($payload, JSON_THROW_ON_ERROR),
            'status' => 'pending',
            'canceled_at' => null,
            'cancel_reason' => null,
            'created_at' => now(),
            'updated_at' => now(),
        ]);
    }
}
