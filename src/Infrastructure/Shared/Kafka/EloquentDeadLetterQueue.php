<?php

declare(strict_types=1);

namespace Infrastructure\Shared\Kafka;

use Application\Shared\Ports\DeadLetterQueue;
use Application\Shared\Ports\Metrics;
use Illuminate\Support\Facades\DB;

final readonly class EloquentDeadLetterQueue implements DeadLetterQueue
{
    public function __construct(private Metrics $metrics) {}

    public function record(
        string $source,
        string $topic,
        ?int $partition,
        ?int $offset,
        ?string $messageKey,
        ?string $traceId,
        string $reason,
        string $rawPayload,
        ?array $decodedPayload = null,
    ): void {
        $inserted = DB::table('dead_letter_messages')->insertOrIgnore([
            'source' => $source,
            'topic' => $topic,
            'message_partition' => $partition,
            'message_offset' => $offset,
            'message_key' => $messageKey,
            'trace_id' => $traceId,
            'reason' => $reason,
            'raw_payload' => $rawPayload,
            'decoded_payload' => $decodedPayload === null ? null : json_encode($decodedPayload, JSON_THROW_ON_ERROR),
            'message_hash' => $this->messageHash($source, $topic, $partition, $offset, $messageKey, $reason, $rawPayload),
            'created_at' => now(),
        ]);

        $this->metrics->increment('dead_letter.recorded', tags: [
            'source' => $source,
            'topic' => $topic,
            'reason' => $reason,
            'result' => $inserted > 0 ? 'inserted' : 'duplicate',
        ]);
    }

    private function messageHash(
        string $source,
        string $topic,
        ?int $partition,
        ?int $offset,
        ?string $messageKey,
        string $reason,
        string $rawPayload,
    ): string {
        return hash('sha256', json_encode([
            'source' => $source,
            'topic' => $topic,
            'partition' => $partition,
            'offset' => $offset,
            'message_key' => $messageKey,
            'reason' => $reason,
            'raw_payload' => $rawPayload,
        ], JSON_THROW_ON_ERROR));
    }
}
