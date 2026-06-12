<?php

declare(strict_types=1);

namespace Tests\Feature;

use Application\Shared\Ports\DeadLetterQueue;
use Application\Shared\Ports\Metrics;
use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Support\Facades\DB;
use Tests\TestCase;

final class DeadLetterQueueTest extends TestCase
{
    use RefreshDatabase;

    public function test_it_records_dead_letter_message_idempotently(): void
    {
        $metrics = new FakeDeadLetterMetrics;
        $this->app->instance(Metrics::class, $metrics);

        $deadLetters = $this->app->make(DeadLetterQueue::class);

        $payload = [
            'external_call_id' => 'asterisk-linkedid-dlq-1',
            'phone' => '+15550009999',
        ];
        $rawPayload = json_encode($payload, JSON_THROW_ON_ERROR);

        $deadLetters->record(
            source: 'incoming-calls-consumer',
            topic: 'incoming-calls',
            partition: 2,
            offset: 42,
            messageKey: 'asterisk-linkedid-dlq-1',
            traceId: 'trace-dlq-1',
            reason: 'invalid_payload',
            rawPayload: $rawPayload,
            decodedPayload: $payload,
        );
        $deadLetters->record(
            source: 'incoming-calls-consumer',
            topic: 'incoming-calls',
            partition: 2,
            offset: 42,
            messageKey: 'asterisk-linkedid-dlq-1',
            traceId: 'trace-dlq-1',
            reason: 'invalid_payload',
            rawPayload: $rawPayload,
            decodedPayload: $payload,
        );

        $this->assertSame(1, DB::table('dead_letter_messages')->count());

        $row = DB::table('dead_letter_messages')->first();

        $this->assertNotNull($row);
        $this->assertSame('incoming-calls-consumer', $row->source);
        $this->assertSame('incoming-calls', $row->topic);
        $this->assertSame(2, $row->message_partition);
        $this->assertSame(42, $row->message_offset);
        $this->assertSame('asterisk-linkedid-dlq-1', $row->message_key);
        $this->assertSame('trace-dlq-1', $row->trace_id);
        $this->assertSame('invalid_payload', $row->reason);
        $this->assertSame($rawPayload, $row->raw_payload);
        $this->assertSame($payload, json_decode((string) $row->decoded_payload, true, flags: JSON_THROW_ON_ERROR));
        $this->assertNull($row->resolved_at);
        $this->assertNotEmpty($row->message_hash);

        $this->assertContains(['dead_letter.recorded', 1, [
            'source' => 'incoming-calls-consumer',
            'topic' => 'incoming-calls',
            'reason' => 'invalid_payload',
            'result' => 'inserted',
        ]], $metrics->counters);
        $this->assertContains(['dead_letter.recorded', 1, [
            'source' => 'incoming-calls-consumer',
            'topic' => 'incoming-calls',
            'reason' => 'invalid_payload',
            'result' => 'duplicate',
        ]], $metrics->counters);
    }
}

final class FakeDeadLetterMetrics implements Metrics
{
    /**
     * @var list<array{0: string, 1: int, 2: array<string, int|string>}>
     */
    public array $counters = [];

    public function increment(string $name, int $value = 1, array $tags = []): void
    {
        $this->counters[] = [$name, $value, $tags];
    }

    public function gauge(string $name, int|float $value, array $tags = []): void {}

    public function timing(string $name, int|float $milliseconds, array $tags = []): void {}
}
