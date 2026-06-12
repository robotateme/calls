<?php

declare(strict_types=1);

namespace Tests\Feature;

use App\Models\Call;
use App\Models\Operator;
use Application\Calls\Commands\HandleKafkaCallFactCommand;
use Application\Calls\Commands\HandleKafkaCallFactHandler;
use Application\Calls\Ports\CallProcessingQueue;
use Application\Shared\Ports\Metrics;
use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Support\Facades\DB;
use Tests\TestCase;

final class KafkaCallFactHandlerTest extends TestCase
{
    use RefreshDatabase;

    public function test_it_handles_incoming_call_payload_without_envelope(): void
    {
        $queue = new KafkaFactCallProcessingQueue;
        $metrics = new KafkaFactMetrics;
        $this->app->instance(CallProcessingQueue::class, $queue);
        $this->app->instance(Metrics::class, $metrics);

        $payload = [
            'external_call_id' => 'asterisk-linkedid-kafka-1',
            'phone' => '+15550007001',
            'operator_search_max_attempts' => 4,
            'operator_search_retry_delay_seconds' => 15,
            'operator_search_hangup_policy' => 'callback_missed',
        ];

        $this->handler()->handle(new HandleKafkaCallFactCommand(
            source: 'incoming-calls-consumer',
            topic: 'incoming-calls',
            partition: 0,
            offset: 701,
            messageKey: 'asterisk-linkedid-kafka-1',
            traceId: 'trace-kafka-1',
            rawPayload: json_encode($payload, JSON_THROW_ON_ERROR),
        ));

        $call = Call::query()->where('external_call_id', 'asterisk-linkedid-kafka-1')->firstOrFail();

        $this->assertSame('+15550007001', $call->phone);
        $this->assertSame('incoming-calls-0-701', $call->kafka_message_id);
        $this->assertSame('new', $call->status);
        $this->assertSame(4, $call->operator_search_max_attempts);
        $this->assertSame(15, $call->operator_search_retry_delay_seconds);
        $this->assertSame('callback_missed', $call->operator_search_hangup_policy);
        $this->assertSame([(int) $call->id], $queue->callIds);
        $this->assertSame(0, DB::table('dead_letter_messages')->count());
        $this->assertContains(['kafka_consumer.message_handled', 1, [
            'source' => 'incoming-calls-consumer',
            'topic' => 'incoming-calls',
            'type' => 'incoming_call_registered',
            'schema_version' => '1',
        ]], $metrics->counters);
    }

    public function test_it_handles_telephony_fact_envelope(): void
    {
        $metrics = new KafkaFactMetrics;
        $this->app->instance(Metrics::class, $metrics);

        $operator = Operator::query()->create([
            'name' => 'Kafka Operator',
            'available' => true,
            'afk' => false,
        ]);
        Call::query()->create([
            'external_call_id' => 'asterisk-linkedid-kafka-2',
            'phone' => '+15550007002',
            'kafka_message_id' => 'incoming-calls-0-702',
            'status' => 'assignment_requested',
            'operator_id' => $operator->id,
            'operator_search_attempts' => 1,
        ]);

        $payload = [
            'schema_version' => 1,
            'type' => 'operator_ringing',
            'payload' => [
                'external_call_id' => 'asterisk-linkedid-kafka-2',
                'operator_id' => $operator->id,
                'assignment_attempt' => 1,
            ],
        ];

        $this->handler()->handle(new HandleKafkaCallFactCommand(
            source: 'telephony-facts-consumer',
            topic: 'telephony.facts',
            partition: 1,
            offset: 702,
            messageKey: 'asterisk-linkedid-kafka-2',
            traceId: 'trace-kafka-2',
            rawPayload: json_encode($payload, JSON_THROW_ON_ERROR),
        ));

        $this->assertDatabaseHas('calls', [
            'external_call_id' => 'asterisk-linkedid-kafka-2',
            'status' => 'operator_ringing',
            'operator_id' => $operator->id,
        ]);
        $this->assertContains(['kafka_consumer.message_handled', 1, [
            'source' => 'telephony-facts-consumer',
            'topic' => 'telephony.facts',
            'type' => 'operator_ringing',
            'schema_version' => '1',
        ]], $metrics->counters);
    }

    public function test_it_records_unsupported_schema_version_to_dlq(): void
    {
        $metrics = new KafkaFactMetrics;
        $this->app->instance(Metrics::class, $metrics);

        $this->handler()->handle(new HandleKafkaCallFactCommand(
            source: 'telephony-facts-consumer',
            topic: 'telephony.facts',
            partition: 1,
            offset: 704,
            messageKey: 'asterisk-linkedid-kafka-4',
            traceId: 'trace-kafka-4',
            rawPayload: json_encode([
                'schema_version' => 2,
                'type' => 'operator_ringing',
                'payload' => [
                    'external_call_id' => 'asterisk-linkedid-kafka-4',
                    'operator_id' => 10,
                    'assignment_attempt' => 1,
                ],
            ], JSON_THROW_ON_ERROR),
        ));

        $this->assertDatabaseHas('dead_letter_messages', [
            'source' => 'telephony-facts-consumer',
            'topic' => 'telephony.facts',
            'message_partition' => 1,
            'message_offset' => 704,
            'trace_id' => 'trace-kafka-4',
            'reason' => 'unsupported_schema_version',
        ]);
        $this->assertContains(['kafka_consumer.message_dlq', 1, [
            'source' => 'telephony-facts-consumer',
            'topic' => 'telephony.facts',
            'reason' => 'unsupported_schema_version',
        ]], $metrics->counters);
    }

    public function test_it_records_invalid_message_to_dlq_with_trace_id(): void
    {
        $metrics = new KafkaFactMetrics;
        $this->app->instance(Metrics::class, $metrics);

        $this->handler()->handle(new HandleKafkaCallFactCommand(
            source: 'telephony-facts-consumer',
            topic: 'telephony.facts',
            partition: 1,
            offset: 703,
            messageKey: 'asterisk-linkedid-kafka-3',
            traceId: 'trace-kafka-3',
            rawPayload: json_encode([
                'schema_version' => 1,
                'type' => 'unknown_fact',
                'payload' => [
                    'external_call_id' => 'asterisk-linkedid-kafka-3',
                ],
            ], JSON_THROW_ON_ERROR),
        ));

        $this->assertDatabaseHas('dead_letter_messages', [
            'source' => 'telephony-facts-consumer',
            'topic' => 'telephony.facts',
            'message_partition' => 1,
            'message_offset' => 703,
            'message_key' => 'asterisk-linkedid-kafka-3',
            'trace_id' => 'trace-kafka-3',
            'reason' => 'unknown_type',
        ]);
        $this->assertContains(['kafka_consumer.message_dlq', 1, [
            'source' => 'telephony-facts-consumer',
            'topic' => 'telephony.facts',
            'reason' => 'unknown_type',
        ]], $metrics->counters);
    }

    private function handler(): HandleKafkaCallFactHandler
    {
        return $this->app->make(HandleKafkaCallFactHandler::class);
    }
}

final class KafkaFactCallProcessingQueue implements CallProcessingQueue
{
    /**
     * @var list<int>
     */
    public array $callIds = [];

    public function enqueue(int $callId): void
    {
        $this->callIds[] = $callId;
    }
}

final class KafkaFactMetrics implements Metrics
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
