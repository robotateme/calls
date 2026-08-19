<?php

declare(strict_types=1);

namespace Tests\Feature;

use App\Models\Call;
use Application\Calls\Ports\CallProcessingQueue;
use Application\Shared\Ports\KafkaConsumer;
use Application\Shared\Ports\KafkaConsumerMessage;
use Application\Shared\Ports\Metrics;
use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Testing\PendingCommand;
use Tests\TestCase;

final class KafkaConsumeCommandTest extends TestCase
{
    use RefreshDatabase;

    public function test_it_consumes_kafka_records_through_configured_consumer(): void
    {
        $queue = new KafkaConsumeCallProcessingQueue;
        $this->app->instance(CallProcessingQueue::class, $queue);
        $this->app->instance(KafkaConsumer::class, new FakeKafkaConsumer([
            new KafkaConsumerMessage(
                source: 'fake-consumer',
                topic: 'incoming-calls',
                partition: 0,
                offset: 9001,
                key: 'asterisk-linkedid-consume-1',
                traceId: 'trace-consume-1',
                payload: json_encode([
                    'schema_version' => 1,
                    'external_call_id' => 'asterisk-linkedid-consume-1',
                    'phone' => '+15550008001',
                ], JSON_THROW_ON_ERROR),
            ),
        ]));

        $command = $this->artisan('calls:kafka:consume', [
            'topic' => 'incoming-calls',
            '--group' => 'calls-test',
            '--source' => 'fake-consumer',
            '--limit' => '10',
            '--timeout-ms' => '100',
        ]);

        if (! $command instanceof PendingCommand) {
            $this->fail('Expected a pending artisan command.');
        }

        $command
            ->expectsOutputToContain('Kafka consumer processed records: 1')
            ->assertSuccessful()
            ->run();

        $call = Call::query()->where('external_call_id', 'asterisk-linkedid-consume-1')->firstOrFail();

        $this->assertSame('+15550008001', $call->phone);
        $this->assertSame('incoming-calls-0-9001', $call->kafka_message_id);
        $this->assertSame([(int) $call->id], $queue->callIds);
    }

    public function test_it_returns_failure_when_consumer_transport_throws(): void
    {
        $metrics = new KafkaConsumeMetrics;
        $this->app->instance(Metrics::class, $metrics);
        $this->app->instance(KafkaConsumer::class, new FailingKafkaConsumer('Kafka transport unavailable'));

        $command = $this->artisan('calls:kafka:consume', [
            'topic' => 'incoming-calls',
            '--group' => 'calls-test',
            '--source' => 'fake-consumer',
            '--limit' => '10',
            '--timeout-ms' => '100',
        ]);

        if (! $command instanceof PendingCommand) {
            $this->fail('Expected a pending artisan command.');
        }

        $command
            ->expectsOutputToContain('Kafka consumer failed: Kafka transport unavailable')
            ->assertFailed()
            ->run();

        $this->assertContains(['kafka_consumer_failures_total', 1, [
            'source' => 'fake-consumer',
            'topic' => 'incoming-calls',
            'reason' => 'consumer_failed',
        ]], $metrics->counters);
    }
}

final readonly class FakeKafkaConsumer implements KafkaConsumer
{
    /**
     * @param  list<KafkaConsumerMessage>  $messages
     */
    public function __construct(private array $messages) {}

    public function consume(
        string $topic,
        string $groupId,
        string $source,
        int $limit,
        int $timeoutMs,
        callable $handler,
    ): int {
        $consumed = 0;

        foreach ($this->messages as $message) {
            if ($consumed >= $limit) {
                break;
            }

            $handler($message);
            $consumed++;
        }

        return $consumed;
    }
}

final readonly class FailingKafkaConsumer implements KafkaConsumer
{
    public function __construct(private string $message) {}

    public function consume(
        string $topic,
        string $groupId,
        string $source,
        int $limit,
        int $timeoutMs,
        callable $handler,
    ): int {
        throw new \RuntimeException($this->message);
    }
}

final class KafkaConsumeCallProcessingQueue implements CallProcessingQueue
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

final class KafkaConsumeMetrics implements Metrics
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
