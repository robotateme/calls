<?php

declare(strict_types=1);

namespace Tests\Unit;

use App\Jobs\ProcessIncomingCallJob;
use Application\Shared\Ports\Metrics;
use Application\Shared\Ports\QueueBus;
use Infrastructure\Calls\Queue\LaravelCallProcessingRetryQueue;
use Tests\TestCase;

final class LaravelCallProcessingRetryQueueTest extends TestCase
{
    public function test_it_applies_minimum_delay_to_immediate_retries(): void
    {
        config()->set('calls.operator_search_retry_min_delay_seconds', 3);
        config()->set('calls.operator_search_retry_jitter_seconds', 0);
        config()->set('calls.operator_search_retry_max_delay_seconds', 3600);

        $queueBus = new FakeRetryQueueBus;
        $metrics = new FakeRetryMetrics;
        $queue = new LaravelCallProcessingRetryQueue($queueBus, $metrics);

        $queue->retryLater(callId: 10, delaySeconds: 0);

        $this->assertSame('calls-retry', $queueBus->queue);
        $this->assertInstanceOf(ProcessIncomingCallJob::class, $queueBus->message);
        $this->assertSame(3, $queueBus->message->delay);
        $this->assertSame([['call_retry.enqueued', 1, []]], $metrics->increments);
        $this->assertSame([['call_retry.delay_seconds', 3, []]], $metrics->gauges);
    }

    public function test_it_bounds_retry_jitter_by_maximum_delay(): void
    {
        config()->set('calls.operator_search_retry_min_delay_seconds', 1);
        config()->set('calls.operator_search_retry_jitter_seconds', 10);
        config()->set('calls.operator_search_retry_max_delay_seconds', 7);

        $queueBus = new FakeRetryQueueBus;
        $metrics = new FakeRetryMetrics;
        $queue = new LaravelCallProcessingRetryQueue($queueBus, $metrics);

        $queue->retryLater(callId: 10, delaySeconds: 5);

        $this->assertInstanceOf(ProcessIncomingCallJob::class, $queueBus->message);
        $this->assertGreaterThanOrEqual(5, $queueBus->message->delay);
        $this->assertLessThanOrEqual(7, $queueBus->message->delay);
    }
}

final class FakeRetryMetrics implements Metrics
{
    /**
     * @var list<array{0: string, 1: int, 2: array<string, int|string>}>
     */
    public array $increments = [];

    /**
     * @var list<array{0: string, 1: int|float, 2: array<string, int|string>}>
     */
    public array $gauges = [];

    public function increment(string $name, int $value = 1, array $tags = []): void
    {
        $this->increments[] = [$name, $value, $tags];
    }

    public function gauge(string $name, int|float $value, array $tags = []): void
    {
        $this->gauges[] = [$name, $value, $tags];
    }

    public function timing(string $name, int|float $milliseconds, array $tags = []): void {}
}

final class FakeRetryQueueBus implements QueueBus
{
    public ?object $message = null;

    public ?string $queue = null;

    public function dispatch(object $message, ?string $queue = null): void
    {
        $this->message = $message;
        $this->queue = $queue;
    }
}
