<?php

declare(strict_types=1);

namespace Tests\Unit;

use Infrastructure\Shared\Observability\LaravelLogMetrics;
use Psr\Log\AbstractLogger;
use Tests\TestCase;

final class LaravelLogMetricsTest extends TestCase
{
    public function test_it_writes_structured_metric_logs(): void
    {
        $logger = new FakeMetricsLogger;
        $metrics = new LaravelLogMetrics($logger);

        $metrics->increment('operator_assignment.requested', tags: ['result' => 'assigned']);
        $metrics->gauge('telephony_outbox.depth', 12);
        $metrics->timing('call_processing.duration_ms', 15.5, ['result' => 'retry_scheduled']);

        $this->assertSame(
            [
                [
                    'level' => 'info',
                    'message' => 'metric',
                    'context' => [
                        'type' => 'counter',
                        'name' => 'operator_assignment.requested',
                        'value' => 1,
                        'tags' => ['result' => 'assigned'],
                    ],
                ],
                [
                    'level' => 'info',
                    'message' => 'metric',
                    'context' => [
                        'type' => 'gauge',
                        'name' => 'telephony_outbox.depth',
                        'value' => 12,
                        'tags' => [],
                    ],
                ],
                [
                    'level' => 'info',
                    'message' => 'metric',
                    'context' => [
                        'type' => 'timing',
                        'name' => 'call_processing.duration_ms',
                        'value' => 15.5,
                        'tags' => ['result' => 'retry_scheduled'],
                    ],
                ],
            ],
            $logger->records,
        );
    }
}

final class FakeMetricsLogger extends AbstractLogger
{
    /**
     * @var list<array{level: mixed, message: string|\Stringable, context: array<string, mixed>}>
     */
    public array $records = [];

    public function log($level, string|\Stringable $message, array $context = []): void
    {
        $this->records[] = [
            'level' => $level,
            'message' => $message,
            'context' => $context,
        ];
    }
}
