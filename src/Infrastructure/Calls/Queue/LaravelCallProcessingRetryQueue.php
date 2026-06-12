<?php

declare(strict_types=1);

namespace Infrastructure\Calls\Queue;

use App\Jobs\ProcessIncomingCallJob;
use Application\Calls\Ports\CallProcessingRetryQueue;
use Application\Shared\Ports\Metrics;
use Application\Shared\Ports\QueueBus;

final readonly class LaravelCallProcessingRetryQueue implements CallProcessingRetryQueue
{
    public function __construct(
        private QueueBus $queue,
        private Metrics $metrics,
    ) {}

    public function retryLater(int $callId, int $delaySeconds): void
    {
        $queueDelaySeconds = $this->queueDelaySeconds($delaySeconds);
        $job = (new ProcessIncomingCallJob($callId))->delay($queueDelaySeconds);

        $this->queue->dispatch($job, 'calls-retry');

        $this->metrics->increment('call_retry.enqueued');
        $this->metrics->gauge('call_retry.delay_seconds', $queueDelaySeconds);
    }

    private function queueDelaySeconds(int $delaySeconds): int
    {
        $delay = max(
            max(0, $delaySeconds),
            max(0, (int) config('calls.operator_search_retry_min_delay_seconds')),
        );

        $jitterSeconds = max(0, (int) config('calls.operator_search_retry_jitter_seconds'));

        if ($jitterSeconds > 0) {
            $delay += random_int(0, $jitterSeconds);
        }

        $maxDelaySeconds = max(0, (int) config('calls.operator_search_retry_max_delay_seconds'));

        return $maxDelaySeconds > 0 ? min($delay, $maxDelaySeconds) : $delay;
    }
}
