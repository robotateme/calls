<?php

declare(strict_types=1);

namespace Application\Calls\Commands;

use Application\Shared\Ports\Metrics;
use Application\Telephony\Ports\TelephonyOutboxWriteRepository;

final readonly class RequeueStaleTelephonyOutboxHandler
{
    public function __construct(
        private TelephonyOutboxWriteRepository $outbox,
        private Metrics $metrics,
    ) {}

    public function handle(int $olderThanSeconds, int $limit): int
    {
        $startedAt = microtime(true);
        $processingTimeoutSeconds = max(0, $olderThanSeconds);
        $requeueLimit = max(1, $limit);
        $requeued = count($this->outbox->requeueStaleProcessing(
            olderThanSeconds: $processingTimeoutSeconds,
            limit: $requeueLimit,
        ));

        $this->metrics->increment('telephony_outbox.stale_requeued', $requeued);
        $this->metrics->timing('telephony_outbox.stale_requeue_duration_ms', (microtime(true) - $startedAt) * 1000);

        return $requeued;
    }
}
