<?php

declare(strict_types=1);

namespace Application\Calls\Commands;

use Application\Shared\Ports\Metrics;
use Application\Telephony\Ports\TelephonyCommandPublisher;
use Application\Telephony\Ports\TelephonyOutboxWriteRepository;
use Throwable;

final readonly class PublishTelephonyOutboxHandler
{
    public function __construct(
        private TelephonyOutboxWriteRepository $outbox,
        private TelephonyCommandPublisher $publisher,
        private Metrics $metrics,
    ) {}

    public function handle(int $limit, int $retryDelaySeconds, int $maxAttempts): PublishTelephonyOutboxResult
    {
        $startedAt = microtime(true);
        $messages = $this->outbox->claimDue(max(1, $limit));
        $published = 0;
        $failed = 0;

        foreach ($messages as $message) {
            try {
                $this->publisher->publish($message);
                $this->outbox->markPublished($message->id);
                $published++;
            } catch (Throwable $exception) {
                $this->outbox->markFailed(
                    id: $message->id,
                    error: $exception->getMessage(),
                    retryDelaySeconds: max(0, $retryDelaySeconds),
                    maxAttempts: max(1, $maxAttempts),
                );
                $failed++;
            }
        }

        $this->metrics->increment('telephony_outbox.claimed', count($messages));
        $this->metrics->increment('telephony_outbox.published', $published);
        $this->metrics->increment('telephony_outbox.publish_failed', $failed);
        $this->metrics->timing('telephony_outbox.publish_duration_ms', (microtime(true) - $startedAt) * 1000);

        return new PublishTelephonyOutboxResult(
            claimed: count($messages),
            published: $published,
            failed: $failed,
        );
    }
}
