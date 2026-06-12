<?php

declare(strict_types=1);

namespace Application\Telephony\Ports;

use Domain\Telephony\TelephonyOutboxMessage;

interface TelephonyOutboxWriteRepository
{
    /**
     * Claims due records for publishing. This is a write-side operation because it moves records to processing.
     *
     * @return list<TelephonyOutboxMessage>
     */
    public function claimDue(int $limit): array;

    /**
     * @return list<TelephonyOutboxMessage>
     */
    public function requeueStaleProcessing(int $olderThanSeconds, int $limit): array;

    public function markPublished(int $id): void;

    public function markFailed(int $id, string $error, int $retryDelaySeconds, int $maxAttempts): void;
}
