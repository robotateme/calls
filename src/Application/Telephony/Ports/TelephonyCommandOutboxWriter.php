<?php

declare(strict_types=1);

namespace Application\Telephony\Ports;

interface TelephonyCommandOutboxWriter
{
    /**
     * @throws \JsonException
     */
    public function recordCallAssignmentRequested(string $externalCallId, int $operatorId, int $attempt): void;

    /**
     * @throws \JsonException
     */
    public function recordCallAssignmentCanceled(string $externalCallId, int $operatorId, int $attempt, string $reason): void;

    /**
     * @throws \JsonException
     */
    public function recordOperatorSearchRetryScheduled(string $externalCallId, int $attempt, int $retryDelaySeconds): void;

    /**
     * @throws \JsonException
     */
    public function recordOperatorSearchExhausted(string $externalCallId, int $attempt, string $finalStatus): void;

    public function cancelPendingAssignmentRequests(string $externalCallId, string $reason): void;
}
