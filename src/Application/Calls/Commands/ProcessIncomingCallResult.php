<?php

declare(strict_types=1);

namespace Application\Calls\Commands;

final readonly class ProcessIncomingCallResult
{
    public function __construct(
        public int $callId,
        public string $externalCallId,
        public ?int $operatorId,
        public ?int $clientId,
        public bool $waitingForOperator,
        public int $attempt,
        public int $retryDelaySeconds,
        public ?string $finalStatus,
    ) {}
}
