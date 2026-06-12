<?php

declare(strict_types=1);

namespace Domain\Calls;

final readonly class OperatorAssignmentFailure
{
    private function __construct(
        private CallId $callId,
        private bool $retryScheduled,
        private int $retryDelaySeconds,
        private ?CallStatus $finalStatus,
    ) {}

    public static function retryScheduled(CallId $callId, int $retryDelaySeconds): self
    {
        return new self($callId, true, $retryDelaySeconds, null);
    }

    public static function exhausted(CallId $callId, CallStatus $finalStatus): self
    {
        return new self($callId, false, 0, $finalStatus);
    }

    public function callId(): int
    {
        return $this->callId->toInt();
    }

    public function shouldRetry(): bool
    {
        return $this->retryScheduled;
    }

    public function retryDelaySeconds(): int
    {
        return $this->retryDelaySeconds;
    }

    public function finalStatus(): ?CallStatus
    {
        return $this->finalStatus;
    }
}
