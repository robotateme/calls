<?php

declare(strict_types=1);

namespace Domain\Calls;

final readonly class OperatorSearchOutcome
{
    private function __construct(
        private bool $operatorAssignmentRequested,
        private bool $retryScheduled,
        private int $retryDelaySeconds,
        private ?CallStatus $finalStatus,
    ) {}

    public static function assignmentRequested(): self
    {
        return new self(true, false, 0, null);
    }

    public static function retryScheduled(int $retryDelaySeconds): self
    {
        return new self(false, true, $retryDelaySeconds, null);
    }

    public static function exhausted(CallStatus $finalStatus): self
    {
        return new self(false, false, 0, $finalStatus);
    }

    public function isAssignmentRequested(): bool
    {
        return $this->operatorAssignmentRequested;
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
