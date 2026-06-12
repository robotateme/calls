<?php

declare(strict_types=1);

namespace Domain\Calls;

use Domain\Shared\Timestamp;
use InvalidArgumentException;

final readonly class OperatorSearchRetryDelay
{
    private function __construct(private int $seconds) {}

    public static function fromSeconds(int $seconds): self
    {
        if ($seconds < 0) {
            throw new InvalidArgumentException('Operator search retry delay must not be negative.');
        }

        return new self($seconds);
    }

    public function seconds(): int
    {
        return $this->seconds;
    }

    public function nextAttemptFrom(Timestamp $timestamp): Timestamp
    {
        return $timestamp->plusSeconds($this->seconds);
    }
}
