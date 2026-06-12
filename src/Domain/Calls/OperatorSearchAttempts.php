<?php

declare(strict_types=1);

namespace Domain\Calls;

use InvalidArgumentException;

final readonly class OperatorSearchAttempts
{
    private function __construct(private int $value) {}

    public static function fromInt(int $value): self
    {
        if ($value < 0) {
            throw new InvalidArgumentException('Operator search attempts must not be negative.');
        }

        return new self($value);
    }

    public function increment(): self
    {
        return new self($this->value + 1);
    }

    public function isLessThan(OperatorSearchMaxAttempts $maxAttempts): bool
    {
        return $this->value < $maxAttempts->toInt();
    }

    public function toInt(): int
    {
        return $this->value;
    }
}
