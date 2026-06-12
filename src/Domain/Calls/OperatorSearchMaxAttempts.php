<?php

declare(strict_types=1);

namespace Domain\Calls;

use InvalidArgumentException;

final readonly class OperatorSearchMaxAttempts
{
    private function __construct(private int $value) {}

    public static function fromInt(int $value): self
    {
        if ($value < 1) {
            throw new InvalidArgumentException('Operator search max attempts must be positive.');
        }

        return new self($value);
    }

    public function toInt(): int
    {
        return $this->value;
    }
}
