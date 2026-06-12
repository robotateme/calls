<?php

declare(strict_types=1);

namespace Domain\Calls;

use InvalidArgumentException;

final readonly class ExternalCallId
{
    private function __construct(private string $value) {}

    public static function fromString(string $value): self
    {
        $value = trim($value);

        if ($value === '') {
            throw new InvalidArgumentException('External call id must not be empty.');
        }

        return new self($value);
    }

    public function toString(): string
    {
        return $this->value;
    }
}
