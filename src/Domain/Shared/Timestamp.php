<?php

declare(strict_types=1);

namespace Domain\Shared;

use DateTimeImmutable;
use DateTimeInterface;
use InvalidArgumentException;

final readonly class Timestamp
{
    private function __construct(private DateTimeImmutable $value) {}

    public static function now(): self
    {
        return new self(new DateTimeImmutable);
    }

    public static function fromDateTime(DateTimeInterface $value): self
    {
        return new self(DateTimeImmutable::createFromInterface($value));
    }

    public static function fromString(string $value): self
    {
        return new self(new DateTimeImmutable($value));
    }

    public function plusSeconds(int $seconds): self
    {
        if ($seconds < 0) {
            throw new InvalidArgumentException('Seconds must be greater than or equal to zero.');
        }

        return new self($this->value->modify("+{$seconds} seconds"));
    }

    public function minusSeconds(int $seconds): self
    {
        if ($seconds < 0) {
            throw new InvalidArgumentException('Seconds must be greater than or equal to zero.');
        }

        return new self($this->value->modify("-{$seconds} seconds"));
    }

    public function toDateTimeImmutable(): DateTimeImmutable
    {
        return $this->value;
    }

    public function toAtom(): string
    {
        return $this->value->format(DateTimeInterface::ATOM);
    }

    public function toDatabaseString(): string
    {
        return $this->value->format('Y-m-d H:i:s');
    }
}
