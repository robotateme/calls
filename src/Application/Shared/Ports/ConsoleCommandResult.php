<?php

declare(strict_types=1);

namespace Application\Shared\Ports;

final readonly class ConsoleCommandResult
{
    public function __construct(
        public int $exitCode,
        public string $stdout,
        public string $stderr,
    ) {}

    public function successful(): bool
    {
        return $this->exitCode === 0;
    }
}
