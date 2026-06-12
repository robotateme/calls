<?php

declare(strict_types=1);

namespace Application\Shared\Ports;

interface Metrics
{
    /**
     * @param  array<string, int|string>  $tags
     */
    public function increment(string $name, int $value = 1, array $tags = []): void;

    /**
     * @param  array<string, int|string>  $tags
     */
    public function gauge(string $name, int|float $value, array $tags = []): void;

    /**
     * @param  array<string, int|string>  $tags
     */
    public function timing(string $name, int|float $milliseconds, array $tags = []): void;
}
