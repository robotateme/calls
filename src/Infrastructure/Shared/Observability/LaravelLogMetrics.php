<?php

declare(strict_types=1);

namespace Infrastructure\Shared\Observability;

use Application\Shared\Ports\Metrics;
use Psr\Log\LoggerInterface;

final readonly class LaravelLogMetrics implements Metrics
{
    public function __construct(private LoggerInterface $logger) {}

    public function increment(string $name, int $value = 1, array $tags = []): void
    {
        $this->record('counter', $name, $value, $tags);
    }

    public function gauge(string $name, int|float $value, array $tags = []): void
    {
        $this->record('gauge', $name, $value, $tags);
    }

    public function timing(string $name, int|float $milliseconds, array $tags = []): void
    {
        $this->record('timing', $name, $milliseconds, $tags);
    }

    /**
     * @param  array<string, int|string>  $tags
     */
    private function record(string $type, string $name, int|float $value, array $tags): void
    {
        $this->logger->info('metric', [
            'type' => $type,
            'name' => $name,
            'value' => $value,
            'tags' => $tags,
        ]);
    }
}
