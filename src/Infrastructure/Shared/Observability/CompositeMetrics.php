<?php

declare(strict_types=1);

namespace Infrastructure\Shared\Observability;

use Application\Shared\Ports\Metrics;

final readonly class CompositeMetrics implements Metrics
{
    /**
     * @param  list<Metrics>  $metrics
     */
    public function __construct(private array $metrics) {}

    public function increment(string $name, int $value = 1, array $tags = []): void
    {
        foreach ($this->metrics as $metric) {
            $metric->increment($name, $value, $tags);
        }
    }

    public function gauge(string $name, int|float $value, array $tags = []): void
    {
        foreach ($this->metrics as $metric) {
            $metric->gauge($name, $value, $tags);
        }
    }

    public function timing(string $name, int|float $milliseconds, array $tags = []): void
    {
        foreach ($this->metrics as $metric) {
            $metric->timing($name, $milliseconds, $tags);
        }
    }
}
