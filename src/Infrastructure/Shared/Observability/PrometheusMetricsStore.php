<?php

declare(strict_types=1);

namespace Infrastructure\Shared\Observability;

use Application\Shared\Ports\Metrics;
use Illuminate\Contracts\Cache\Factory as CacheFactory;
use Illuminate\Contracts\Cache\Lock;
use Illuminate\Contracts\Cache\LockProvider;
use Illuminate\Contracts\Cache\LockTimeoutException;
use Illuminate\Contracts\Cache\Repository;
use JsonException;
use LogicException;

/**
 * @phpstan-type MetricLabels array<string, string>
 * @phpstan-type RegisteredSummarySeries array{id: string, name: string, type: 'summary', labels: MetricLabels}
 * @phpstan-type RegisteredValueSeries array{id: string, name: string, type: 'counter'|'gauge', labels: MetricLabels}
 * @phpstan-type RegisteredSeries RegisteredSummarySeries|RegisteredValueSeries
 * @phpstan-type SnapshotSummarySeries array{id: string, name: string, type: 'summary', labels: MetricLabels, count: int, sum: float}
 * @phpstan-type SnapshotValueSeries array{id: string, name: string, type: 'counter'|'gauge', labels: MetricLabels, value: int|float}
 */
final class PrometheusMetricsStore implements Metrics
{
    private const int REGISTRY_LOCK_SECONDS = 5;

    private readonly Repository $cache;

    public function __construct(
        CacheFactory $cacheFactory,
        ?string $storeName,
        private readonly string $prefix,
    ) {
        $this->cache = $storeName !== null && $storeName !== ''
            ? $cacheFactory->store($storeName)
            : $cacheFactory->store();
    }

    public function increment(string $name, int $value = 1, array $tags = []): void
    {
        $this->rescue(function () use ($name, $value, $tags): void {
            $series = $this->registerSeries($name, 'counter', $tags);

            $this->lock($this->seriesLockKey($series['id']))->block(1, function () use ($series, $value): void {
                $valueKey = $this->seriesValueKey($series['id'], 'value');
                $current = (int) $this->cache->get($valueKey, 0);

                $this->cache->forever($valueKey, $current + $value);
            });
        });
    }

    public function gauge(string $name, int|float $value, array $tags = []): void
    {
        $this->rescue(function () use ($name, $value, $tags): void {
            $series = $this->registerSeries($name, 'gauge', $tags);

            $this->cache->forever($this->seriesValueKey($series['id'], 'value'), $value);
        });
    }

    public function timing(string $name, int|float $milliseconds, array $tags = []): void
    {
        $this->rescue(function () use ($name, $milliseconds, $tags): void {
            $series = $this->registerSeries($name, 'summary', $tags);

            $this->lock($this->seriesLockKey($series['id']))->block(1, function () use ($series, $milliseconds): void {
                $countKey = $this->seriesValueKey($series['id'], 'count');
                $sumKey = $this->seriesValueKey($series['id'], 'sum');

                $currentCount = (int) $this->cache->get($countKey, 0);
                $currentSum = (float) $this->cache->get($sumKey, 0.0);

                $this->cache->forever($countKey, $currentCount + 1);
                $this->cache->forever($sumKey, $currentSum + $milliseconds);
            });
        });
    }

    public function forgetGaugeSeries(string $name): void
    {
        $this->rescue(function () use ($name): void {
            $normalizedName = $this->normalizeMetricName($name);

            $this->lock($this->registryLockKey())->block(1, function () use ($normalizedName): void {
                $registry = $this->registry();
                $updatedRegistry = [];

                foreach ($registry as $seriesId => $series) {
                    if ($series['type'] === 'gauge' && $series['name'] === $normalizedName) {
                        $this->cache->forget($this->seriesValueKey($seriesId, 'value'));

                        continue;
                    }

                    $updatedRegistry[$seriesId] = $series;
                }

                $this->cache->forever($this->registryKey(), $updatedRegistry);
            });
        });
    }

    /** @phpstan-return list<SnapshotSummarySeries|SnapshotValueSeries> */
    public function snapshot(): array
    {
        $snapshot = [];

        foreach ($this->registry() as $series) {
            if ($series['type'] === 'summary') {
                $snapshot[] = [
                    'id' => $series['id'],
                    'name' => $series['name'],
                    'type' => $series['type'],
                    'labels' => $series['labels'],
                    'count' => (int) $this->cache->get($this->seriesValueKey($series['id'], 'count'), 0),
                    'sum' => (float) $this->cache->get($this->seriesValueKey($series['id'], 'sum'), 0.0),
                ];

                continue;
            }

            $snapshot[] = [
                'id' => $series['id'],
                'name' => $series['name'],
                'type' => $series['type'],
                'labels' => $series['labels'],
                'value' => $this->normalizeNumericValue(
                    $this->cache->get($this->seriesValueKey($series['id'], 'value'), 0),
                ),
            ];
        }

        usort($snapshot, function (array $left, array $right): int {
            return [$left['name'], $left['id']] <=> [$right['name'], $right['id']];
        });

        return $snapshot;
    }

    /**
     * @param  array<string, int|string>  $tags
     *
     * @phpstan-param  'counter'|'gauge'|'summary'  $type
     *
     * @phpstan-return RegisteredSeries
     *
     * @throws JsonException
     * @throws LogicException
     * @throws LockTimeoutException
     */
    private function registerSeries(string $name, string $type, array $tags): array
    {
        $normalizedName = $this->normalizeMetricName($name);
        $labels = $this->normalizeLabels($tags);
        $seriesId = sha1(json_encode([$normalizedName, $type, $labels], JSON_THROW_ON_ERROR));
        $series = [
            'id' => $seriesId,
            'name' => $normalizedName,
            'type' => $type,
            'labels' => $labels,
        ];

        $this->lock($this->registryLockKey())->block(1, function () use ($series, $seriesId): void {
            $registry = $this->registry();
            $registry[$seriesId] = $series;

            $this->cache->forever($this->registryKey(), $registry);
        });

        return $series;
    }

    /**
     * @param  array<string, int|string>  $tags
     * @return array<string, string>
     */
    private function normalizeLabels(array $tags): array
    {
        $labels = [];

        foreach ($tags as $key => $value) {
            $labels[$this->normalizeLabelName($key)] = (string) $value;
        }

        ksort($labels);

        return $labels;
    }

    private function normalizeMetricName(string $name): string
    {
        $normalized = preg_replace('/[^a-zA-Z0-9_:]/', '_', str_replace('.', '_', $name)) ?? $name;

        if ($normalized === '' || preg_match('/^[0-9]/', $normalized) === 1) {
            return '_'.$normalized;
        }

        return $normalized;
    }

    private function normalizeLabelName(string $name): string
    {
        $normalized = preg_replace('/[^a-zA-Z0-9_]/', '_', str_replace('.', '_', $name)) ?? $name;

        if ($normalized === '' || preg_match('/^[^a-zA-Z_]/', $normalized) === 1) {
            return '_'.$normalized;
        }

        return $normalized;
    }

    private function registryKey(): string
    {
        return $this->prefix.':registry';
    }

    private function registryLockKey(): string
    {
        return $this->prefix.':registry_lock';
    }

    private function seriesLockKey(string $seriesId): string
    {
        return $this->prefix.':series_lock:'.$seriesId;
    }

    private function seriesValueKey(string $seriesId, string $suffix): string
    {
        return $this->prefix.':series:'.$seriesId.':'.$suffix;
    }

    private function normalizeNumericValue(mixed $value): int|float
    {
        if (is_int($value) || is_float($value)) {
            return $value;
        }

        if (is_numeric($value)) {
            return str_contains((string) $value, '.')
                ? (float) $value
                : (int) $value;
        }

        return 0;
    }

    /** @phpstan-return array<string, RegisteredSeries> */
    private function registry(): array
    {
        $rawRegistry = $this->cache->get($this->registryKey(), []);

        if (! is_array($rawRegistry)) {
            return [];
        }

        $registry = [];

        foreach ($rawRegistry as $seriesId => $series) {
            if (! is_string($seriesId) || ! is_array($series)) {
                continue;
            }

            $normalizedSeries = $this->normalizeRegisteredSeries($series);

            if ($normalizedSeries === null) {
                continue;
            }

            $registry[$seriesId] = $normalizedSeries;
        }

        return $registry;
    }

    /**
     * @param  array<mixed>  $series
     *
     * @phpstan-return RegisteredSeries|null
     */
    private function normalizeRegisteredSeries(array $series): ?array
    {
        $id = $series['id'] ?? null;
        $name = $series['name'] ?? null;
        $type = $series['type'] ?? null;
        $labels = $series['labels'] ?? null;

        if (! is_string($id) || ! is_string($name) || ! is_string($type) || ! is_array($labels)) {
            return null;
        }

        $normalizedLabels = [];

        foreach ($labels as $key => $value) {
            if (! is_string($key) || ! is_string($value)) {
                return null;
            }

            $normalizedLabels[$key] = $value;
        }

        if ($type === 'summary') {
            return [
                'id' => $id,
                'name' => $name,
                'type' => 'summary',
                'labels' => $normalizedLabels,
            ];
        }

        if ($type === 'counter' || $type === 'gauge') {
            return [
                'id' => $id,
                'name' => $name,
                'type' => $type,
                'labels' => $normalizedLabels,
            ];
        }

        return null;
    }

    private function lock(string $key): Lock
    {
        $store = $this->cache->getStore();

        if (! $store instanceof LockProvider) {
            throw new LogicException(sprintf(
                'Cache store [%s] does not support locks required by PrometheusMetricsStore.',
                $store::class,
            ));
        }

        return $store->lock($key, self::REGISTRY_LOCK_SECONDS);
    }

    private function rescue(callable $callback): void
    {
        try {
            $callback();
        } catch (JsonException|LockTimeoutException|LogicException) {
            // Metrics are best-effort and must not interrupt call processing.
        }
    }
}
