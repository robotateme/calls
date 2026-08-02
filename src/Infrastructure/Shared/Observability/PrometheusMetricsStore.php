<?php

declare(strict_types=1);

namespace Infrastructure\Shared\Observability;

use Application\Shared\Ports\Metrics;
use Illuminate\Cache\Repository;
use Illuminate\Contracts\Cache\Factory as CacheFactory;

final class PrometheusMetricsStore implements Metrics
{
    private const REGISTRY_LOCK_SECONDS = 5;

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
        $series = $this->registerSeries($name, 'counter', $tags);

        $this->cache->lock($this->seriesLockKey($series['id']), self::REGISTRY_LOCK_SECONDS)->block(1, function () use ($series, $value): void {
            $valueKey = $this->seriesValueKey($series['id'], 'value');
            $current = (int) $this->cache->get($valueKey, 0);

            $this->cache->forever($valueKey, $current + $value);
        });
    }

    public function gauge(string $name, int|float $value, array $tags = []): void
    {
        $series = $this->registerSeries($name, 'gauge', $tags);

        $this->cache->forever($this->seriesValueKey($series['id'], 'value'), $value);
    }

    public function timing(string $name, int|float $milliseconds, array $tags = []): void
    {
        $series = $this->registerSeries($name, 'summary', $tags);

        $this->cache->lock($this->seriesLockKey($series['id']), self::REGISTRY_LOCK_SECONDS)->block(1, function () use ($series, $milliseconds): void {
            $countKey = $this->seriesValueKey($series['id'], 'count');
            $sumKey = $this->seriesValueKey($series['id'], 'sum');

            $currentCount = (int) $this->cache->get($countKey, 0);
            $currentSum = (float) $this->cache->get($sumKey, 0.0);

            $this->cache->forever($countKey, $currentCount + 1);
            $this->cache->forever($sumKey, $currentSum + $milliseconds);
        });
    }

    /**
     * @return list<array{
     *     id: string,
     *     name: string,
     *     type: string,
     *     labels: array<string, string>,
     *     value?: int|float,
     *     count?: int,
     *     sum?: float
     * }>
     */
    public function snapshot(): array
    {
        /** @var array<string, array{id: string, name: string, type: string, labels: array<string, string>}> $registry */
        $registry = $this->cache->get($this->registryKey(), []);
        $snapshot = [];

        foreach ($registry as $series) {
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
     * @return array{id: string, name: string, type: string, labels: array<string, string>}
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

        $this->cache->lock($this->registryLockKey(), self::REGISTRY_LOCK_SECONDS)->block(1, function () use ($series, $seriesId): void {
            /** @var array<string, array{id: string, name: string, type: string, labels: array<string, string>}> $registry */
            $registry = $this->cache->get($this->registryKey(), []);
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
}
