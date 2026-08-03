<?php

declare(strict_types=1);

namespace Tests\Unit;

use Illuminate\Contracts\Cache\Factory as CacheFactory;
use Illuminate\Contracts\Cache\Repository;
use Illuminate\Contracts\Cache\Store;
use Infrastructure\Shared\Observability\PrometheusMetricsStore;
use Tests\TestCase;

final class PrometheusMetricsStoreTest extends TestCase
{
    public function test_it_ignores_invalid_utf8_series_registration_failures(): void
    {
        $store = new PrometheusMetricsStore(
            $this->app->make(CacheFactory::class),
            'array',
            'tests:metrics:invalid-utf8',
        );

        $store->gauge('calls.depth', 1, [
            'status' => "\xB1\x31",
        ]);

        $this->assertSame([], $store->snapshot());
    }

    public function test_it_ignores_missing_lock_support_in_cache_store(): void
    {
        $cacheFactory = $this->createMock(CacheFactory::class);
        $cache = $this->createMock(Repository::class);

        $cacheFactory
            ->expects($this->once())
            ->method('store')
            ->with()
            ->willReturn($cache);

        $cache
            ->method('getStore')
            ->willReturn(new NonLockingCacheStore);

        $cache
            ->expects($this->once())
            ->method('get')
            ->with('tests:metrics:no-lock:registry', [])
            ->willReturn([]);

        $store = new PrometheusMetricsStore($cacheFactory, null, 'tests:metrics:no-lock');

        $store->increment('calls.depth');

        $this->assertSame([], $store->snapshot());
    }
}

final class NonLockingCacheStore implements Store
{
    public function get($key): mixed
    {
        return null;
    }

    /**
     * @param  array<string>  $keys
     * @return array<string, mixed>
     */
    public function many(array $keys): array
    {
        return [];
    }

    public function put($key, $value, $seconds): bool
    {
        return true;
    }

    /**
     * @param  array<string, mixed>  $values
     */
    public function putMany(array $values, $seconds): bool
    {
        return true;
    }

    public function increment($key, $value = 1): int|bool
    {
        return true;
    }

    public function decrement($key, $value = 1): int|bool
    {
        return true;
    }

    public function forever($key, $value): bool
    {
        return true;
    }

    public function touch($key, $seconds): bool
    {
        return true;
    }

    public function forget($key): bool
    {
        return true;
    }

    public function flush(): bool
    {
        return true;
    }

    public function getPrefix(): string
    {
        return 'tests:';
    }
}
