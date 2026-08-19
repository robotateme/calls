<?php

declare(strict_types=1);

namespace Tests\Feature;

use Application\Shared\Ports\Metrics;
use Illuminate\Support\Facades\Cache;
use Infrastructure\Shared\Observability\PrometheusMetricsStore;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Tests\TestCase;

final class MetricsEndpointTest extends TestCase
{
    protected function setUp(): void
    {
        parent::setUp();

        config()->set('cache.default', 'array');
        config()->set('calls.metrics_cache_store', 'array');

        Cache::store('array')->flush();
        $this->app->instance(LoggerInterface::class, new NullLogger);
    }

    public function test_root_route_is_not_exposed(): void
    {
        $this->get('/')->assertNotFound();
    }

    public function test_it_exposes_metrics_in_prometheus_text_format(): void
    {
        $metrics = $this->app->make(Metrics::class);

        $metrics->increment('operator_assignment.requested', 2, ['result' => 'assigned']);
        $metrics->gauge('telephony_outbox.depth', 12, ['status' => 'pending']);
        $metrics->timing('call_processing.duration_ms', 15.5, ['result' => 'retry_scheduled']);

        $response = $this->get('/metrics');

        $response->assertOk();
        $this->assertSame('text/plain; version=0.0.4; charset=utf-8', $response->headers->get('content-type'));

        $content = (string) $response->getContent();

        $this->assertStringContainsString('# TYPE operator_assignment_requested counter', $content);
        $this->assertStringContainsString('operator_assignment_requested{result="assigned"} 2', $content);
        $this->assertStringContainsString('# TYPE telephony_outbox_depth gauge', $content);
        $this->assertStringContainsString('telephony_outbox_depth{status="pending"} 12', $content);
        $this->assertStringContainsString('# TYPE call_processing_duration_ms summary', $content);
        $this->assertStringContainsString('call_processing_duration_ms_sum{result="retry_scheduled"} 15.5', $content);
        $this->assertStringContainsString('call_processing_duration_ms_count{result="retry_scheduled"} 1', $content);
    }

    public function test_it_requires_basic_auth_when_metrics_credentials_are_configured(): void
    {
        config()->set('calls.metrics_basic_auth_user', 'prometheus');
        config()->set('calls.metrics_basic_auth_password', 'secret');

        $this->get('/metrics')
            ->assertUnauthorized()
            ->assertHeader('WWW-Authenticate', 'Basic realm="Calls metrics"');

        $this->withHeaders([
            'Authorization' => 'Basic '.base64_encode('prometheus:wrong'),
        ])->get('/metrics')->assertUnauthorized();

        $this->withHeaders([
            'Authorization' => 'Basic '.base64_encode('prometheus:secret'),
        ])->get('/metrics')->assertOk();
    }

    public function test_it_rejects_metrics_when_basic_auth_is_partially_configured(): void
    {
        config()->set('calls.metrics_basic_auth_user', 'prometheus');
        config()->set('calls.metrics_basic_auth_password', null);

        $this->get('/metrics')->assertUnauthorized();
    }

    public function test_it_restricts_metrics_by_allowed_ip_when_configured(): void
    {
        config()->set('calls.metrics_basic_auth_user', 'prometheus');
        config()->set('calls.metrics_basic_auth_password', 'secret');
        config()->set('calls.metrics_allowed_ips', '10.10.0.0/16,127.0.0.1');

        $authorization = 'Basic '.base64_encode('prometheus:secret');

        $this->withServerVariables(['REMOTE_ADDR' => '192.168.1.10'])
            ->withHeaders(['Authorization' => $authorization])
            ->get('/metrics')
            ->assertUnauthorized();

        $this->withServerVariables(['REMOTE_ADDR' => '10.10.5.20'])
            ->withHeaders(['Authorization' => $authorization])
            ->get('/metrics')
            ->assertOk();
    }

    public function test_it_drops_stale_gauge_series_before_new_snapshot_values_are_written(): void
    {
        $metrics = $this->app->make(Metrics::class);
        $store = $this->app->make(PrometheusMetricsStore::class);

        $metrics->gauge('telephony_outbox.depth', 12, ['status' => 'pending']);
        $store->forgetGaugeSeries('telephony_outbox.depth');
        $metrics->gauge('telephony_outbox.depth', 3, ['status' => 'processing']);

        $content = (string) $this->get('/metrics')->getContent();

        $this->assertStringNotContainsString('telephony_outbox_depth{status="pending"} 12', $content);
        $this->assertStringContainsString('telephony_outbox_depth{status="processing"} 3', $content);
    }
}
