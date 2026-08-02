<?php

declare(strict_types=1);

namespace Tests\Feature;

use Application\Shared\Ports\Metrics;
use Illuminate\Support\Facades\Cache;
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
}
