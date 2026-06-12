<?php

declare(strict_types=1);

namespace Tests\Feature;

use App\Models\Call;
use App\Models\Operator;
use Application\Shared\Ports\Metrics;
use DateTimeInterface;
use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Str;
use Illuminate\Testing\PendingCommand;
use Tests\TestCase;

final class OperationalMetricsSnapshotTest extends TestCase
{
    use RefreshDatabase;

    public function test_it_records_operational_gauges(): void
    {
        config()->set('calls.operator_reservation_ttl_seconds', 120);

        $metrics = new FakeOperationalMetrics;
        $this->app->instance(Metrics::class, $metrics);

        Call::query()->create([
            'external_call_id' => 'metrics-call-1',
            'phone' => '+15550009001',
            'kafka_message_id' => 'metrics-1',
            'status' => 'waiting',
        ]);
        Call::query()->create([
            'external_call_id' => 'metrics-call-2',
            'phone' => '+15550009002',
            'kafka_message_id' => 'metrics-2',
            'status' => 'connected',
        ]);
        Operator::query()->create([
            'name' => 'Metrics Operator',
            'available' => true,
            'reserved_call_id' => 1,
            'reserved_at' => now()->subMinutes(5),
        ]);
        $this->insertOutbox('pending');
        $this->insertOutbox('processing');
        $this->insertDeadLetter('invalid_payload');
        $this->insertDeadLetter('invalid_payload');
        $this->insertDeadLetter('handler_failed', now());

        $command = $this->artisan('calls:metrics:snapshot');

        if (! $command instanceof PendingCommand) {
            $this->fail('Expected a pending artisan command.');
        }

        $command
            ->expectsOutputToContain('Operational metrics snapshot recorded.')
            ->assertSuccessful()
            ->run();

        $this->assertContains(['calls.depth', 1, ['status' => 'waiting']], $metrics->gauges);
        $this->assertContains(['calls.depth', 1, ['status' => 'connected']], $metrics->gauges);
        $this->assertContains(['telephony_outbox.depth', 1, ['status' => 'pending']], $metrics->gauges);
        $this->assertContains(['telephony_outbox.depth', 1, ['status' => 'processing']], $metrics->gauges);
        $this->assertContains(['dead_letter.depth', 2, ['reason' => 'invalid_payload']], $metrics->gauges);
        $this->assertContains(['operator_reservation.active', 1, []], $metrics->gauges);
        $this->assertContains(['operator_reservation.expired', 1, []], $metrics->gauges);
        $this->assertContains(['queue.depth', 0, ['queue' => 'calls']], $metrics->gauges);
        $this->assertContains(['queue.depth', 0, ['queue' => 'calls-retry']], $metrics->gauges);
    }

    private function insertOutbox(string $status): void
    {
        DB::table('telephony_outbox')->insert([
            'command_id' => (string) Str::uuid(),
            'idempotency_key' => 'metrics-outbox-'.$status,
            'type' => 'call_assignment_requested',
            'external_call_id' => 'metrics-call-'.$status,
            'payload' => json_encode(['external_call_id' => 'metrics-call-'.$status], JSON_THROW_ON_ERROR),
            'status' => $status,
            'attempts' => 0,
            'created_at' => now(),
            'updated_at' => now(),
        ]);
    }

    private function insertDeadLetter(string $reason, DateTimeInterface|string|null $resolvedAt = null): void
    {
        DB::table('dead_letter_messages')->insert([
            'source' => 'incoming-calls-consumer',
            'topic' => 'incoming-calls',
            'message_partition' => 0,
            'message_offset' => random_int(1, 1000000),
            'message_key' => (string) Str::uuid(),
            'trace_id' => (string) Str::uuid(),
            'reason' => $reason,
            'raw_payload' => '{}',
            'decoded_payload' => json_encode([], JSON_THROW_ON_ERROR),
            'message_hash' => (string) Str::uuid(),
            'resolved_at' => $resolvedAt,
            'created_at' => now(),
        ]);
    }
}

final class FakeOperationalMetrics implements Metrics
{
    /**
     * @var list<array{0: string, 1: int|float, 2: array<string, int|string>}>
     */
    public array $gauges = [];

    public function increment(string $name, int $value = 1, array $tags = []): void {}

    public function gauge(string $name, int|float $value, array $tags = []): void
    {
        $this->gauges[] = [$name, $value, $tags];
    }

    public function timing(string $name, int|float $milliseconds, array $tags = []): void {}
}
