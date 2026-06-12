<?php

declare(strict_types=1);

namespace Tests\Feature;

use App\Models\Call;
use App\Models\Operator;
use Application\Calls\Commands\ReleaseExpiredOperatorReservationsHandler;
use Application\Calls\Ports\CallProcessingRetryQueue;
use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Str;
use Illuminate\Testing\PendingCommand;
use Tests\TestCase;

final class ReleaseExpiredOperatorReservationsHandlerTest extends TestCase
{
    use RefreshDatabase;

    public function test_it_releases_expired_assignment_and_schedules_retry(): void
    {
        $retryQueue = new FakeExpiredReservationRetryQueue;
        $this->app->instance(CallProcessingRetryQueue::class, $retryQueue);

        $call = $this->createCall([
            'external_call_id' => 'asterisk-linkedid-expired-1',
            'status' => 'assignment_requested',
            'operator_search_attempts' => 1,
            'operator_search_max_attempts' => 3,
            'operator_search_retry_delay_seconds' => 20,
        ]);
        $operator = $this->reserveOperator($call, now()->subMinutes(5));
        $call->forceFill(['operator_id' => $operator->id])->save();
        $this->insertOutbox([
            'external_call_id' => 'asterisk-linkedid-expired-1',
            'idempotency_key' => 'asterisk-linkedid-expired-1:call_assignment_requested:1',
            'status' => 'published',
            'published_at' => now()->subMinutes(4),
        ]);

        $released = $this->handler()->handle(olderThanSeconds: 120, limit: 10);

        $this->assertSame(1, $released);
        $this->assertDatabaseHas('calls', [
            'id' => $call->id,
            'operator_id' => null,
            'status' => 'waiting',
            'operator_search_attempts' => 1,
        ]);
        $this->assertDatabaseHas('operators', [
            'id' => $operator->id,
            'reserved_call_id' => null,
            'reserved_at' => null,
        ]);
        $this->assertDatabaseHas('telephony_outbox', [
            'external_call_id' => 'asterisk-linkedid-expired-1',
            'type' => 'call_assignment_canceled',
            'idempotency_key' => 'asterisk-linkedid-expired-1:call_assignment_canceled:1',
            'status' => 'pending',
        ]);
        $this->assertDatabaseHas('telephony_outbox', [
            'external_call_id' => 'asterisk-linkedid-expired-1',
            'type' => 'operator_search_retry_scheduled',
            'idempotency_key' => 'asterisk-linkedid-expired-1:operator_search_retry_scheduled:1',
        ]);
        $this->assertSame([[$call->id, 20]], $retryQueue->retries);
    }

    public function test_it_releases_expired_assignment_and_finishes_when_attempts_are_exhausted(): void
    {
        $retryQueue = new FakeExpiredReservationRetryQueue;
        $this->app->instance(CallProcessingRetryQueue::class, $retryQueue);

        $call = $this->createCall([
            'external_call_id' => 'asterisk-linkedid-expired-2',
            'status' => 'operator_ringing',
            'operator_search_attempts' => 1,
            'operator_search_max_attempts' => 1,
            'operator_search_hangup_policy' => 'callback_missed',
        ]);
        $operator = $this->reserveOperator($call, now()->subMinutes(5));
        $call->forceFill(['operator_id' => $operator->id])->save();

        $released = $this->handler()->handle(olderThanSeconds: 120, limit: 10);

        $this->assertSame(1, $released);
        $this->assertDatabaseHas('calls', [
            'id' => $call->id,
            'operator_id' => null,
            'status' => 'callback_missed',
        ]);
        $this->assertDatabaseHas('operators', [
            'id' => $operator->id,
            'reserved_call_id' => null,
            'reserved_at' => null,
        ]);
        $this->assertDatabaseHas('telephony_outbox', [
            'external_call_id' => 'asterisk-linkedid-expired-2',
            'type' => 'operator_search_exhausted',
            'idempotency_key' => 'asterisk-linkedid-expired-2:operator_search_exhausted:1',
        ]);
        $this->assertSame([], $retryQueue->retries);
    }

    public function test_artisan_command_releases_expired_reservations_once(): void
    {
        $retryQueue = new FakeExpiredReservationRetryQueue;
        $this->app->instance(CallProcessingRetryQueue::class, $retryQueue);

        $call = $this->createCall([
            'external_call_id' => 'asterisk-linkedid-expired-3',
            'status' => 'assignment_requested',
            'operator_search_attempts' => 1,
            'operator_search_max_attempts' => 2,
        ]);
        $operator = $this->reserveOperator($call, now()->subMinutes(5));
        $call->forceFill(['operator_id' => $operator->id])->save();

        $command = $this->artisan('calls:operator-reservations:release-expired', [
            '--older-than' => 120,
            '--limit' => 10,
        ]);

        if (! $command instanceof PendingCommand) {
            $this->fail('Expected a pending artisan command.');
        }

        $command
            ->expectsOutputToContain('Expired operator reservations released: 1')
            ->assertSuccessful()
            ->run();

        $this->assertSame([[$call->id, 0]], $retryQueue->retries);
    }

    /**
     * @param  array<string, mixed>  $attributes
     */
    private function createCall(array $attributes): Call
    {
        return Call::query()->create(array_merge([
            'external_call_id' => 'asterisk-linkedid-expired-default',
            'phone' => '+15550000000',
            'kafka_message_id' => 'calls-expired-default',
            'status' => 'assignment_requested',
            'operator_search_attempts' => 1,
            'operator_search_max_attempts' => 3,
            'operator_search_retry_delay_seconds' => 0,
            'operator_search_hangup_policy' => 'missed',
            'assignment_requested_at' => now()->subMinutes(5),
        ], $attributes));
    }

    private function reserveOperator(Call $call, mixed $reservedAt): Operator
    {
        return Operator::query()->create([
            'name' => 'Expired Reservation Operator',
            'available' => true,
            'afk' => false,
            'reserved_call_id' => $call->id,
            'reserved_at' => $reservedAt,
            'last_call_at' => now()->subMinutes(5),
        ]);
    }

    /**
     * @param  array<string, mixed>  $attributes
     */
    private function insertOutbox(array $attributes): int
    {
        return (int) DB::table('telephony_outbox')->insertGetId(array_merge([
            'command_id' => (string) Str::uuid(),
            'idempotency_key' => 'asterisk-linkedid-expired-default:call_assignment_requested:1',
            'type' => 'call_assignment_requested',
            'external_call_id' => 'asterisk-linkedid-expired-default',
            'payload' => json_encode([
                'external_call_id' => 'asterisk-linkedid-expired-default',
            ], JSON_THROW_ON_ERROR),
            'status' => 'pending',
            'attempts' => 0,
            'available_at' => null,
            'published_at' => null,
            'canceled_at' => null,
            'cancel_reason' => null,
            'last_error' => null,
            'created_at' => now(),
            'updated_at' => now(),
        ], [
            ...$attributes,
            'payload' => json_encode($attributes['payload'] ?? [
                'external_call_id' => $attributes['external_call_id'] ?? 'asterisk-linkedid-expired-default',
            ], JSON_THROW_ON_ERROR),
        ]));
    }

    private function handler(): ReleaseExpiredOperatorReservationsHandler
    {
        return $this->app->make(ReleaseExpiredOperatorReservationsHandler::class);
    }
}

final class FakeExpiredReservationRetryQueue implements CallProcessingRetryQueue
{
    /**
     * @var list<array{0: int, 1: int}>
     */
    public array $retries = [];

    public function retryLater(int $callId, int $delaySeconds): void
    {
        $this->retries[] = [$callId, $delaySeconds];
    }
}
