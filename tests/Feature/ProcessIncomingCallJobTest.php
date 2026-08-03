<?php

declare(strict_types=1);

namespace Tests\Feature;

use App\Jobs\ProcessIncomingCallJob;
use App\Models\Call;
use App\Models\Client;
use App\Models\Operator;
use Application\Calls\Commands\ProcessIncomingCallHandler;
use Application\Calls\Ports\CallProcessingRetryQueue;
use Application\Shared\Ports\Metrics;
use Illuminate\Foundation\Testing\RefreshDatabase;
use Tests\TestCase;

final class ProcessIncomingCallJobTest extends TestCase
{
    use RefreshDatabase;

    public function test_it_assigns_new_call_to_available_operator(): void
    {
        $retryQueue = new FakeCallProcessingRetryQueue;
        $metrics = new FakeProcessIncomingCallMetrics;
        $this->app->instance(CallProcessingRetryQueue::class, $retryQueue);
        $this->app->instance(Metrics::class, $metrics);

        $client = Client::query()->create(['phone' => '+15550000001']);
        $operator = Operator::query()->create([
            'name' => 'Operator 1',
            'available' => true,
            'last_call_at' => now()->subHour(),
        ]);
        $call = $this->createCall([
            'external_call_id' => 'asterisk-linkedid-2001',
            'phone' => '+15550000001',
            'operator_search_max_attempts' => 3,
            'operator_search_retry_delay_seconds' => 15,
        ]);

        $this->handleJob($call);

        $this->assertDatabaseHas('calls', [
            'id' => $call->id,
            'client_id' => $client->id,
            'operator_id' => $operator->id,
            'status' => 'assignment_requested',
            'operator_search_attempts' => 1,
        ]);
        $this->assertDatabaseHas('operators', [
            'id' => $operator->id,
            'available' => true,
            'reserved_call_id' => $call->id,
        ]);
        $this->assertDatabaseHas('telephony_outbox', [
            'type' => 'call_assignment_requested',
            'external_call_id' => 'asterisk-linkedid-2001',
            'idempotency_key' => 'asterisk-linkedid-2001:call_assignment_requested:1',
            'status' => 'pending',
        ]);
        $this->assertSame([], $retryQueue->retries);
        $this->assertContains(['operator_reservation_total', 1, ['result' => 'success']], $metrics->increments);
        $this->assertContains(['operator_reservation_attempts_total', 1, ['result' => 'success']], $metrics->increments);
        $this->assertContains(['call_transitions_total', 1, ['from' => 'new', 'to' => 'assignment_requested']], $metrics->increments);
        $this->assertTimingRecorded('call_processing_duration_seconds', ['result' => 'assignment_requested'], $metrics);
    }

    public function test_it_does_not_allocate_afk_operator(): void
    {
        $retryQueue = new FakeCallProcessingRetryQueue;
        $metrics = new FakeProcessIncomingCallMetrics;
        $this->app->instance(CallProcessingRetryQueue::class, $retryQueue);
        $this->app->instance(Metrics::class, $metrics);

        $afkOperator = Operator::query()->create([
            'name' => 'AFK Operator',
            'available' => true,
            'afk' => true,
            'last_call_at' => now()->subHours(2),
        ]);
        $availableOperator = Operator::query()->create([
            'name' => 'Operator 1',
            'available' => true,
            'afk' => false,
            'last_call_at' => now()->subHour(),
        ]);
        $call = $this->createCall([
            'external_call_id' => 'asterisk-linkedid-2006',
            'phone' => '+15550000006',
            'operator_search_max_attempts' => 3,
        ]);

        $this->handleJob($call);

        $this->assertDatabaseHas('calls', [
            'id' => $call->id,
            'operator_id' => $availableOperator->id,
            'status' => 'assignment_requested',
        ]);
        $this->assertDatabaseHas('operators', [
            'id' => $afkOperator->id,
            'available' => true,
        ]);
        $this->assertDatabaseHas('telephony_outbox', [
            'type' => 'call_assignment_requested',
            'external_call_id' => 'asterisk-linkedid-2006',
            'idempotency_key' => 'asterisk-linkedid-2006:call_assignment_requested:1',
        ]);
        $this->assertSame([], $retryQueue->retries);
    }

    public function test_it_does_not_allocate_operator_reserved_by_another_call(): void
    {
        $retryQueue = new FakeCallProcessingRetryQueue;
        $metrics = new FakeProcessIncomingCallMetrics;
        $this->app->instance(CallProcessingRetryQueue::class, $retryQueue);
        $this->app->instance(Metrics::class, $metrics);

        $reservedByAnotherCall = $this->createCall([
            'external_call_id' => 'asterisk-linkedid-reserved-owner',
            'phone' => '+15550009999',
            'status' => 'assignment_requested',
        ]);
        $operator = Operator::query()->create([
            'name' => 'Operator 1',
            'available' => true,
            'afk' => false,
            'reserved_call_id' => $reservedByAnotherCall->id,
            'reserved_at' => now(),
        ]);
        $call = $this->createCall([
            'external_call_id' => 'asterisk-linkedid-2007',
            'phone' => '+15550000007',
            'operator_search_max_attempts' => 2,
        ]);

        $this->handleJob($call);

        $this->assertDatabaseHas('calls', [
            'id' => $call->id,
            'operator_id' => null,
            'status' => 'waiting',
            'operator_search_attempts' => 1,
        ]);
        $this->assertDatabaseHas('operators', [
            'id' => $operator->id,
            'available' => true,
            'reserved_call_id' => $reservedByAnotherCall->id,
        ]);
        $this->assertSame([[$call->id, 0]], $retryQueue->retries);
    }

    public function test_it_does_not_reprocess_call_that_is_not_processable(): void
    {
        $retryQueue = new FakeCallProcessingRetryQueue;
        $metrics = new FakeProcessIncomingCallMetrics;
        $this->app->instance(CallProcessingRetryQueue::class, $retryQueue);
        $this->app->instance(Metrics::class, $metrics);

        $operator = Operator::query()->create([
            'name' => 'Operator 1',
            'available' => false,
            'last_call_at' => now(),
        ]);
        $otherOperator = Operator::query()->create([
            'name' => 'Operator 2',
            'available' => true,
        ]);
        $call = $this->createCall([
            'external_call_id' => 'asterisk-linkedid-2002',
            'phone' => '+15550000002',
            'operator_id' => $operator->id,
            'status' => 'connected',
        ]);

        $this->handleJob($call);

        $this->assertDatabaseHas('calls', [
            'id' => $call->id,
            'operator_id' => $operator->id,
            'status' => 'connected',
        ]);
        $this->assertDatabaseHas('operators', [
            'id' => $otherOperator->id,
            'available' => true,
            'reserved_call_id' => null,
        ]);
        $this->assertDatabaseCount('telephony_outbox', 0);
        $this->assertSame([], $retryQueue->retries);
    }

    public function test_it_does_not_search_or_assign_operator_for_hung_up_call(): void
    {
        $retryQueue = new FakeCallProcessingRetryQueue;
        $this->app->instance(CallProcessingRetryQueue::class, $retryQueue);

        $client = Client::query()->create(['phone' => '+15550000008']);
        $operator = Operator::query()->create([
            'name' => 'Operator 1',
            'available' => true,
            'afk' => false,
            'last_call_at' => now()->subHour(),
        ]);
        $call = $this->createCall([
            'external_call_id' => 'asterisk-linkedid-2008',
            'phone' => '+15550000008',
            'status' => 'missed',
        ]);

        $this->handleJob($call);

        $this->assertDatabaseHas('calls', [
            'id' => $call->id,
            'client_id' => null,
            'operator_id' => null,
            'status' => 'missed',
            'operator_search_attempts' => 0,
        ]);
        $this->assertDatabaseHas('operators', [
            'id' => $operator->id,
            'available' => true,
            'reserved_call_id' => null,
        ]);
        $this->assertDatabaseMissing('calls', [
            'id' => $call->id,
            'client_id' => $client->id,
        ]);
        $this->assertDatabaseCount('telephony_outbox', 0);
        $this->assertSame([], $retryQueue->retries);
    }

    public function test_it_marks_call_waiting_and_retries_later_when_no_operator_is_available_and_attempts_remain(): void
    {
        $retryQueue = new FakeCallProcessingRetryQueue;
        $metrics = new FakeProcessIncomingCallMetrics;
        $this->app->instance(CallProcessingRetryQueue::class, $retryQueue);
        $this->app->instance(Metrics::class, $metrics);

        $client = Client::query()->create(['phone' => '+15550000003']);
        $call = $this->createCall([
            'external_call_id' => 'asterisk-linkedid-2003',
            'phone' => '+15550000003',
            'operator_search_max_attempts' => 3,
            'operator_search_retry_delay_seconds' => 15,
        ]);

        $this->handleJob($call);

        $this->assertDatabaseHas('calls', [
            'id' => $call->id,
            'client_id' => $client->id,
            'operator_id' => null,
            'status' => 'waiting',
            'operator_search_attempts' => 1,
        ]);
        $this->assertNotNull(Call::query()->findOrFail($call->id)->next_operator_search_at);
        $this->assertSame([[$call->id, 15]], $retryQueue->retries);
        $this->assertContains(['operator_reservation_total', 1, ['result' => 'no_available']], $metrics->increments);
        $this->assertContains(['operator_reservation_attempts_total', 1, ['result' => 'no_available']], $metrics->increments);
        $this->assertContains(['call_transitions_total', 1, ['from' => 'new', 'to' => 'waiting']], $metrics->increments);
        $this->assertContains(['retry_scheduled_total', 1, ['reason' => 'no_available_operator']], $metrics->increments);
        $this->assertTimingRecorded('call_processing_duration_seconds', ['result' => 'retry_scheduled'], $metrics);
        $this->assertDatabaseHas('telephony_outbox', [
            'type' => 'operator_search_retry_scheduled',
            'external_call_id' => 'asterisk-linkedid-2003',
            'idempotency_key' => 'asterisk-linkedid-2003:operator_search_retry_scheduled:1',
        ]);
    }

    public function test_it_assigns_waiting_call_when_operator_becomes_available(): void
    {
        $retryQueue = new FakeCallProcessingRetryQueue;
        $metrics = new FakeProcessIncomingCallMetrics;
        $this->app->instance(CallProcessingRetryQueue::class, $retryQueue);
        $this->app->instance(Metrics::class, $metrics);

        $operator = Operator::query()->create([
            'name' => 'Operator 1',
            'available' => true,
        ]);
        $call = $this->createCall([
            'external_call_id' => 'asterisk-linkedid-2004',
            'phone' => '+15550000004',
            'status' => 'waiting',
            'operator_search_attempts' => 1,
            'operator_search_max_attempts' => 3,
        ]);

        $this->handleJob($call);

        $this->assertDatabaseHas('calls', [
            'id' => $call->id,
            'operator_id' => $operator->id,
            'status' => 'assignment_requested',
            'operator_search_attempts' => 2,
            'next_operator_search_at' => null,
        ]);
        $this->assertDatabaseHas('telephony_outbox', [
            'type' => 'call_assignment_requested',
            'external_call_id' => 'asterisk-linkedid-2004',
            'idempotency_key' => 'asterisk-linkedid-2004:call_assignment_requested:2',
        ]);
        $this->assertSame([], $retryQueue->retries);
    }

    public function test_it_finishes_with_policy_status_when_operator_attempts_are_exhausted(): void
    {
        $retryQueue = new FakeCallProcessingRetryQueue;
        $metrics = new FakeProcessIncomingCallMetrics;
        $this->app->instance(CallProcessingRetryQueue::class, $retryQueue);
        $this->app->instance(Metrics::class, $metrics);

        $call = $this->createCall([
            'external_call_id' => 'asterisk-linkedid-2005',
            'phone' => '+15550000005',
            'operator_search_max_attempts' => 1,
            'operator_search_hangup_policy' => 'callback_missed',
        ]);

        $this->handleJob($call);

        $this->assertDatabaseHas('calls', [
            'id' => $call->id,
            'status' => 'callback_missed',
            'operator_search_attempts' => 1,
            'next_operator_search_at' => null,
        ]);
        $this->assertSame([], $retryQueue->retries);
        $this->assertContains(['operator_reservation_total', 1, ['result' => 'no_available']], $metrics->increments);
        $this->assertContains(['operator_reservation_attempts_total', 1, ['result' => 'no_available']], $metrics->increments);
        $this->assertContains(['call_transitions_total', 1, ['from' => 'new', 'to' => 'callback_missed']], $metrics->increments);
        $this->assertContains(['calls_finished_total', 1, ['result' => 'callback_missed']], $metrics->increments);
        $this->assertTimingRecorded('call_processing_duration_seconds', ['result' => 'exhausted'], $metrics);
        $this->assertDatabaseHas('telephony_outbox', [
            'type' => 'operator_search_exhausted',
            'external_call_id' => 'asterisk-linkedid-2005',
            'idempotency_key' => 'asterisk-linkedid-2005:operator_search_exhausted:1',
        ]);
    }

    /**
     * @param  array<string, mixed>  $attributes
     */
    private function createCall(array $attributes): Call
    {
        return Call::query()->create(array_merge([
            'external_call_id' => 'asterisk-linkedid-default',
            'phone' => '+15550000000',
            'kafka_message_id' => 'calls-0-default',
            'status' => 'new',
            'operator_search_attempts' => 0,
            'operator_search_max_attempts' => 1,
            'operator_search_retry_delay_seconds' => 0,
            'operator_search_hangup_policy' => 'missed',
        ], $attributes));
    }

    private function handleJob(Call $call): void
    {
        (new ProcessIncomingCallJob((int) $call->id))
            ->handle($this->app->make(ProcessIncomingCallHandler::class));
    }

    /**
     * @param  array<string, int|string>  $tags
     */
    private function assertTimingRecorded(string $name, array $tags, FakeProcessIncomingCallMetrics $metrics): void
    {
        foreach ($metrics->timings as $timing) {
            if ($timing[0] === $name && $timing[2] === $tags) {
                $this->addToAssertionCount(1);

                return;
            }
        }

        $this->fail(sprintf('Timing metric [%s] was not recorded.', $name));
    }
}

final class FakeCallProcessingRetryQueue implements CallProcessingRetryQueue
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

final class FakeProcessIncomingCallMetrics implements Metrics
{
    /**
     * @var list<array{0: string, 1: int, 2: array<string, int|string>}>
     */
    public array $increments = [];

    /**
     * @var list<array{0: string, 1: int|float, 2: array<string, int|string>}>
     */
    public array $timings = [];

    public function increment(string $name, int $value = 1, array $tags = []): void
    {
        $this->increments[] = [$name, $value, $tags];
    }

    public function gauge(string $name, int|float $value, array $tags = []): void {}

    public function timing(string $name, int|float $milliseconds, array $tags = []): void
    {
        $this->timings[] = [$name, $milliseconds, $tags];
    }
}
