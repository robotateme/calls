<?php

declare(strict_types=1);

namespace Tests\Feature;

use App\Models\Call;
use App\Models\Operator;
use Application\Calls\Commands\MarkCallBridgeEstablishedFromKafkaCommand;
use Application\Calls\Commands\MarkCallBridgeEstablishedHandler;
use Application\Calls\Commands\MarkOperatorLegDroppedFromKafkaCommand;
use Application\Calls\Commands\MarkOperatorLegDroppedHandler;
use Application\Calls\Commands\MarkOperatorNoAnswerFromKafkaCommand;
use Application\Calls\Commands\MarkOperatorNoAnswerHandler;
use Application\Calls\Commands\MarkOperatorRingingFromKafkaCommand;
use Application\Calls\Commands\MarkOperatorRingingHandler;
use Application\Calls\Ports\CallProcessingRetryQueue;
use Illuminate\Foundation\Testing\RefreshDatabase;
use Tests\TestCase;

final class TelephonyEventsHandlerTest extends TestCase
{
    use RefreshDatabase;

    public function test_it_marks_operator_ringing(): void
    {
        $operator = $this->operator();
        $call = $this->createCall([
            'external_call_id' => 'asterisk-linkedid-4001',
            'status' => 'assignment_requested',
            'operator_id' => $operator->id,
            'operator_search_attempts' => 1,
        ]);

        $this->app->make(MarkOperatorRingingHandler::class)->handle(new MarkOperatorRingingFromKafkaCommand(
            externalCallId: 'asterisk-linkedid-4001',
            operatorId: (int) $operator->id,
            assignmentAttempt: 1,
            kafkaMessageId: 'telephony-0-4001',
        ));

        $this->assertDatabaseHas('calls', [
            'id' => $call->id,
            'status' => 'operator_ringing',
            'operator_id' => $operator->id,
        ]);
        $this->assertNotNull(Call::query()->findOrFail($call->id)->operator_ringing_at);
    }

    public function test_it_marks_bridge_established_as_connected(): void
    {
        $operator = $this->operator(available: true);
        $call = $this->createCall([
            'external_call_id' => 'asterisk-linkedid-4002',
            'status' => 'operator_ringing',
            'operator_id' => $operator->id,
            'operator_search_attempts' => 2,
        ]);
        $this->reserveOperator($operator, $call);

        $this->app->make(MarkCallBridgeEstablishedHandler::class)->handle(new MarkCallBridgeEstablishedFromKafkaCommand(
            externalCallId: 'asterisk-linkedid-4002',
            operatorId: (int) $operator->id,
            assignmentAttempt: 2,
            kafkaMessageId: 'telephony-0-4002',
        ));

        $this->assertDatabaseHas('calls', [
            'id' => $call->id,
            'status' => 'connected',
            'operator_id' => $operator->id,
        ]);
        $this->assertDatabaseHas('operators', [
            'id' => $operator->id,
            'available' => true,
            'reserved_call_id' => null,
            'reserved_at' => null,
        ]);
        $this->assertNotNull(Call::query()->findOrFail($call->id)->connected_at);
    }

    public function test_operator_no_answer_releases_operator_and_schedules_next_search_attempt(): void
    {
        $retryQueue = new FakeTelephonyEventRetryQueue;
        $this->app->instance(CallProcessingRetryQueue::class, $retryQueue);

        $operator = $this->operator(available: true);
        $call = $this->createCall([
            'external_call_id' => 'asterisk-linkedid-4003',
            'status' => 'operator_ringing',
            'operator_id' => $operator->id,
            'operator_search_attempts' => 1,
            'operator_search_max_attempts' => 3,
            'operator_search_retry_delay_seconds' => 20,
        ]);
        $this->reserveOperator($operator, $call);

        $this->app->make(MarkOperatorNoAnswerHandler::class)->handle(new MarkOperatorNoAnswerFromKafkaCommand(
            externalCallId: 'asterisk-linkedid-4003',
            operatorId: (int) $operator->id,
            assignmentAttempt: 1,
            kafkaMessageId: 'telephony-0-4003',
        ));

        $this->assertDatabaseHas('calls', [
            'id' => $call->id,
            'status' => 'waiting',
            'operator_id' => null,
            'operator_search_attempts' => 1,
        ]);
        $this->assertDatabaseHas('operators', [
            'id' => $operator->id,
            'available' => true,
            'reserved_call_id' => null,
        ]);
        $this->assertDatabaseHas('telephony_outbox', [
            'type' => 'operator_search_retry_scheduled',
            'external_call_id' => 'asterisk-linkedid-4003',
            'idempotency_key' => 'asterisk-linkedid-4003:operator_search_retry_scheduled:1',
        ]);
        $this->assertSame([[$call->id, 20]], $retryQueue->retries);
    }

    public function test_operator_leg_dropped_before_bridge_finishes_by_policy_when_attempts_are_exhausted(): void
    {
        $retryQueue = new FakeTelephonyEventRetryQueue;
        $this->app->instance(CallProcessingRetryQueue::class, $retryQueue);

        $operator = $this->operator(available: true);
        $call = $this->createCall([
            'external_call_id' => 'asterisk-linkedid-4004',
            'status' => 'operator_ringing',
            'operator_id' => $operator->id,
            'operator_search_attempts' => 1,
            'operator_search_max_attempts' => 1,
            'operator_search_hangup_policy' => 'callback_missed',
        ]);
        $this->reserveOperator($operator, $call);

        $this->app->make(MarkOperatorLegDroppedHandler::class)->handle(new MarkOperatorLegDroppedFromKafkaCommand(
            externalCallId: 'asterisk-linkedid-4004',
            operatorId: (int) $operator->id,
            assignmentAttempt: 1,
            kafkaMessageId: 'telephony-0-4004',
        ));

        $this->assertDatabaseHas('calls', [
            'id' => $call->id,
            'status' => 'callback_missed',
            'operator_id' => null,
        ]);
        $this->assertDatabaseHas('operators', [
            'id' => $operator->id,
            'available' => true,
            'reserved_call_id' => null,
        ]);
        $this->assertDatabaseHas('telephony_outbox', [
            'type' => 'operator_search_exhausted',
            'external_call_id' => 'asterisk-linkedid-4004',
            'idempotency_key' => 'asterisk-linkedid-4004:operator_search_exhausted:1',
        ]);
        $this->assertSame([], $retryQueue->retries);
    }

    public function test_operator_leg_dropped_after_bridge_does_not_change_connected_call(): void
    {
        $retryQueue = new FakeTelephonyEventRetryQueue;
        $this->app->instance(CallProcessingRetryQueue::class, $retryQueue);

        $operator = $this->operator(available: true);
        $call = $this->createCall([
            'external_call_id' => 'asterisk-linkedid-4005',
            'status' => 'connected',
            'operator_id' => $operator->id,
            'operator_search_attempts' => 1,
            'operator_search_max_attempts' => 3,
        ]);

        $this->app->make(MarkOperatorLegDroppedHandler::class)->handle(new MarkOperatorLegDroppedFromKafkaCommand(
            externalCallId: 'asterisk-linkedid-4005',
            operatorId: (int) $operator->id,
            assignmentAttempt: 1,
            kafkaMessageId: 'telephony-0-4005',
        ));

        $this->assertDatabaseHas('calls', [
            'id' => $call->id,
            'status' => 'connected',
            'operator_id' => $operator->id,
        ]);
        $this->assertDatabaseHas('operators', [
            'id' => $operator->id,
            'available' => true,
            'reserved_call_id' => null,
        ]);
        $this->assertDatabaseCount('telephony_outbox', 0);
        $this->assertSame([], $retryQueue->retries);
    }

    private function operator(bool $available = false): Operator
    {
        return Operator::query()->create([
            'name' => 'Operator 1',
            'available' => $available,
            'afk' => false,
        ]);
    }

    private function reserveOperator(Operator $operator, Call $call): void
    {
        $operator->forceFill([
            'reserved_call_id' => $call->id,
            'reserved_at' => now(),
        ])->save();
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
}

final class FakeTelephonyEventRetryQueue implements CallProcessingRetryQueue
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
