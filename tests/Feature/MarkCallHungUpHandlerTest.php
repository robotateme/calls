<?php

declare(strict_types=1);

namespace Tests\Feature;

use App\Models\Call;
use App\Models\Operator;
use Application\Calls\Commands\MarkCallHungUpFromKafkaCommand;
use Application\Calls\Commands\MarkCallHungUpHandler;
use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Str;
use Tests\TestCase;

final class MarkCallHungUpHandlerTest extends TestCase
{
    use RefreshDatabase;

    public function test_it_marks_new_call_missed_by_policy(): void
    {
        $call = $this->createCall([
            'external_call_id' => 'asterisk-linkedid-3001',
            'status' => 'new',
            'operator_search_hangup_policy' => 'missed',
        ]);

        $this->handler()->handle(new MarkCallHungUpFromKafkaCommand(
            externalCallId: 'asterisk-linkedid-3001',
            kafkaMessageId: 'hangups-0-3001',
        ));

        $this->assertDatabaseHas('calls', [
            'id' => $call->id,
            'status' => 'missed',
            'next_operator_search_at' => null,
        ]);
    }

    public function test_it_marks_waiting_call_callback_missed_by_policy(): void
    {
        $call = $this->createCall([
            'external_call_id' => 'asterisk-linkedid-3002',
            'status' => 'waiting',
            'operator_search_hangup_policy' => 'callback_missed',
            'next_operator_search_at' => now()->addSeconds(10),
        ]);

        $this->handler()->handle(new MarkCallHungUpFromKafkaCommand(
            externalCallId: 'asterisk-linkedid-3002',
            kafkaMessageId: 'hangups-0-3002',
        ));

        $this->assertDatabaseHas('calls', [
            'id' => $call->id,
            'status' => 'callback_missed',
            'next_operator_search_at' => null,
        ]);
    }

    public function test_it_does_not_change_connected_call(): void
    {
        $operator = Operator::query()->create([
            'name' => 'Operator 1',
            'available' => true,
        ]);
        $call = $this->createCall([
            'external_call_id' => 'asterisk-linkedid-3003',
            'status' => 'connected',
            'operator_id' => $operator->id,
            'operator_search_hangup_policy' => 'hangup_on_retry',
        ]);

        $this->handler()->handle(new MarkCallHungUpFromKafkaCommand(
            externalCallId: 'asterisk-linkedid-3003',
            kafkaMessageId: 'hangups-0-3003',
        ));

        $this->assertDatabaseHas('calls', [
            'id' => $call->id,
            'status' => 'connected',
            'operator_id' => $operator->id,
        ]);
    }

    public function test_it_marks_assignment_requested_call_missed_and_releases_operator(): void
    {
        $operator = Operator::query()->create([
            'name' => 'Operator 1',
            'available' => true,
        ]);
        $call = $this->createCall([
            'external_call_id' => 'asterisk-linkedid-3004',
            'status' => 'assignment_requested',
            'operator_id' => $operator->id,
            'operator_search_attempts' => 1,
            'operator_search_max_attempts' => 3,
            'operator_search_hangup_policy' => 'missed',
        ]);
        $this->reserveOperator($operator, $call);

        $this->handler()->handle(new MarkCallHungUpFromKafkaCommand(
            externalCallId: 'asterisk-linkedid-3004',
            kafkaMessageId: 'hangups-0-3004',
        ));

        $this->assertDatabaseHas('calls', [
            'id' => $call->id,
            'status' => 'missed',
            'operator_id' => null,
        ]);
        $this->assertDatabaseHas('operators', [
            'id' => $operator->id,
            'available' => true,
            'reserved_call_id' => null,
        ]);
    }

    public function test_it_cancels_pending_assignment_request_when_call_hangs_up(): void
    {
        $operator = Operator::query()->create([
            'name' => 'Operator 1',
            'available' => true,
        ]);
        $call = $this->createCall([
            'external_call_id' => 'asterisk-linkedid-3005',
            'status' => 'assignment_requested',
            'operator_id' => $operator->id,
            'operator_search_attempts' => 1,
            'operator_search_max_attempts' => 3,
            'operator_search_hangup_policy' => 'missed',
        ]);
        $this->reserveOperator($operator, $call);
        $outboxId = $this->insertOutbox([
            'external_call_id' => 'asterisk-linkedid-3005',
            'type' => 'call_assignment_requested',
            'idempotency_key' => 'asterisk-linkedid-3005:call_assignment_requested:1',
            'status' => 'pending',
        ]);

        $this->handler()->handle(new MarkCallHungUpFromKafkaCommand(
            externalCallId: 'asterisk-linkedid-3005',
            kafkaMessageId: 'hangups-0-3005',
        ));

        $this->assertDatabaseHas('telephony_outbox', [
            'id' => $outboxId,
            'status' => 'pending',
            'cancel_reason' => 'call_hung_up',
        ]);
        $this->assertNotNull(DB::table('telephony_outbox')->where('id', $outboxId)->value('canceled_at'));
        $this->assertDatabaseMissing('telephony_outbox', [
            'external_call_id' => 'asterisk-linkedid-3005',
            'type' => 'call_assignment_canceled',
        ]);
    }

    public function test_it_records_assignment_canceled_when_published_assignment_request_exists(): void
    {
        $operator = Operator::query()->create([
            'name' => 'Operator 1',
            'available' => true,
        ]);
        $call = $this->createCall([
            'external_call_id' => 'asterisk-linkedid-3006',
            'status' => 'operator_ringing',
            'operator_id' => $operator->id,
            'operator_search_attempts' => 2,
            'operator_search_max_attempts' => 3,
            'operator_search_hangup_policy' => 'callback_missed',
        ]);
        $this->reserveOperator($operator, $call);
        $this->insertOutbox([
            'external_call_id' => 'asterisk-linkedid-3006',
            'type' => 'call_assignment_requested',
            'idempotency_key' => 'asterisk-linkedid-3006:call_assignment_requested:2',
            'status' => 'published',
            'published_at' => now(),
        ]);

        $this->handler()->handle(new MarkCallHungUpFromKafkaCommand(
            externalCallId: 'asterisk-linkedid-3006',
            kafkaMessageId: 'hangups-0-3006',
        ));

        $this->assertDatabaseHas('calls', [
            'external_call_id' => 'asterisk-linkedid-3006',
            'status' => 'callback_missed',
        ]);
        $this->assertDatabaseHas('telephony_outbox', [
            'external_call_id' => 'asterisk-linkedid-3006',
            'type' => 'call_assignment_canceled',
            'idempotency_key' => 'asterisk-linkedid-3006:call_assignment_canceled:2',
            'status' => 'pending',
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

    private function handler(): MarkCallHungUpHandler
    {
        return $this->app->make(MarkCallHungUpHandler::class);
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
    private function insertOutbox(array $attributes): int
    {
        return (int) DB::table('telephony_outbox')->insertGetId(array_merge([
            'command_id' => (string) Str::uuid(),
            'idempotency_key' => 'asterisk-linkedid-default:call_assignment_requested:1',
            'type' => 'call_assignment_requested',
            'external_call_id' => 'asterisk-linkedid-default',
            'payload' => json_encode([
                'external_call_id' => 'asterisk-linkedid-default',
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
                'external_call_id' => $attributes['external_call_id'] ?? 'asterisk-linkedid-default',
            ], JSON_THROW_ON_ERROR),
        ]));
    }
}
