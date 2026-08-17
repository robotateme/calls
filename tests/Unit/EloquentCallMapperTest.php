<?php

declare(strict_types=1);

namespace Tests\Unit;

use App\Models\Call as CallRecord;
use Domain\Calls\Call;
use Domain\Calls\CallHangupPolicy;
use Domain\Calls\CallId;
use Domain\Calls\CallStatus;
use Domain\Calls\ExternalCallId;
use Domain\Calls\OperatorSearchAttempts;
use Domain\Calls\OperatorSearchMaxAttempts;
use Domain\Calls\OperatorSearchRetryDelay;
use Domain\Calls\PhoneNumber;
use Domain\Clients\ClientId;
use Domain\Operators\OperatorId;
use Domain\Shared\Timestamp;
use Infrastructure\Calls\Persistence\EloquentCallMapper;
use Tests\TestCase;
use UnexpectedValueException;

final class EloquentCallMapperTest extends TestCase
{
    public function test_it_restores_call_from_persistence_record(): void
    {
        $call = $this->mapper()->toDomain($this->callRecord([
            'id' => 15,
            'external_call_id' => 'asterisk-linkedid-1500',
            'phone' => '+15550001500',
            'client_id' => 7,
            'operator_id' => 9,
            'status' => 'operator_dialing',
            'operator_search_attempts' => 2,
            'operator_search_max_attempts' => 5,
            'operator_search_retry_delay_seconds' => 30,
            'operator_search_hangup_policy' => 'callback_missed',
            'next_operator_search_at' => null,
            'assignment_requested_at' => '2026-06-11 10:00:00',
            'operator_dialing_at' => '2026-06-11 10:00:05',
            'connected_at' => null,
            'created_at' => '2026-06-11 09:59:00',
        ]));

        $this->assertSame(15, $call->id());
        $this->assertSame('asterisk-linkedid-1500', $call->externalCallId());
        $this->assertSame('+15550001500', $call->phone());
        $this->assertSame(CallStatus::OperatorDialing, $call->status());
        $this->assertSame(7, $call->clientId());
        $this->assertSame(9, $call->operatorId());
        $this->assertSame(2, $call->operatorSearchAttempts());
        $this->assertSame(5, $call->operatorSearchMaxAttempts());
        $this->assertSame(30, $call->operatorSearchRetryDelaySeconds());
        $this->assertSame(CallHangupPolicy::CallbackMissed, $call->operatorSearchHangupPolicy());
        $this->assertSame('2026-06-11 09:59:00', $call->createdTimestamp()->toDatabaseString());
    }

    public function test_it_maps_call_to_update_data_without_generating_updated_at(): void
    {
        $data = $this->mapper()->toUpdateData($this->domainCall());

        $this->assertSame([
            'client_id' => 8,
            'operator_id' => 10,
            'status' => 'assignment_requested',
            'operator_search_attempts' => 1,
            'next_operator_search_at' => null,
            'assignment_requested_at' => '2026-06-11 10:01:00',
            'operator_dialing_at' => null,
            'connected_at' => null,
        ], $data);
        $this->assertArrayNotHasKey('updated_at', $data);
    }

    public function test_it_maps_incoming_call_values_to_insert_data(): void
    {
        $data = $this->mapper()->toIncomingInsertData(
            externalCallId: ExternalCallId::fromString('asterisk-linkedid-1600'),
            phone: PhoneNumber::fromString('+15550001600'),
            kafkaMessageId: 'incoming-calls-0-1600',
            operatorSearchMaxAttempts: OperatorSearchMaxAttempts::fromInt(4),
            operatorSearchRetryDelay: OperatorSearchRetryDelay::fromSeconds(20),
            operatorSearchHangupPolicy: CallHangupPolicy::HangupOnRetry,
        );

        $this->assertSame([
            'external_call_id' => 'asterisk-linkedid-1600',
            'phone' => '+15550001600',
            'kafka_message_id' => 'incoming-calls-0-1600',
            'status' => 'new',
            'operator_search_max_attempts' => 4,
            'operator_search_retry_delay_seconds' => 20,
            'operator_search_hangup_policy' => 'hangup_on_retry',
        ], $data);
    }

    public function test_it_fails_when_persisted_call_has_no_created_at(): void
    {
        $this->expectException(UnexpectedValueException::class);
        $this->expectExceptionMessage('Persisted Call has no created_at timestamp.');

        $this->mapper()->toDomain($this->callRecord([
            'id' => 15,
            'external_call_id' => 'asterisk-linkedid-1500',
            'phone' => '+15550001500',
            'client_id' => null,
            'operator_id' => null,
            'status' => 'new',
            'operator_search_attempts' => 0,
            'operator_search_max_attempts' => 1,
            'operator_search_retry_delay_seconds' => 0,
            'operator_search_hangup_policy' => 'missed',
            'next_operator_search_at' => null,
            'assignment_requested_at' => null,
            'operator_dialing_at' => null,
            'connected_at' => null,
            'created_at' => null,
        ]));
    }

    private function mapper(): EloquentCallMapper
    {
        return new EloquentCallMapper;
    }

    /**
     * @param  array<string, mixed>  $attributes
     */
    private function callRecord(array $attributes): CallRecord
    {
        $record = new CallRecord;
        $record->setRawAttributes($attributes, sync: true);

        return $record;
    }

    private function domainCall(): Call
    {
        return Call::restore(
            id: CallId::fromInt(25),
            externalCallId: ExternalCallId::fromString('asterisk-linkedid-2500'),
            phone: PhoneNumber::fromString('+15550002500'),
            status: CallStatus::AssignmentRequested,
            clientId: ClientId::fromInt(8),
            operatorId: OperatorId::fromInt(10),
            operatorSearchAttempts: OperatorSearchAttempts::fromInt(1),
            operatorSearchMaxAttempts: OperatorSearchMaxAttempts::fromInt(3),
            operatorSearchRetryDelay: OperatorSearchRetryDelay::fromSeconds(15),
            operatorSearchHangupPolicy: CallHangupPolicy::Missed,
            nextOperatorSearchAt: null,
            assignmentRequestedAt: Timestamp::fromString('2026-06-11 10:01:00'),
            operatorDialingAt: null,
            connectedAt: null,
            createdAt: Timestamp::fromString('2026-06-11 10:00:00'),
        );
    }
}
