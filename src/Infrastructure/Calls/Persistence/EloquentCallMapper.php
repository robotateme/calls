<?php

declare(strict_types=1);

namespace Infrastructure\Calls\Persistence;

use App\Models\Call as CallRecord;
use DateTimeInterface;
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

final readonly class EloquentCallMapper
{
    public function toDomain(CallRecord $record): Call
    {
        return Call::restore(
            id: CallId::fromInt((int) $record->id),
            externalCallId: ExternalCallId::fromString((string) $record->external_call_id),
            phone: PhoneNumber::fromString((string) $record->phone),
            status: CallStatus::from((string) $record->status),
            clientId: $this->clientId($record->getRawOriginal('client_id')),
            operatorId: $this->operatorId($record->getRawOriginal('operator_id')),
            operatorSearchAttempts: OperatorSearchAttempts::fromInt((int) $record->getRawOriginal('operator_search_attempts')),
            operatorSearchMaxAttempts: OperatorSearchMaxAttempts::fromInt((int) $record->getRawOriginal('operator_search_max_attempts')),
            operatorSearchRetryDelay: OperatorSearchRetryDelay::fromSeconds((int) $record->getRawOriginal('operator_search_retry_delay_seconds')),
            operatorSearchHangupPolicy: CallHangupPolicy::from((string) $record->operator_search_hangup_policy),
            nextOperatorSearchAt: $this->timestamp($record->getRawOriginal('next_operator_search_at')),
            assignmentRequestedAt: $this->timestamp($record->getRawOriginal('assignment_requested_at')),
            operatorRingingAt: $this->timestamp($record->getRawOriginal('operator_ringing_at')),
            connectedAt: $this->timestamp($record->getRawOriginal('connected_at')),
        );
    }

    /**
     * @return array<string, mixed>
     */
    public function toDatabase(Call $call): array
    {
        return [
            'client_id' => $call->clientId(),
            'operator_id' => $call->operatorId(),
            'status' => $call->status()->value,
            'operator_search_attempts' => $call->operatorSearchAttempts(),
            'next_operator_search_at' => $call->nextOperatorSearchTimestamp()?->toDatabaseString(),
            'assignment_requested_at' => $call->assignmentRequestedTimestamp()?->toDatabaseString(),
            'operator_ringing_at' => $call->operatorRingingTimestamp()?->toDatabaseString(),
            'connected_at' => $call->connectedTimestamp()?->toDatabaseString(),
            'updated_at' => now(),
        ];
    }

    private function timestamp(mixed $value): ?Timestamp
    {
        if ($value === null || $value === '') {
            return null;
        }

        if ($value instanceof DateTimeInterface) {
            return Timestamp::fromDateTime($value);
        }

        return Timestamp::fromString((string) $value);
    }

    private function clientId(mixed $value): ?ClientId
    {
        return $value === null ? null : ClientId::fromInt((int) $value);
    }

    private function operatorId(mixed $value): ?OperatorId
    {
        return $value === null ? null : OperatorId::fromInt((int) $value);
    }
}
