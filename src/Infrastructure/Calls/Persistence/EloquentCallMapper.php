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
use Exception;
use InvalidArgumentException;
use UnexpectedValueException;
use ValueError;

final readonly class EloquentCallMapper
{
    /**
     * @throws Exception
     * @throws InvalidArgumentException
     * @throws UnexpectedValueException
     * @throws ValueError
     */
    public function toDomain(CallRecord $record): Call
    {
        return Call::restore(
            id: CallId::fromInt((int) $this->raw($record, 'id')),
            externalCallId: ExternalCallId::fromString((string) $this->raw($record, 'external_call_id')),
            phone: PhoneNumber::fromString((string) $this->raw($record, 'phone')),
            status: CallStatus::from((string) $this->raw($record, 'status')),
            clientId: $this->clientId($this->raw($record, 'client_id')),
            operatorId: $this->operatorId($this->raw($record, 'operator_id')),
            operatorSearchAttempts: OperatorSearchAttempts::fromInt((int) $this->raw($record, 'operator_search_attempts')),
            operatorSearchMaxAttempts: OperatorSearchMaxAttempts::fromInt((int) $this->raw($record, 'operator_search_max_attempts')),
            operatorSearchRetryDelay: OperatorSearchRetryDelay::fromSeconds((int) $this->raw($record, 'operator_search_retry_delay_seconds')),
            operatorSearchHangupPolicy: CallHangupPolicy::from((string) $this->raw($record, 'operator_search_hangup_policy')),
            nextOperatorSearchAt: $this->timestamp($this->raw($record, 'next_operator_search_at')),
            assignmentRequestedAt: $this->timestamp($this->raw($record, 'assignment_requested_at')),
            operatorDialingAt: $this->timestamp($this->raw($record, 'operator_dialing_at')),
            connectedAt: $this->timestamp($this->raw($record, 'connected_at')),
            createdAt: $this->requiredTimestamp($this->raw($record, 'created_at')),
        );
    }

    /**
     * @return array<string, mixed>
     */
    public function toUpdateData(Call $call): array
    {
        return [
            'client_id' => $call->clientId(),
            'operator_id' => $call->operatorId(),
            'status' => $call->status()->value,
            'operator_search_attempts' => $call->operatorSearchAttempts(),
            'next_operator_search_at' => $call->nextOperatorSearchTimestamp()?->toDatabaseString(),
            'assignment_requested_at' => $call->assignmentRequestedTimestamp()?->toDatabaseString(),
            'operator_dialing_at' => $call->operatorDialingTimestamp()?->toDatabaseString(),
            'connected_at' => $call->connectedTimestamp()?->toDatabaseString(),
        ];
    }

    /**
     * @return array<string, mixed>
     */
    public function toIncomingInsertData(
        ExternalCallId $externalCallId,
        PhoneNumber $phone,
        string $kafkaMessageId,
        OperatorSearchMaxAttempts $operatorSearchMaxAttempts,
        OperatorSearchRetryDelay $operatorSearchRetryDelay,
        CallHangupPolicy $operatorSearchHangupPolicy,
    ): array {
        return [
            'external_call_id' => $externalCallId->toString(),
            'phone' => $phone->toString(),
            'kafka_message_id' => $kafkaMessageId,
            'status' => CallStatus::New->value,
            'operator_search_max_attempts' => $operatorSearchMaxAttempts->toInt(),
            'operator_search_retry_delay_seconds' => $operatorSearchRetryDelay->seconds(),
            'operator_search_hangup_policy' => $operatorSearchHangupPolicy->value,
        ];
    }

    /**
     * @throws Exception
     */
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

    /**
     * @throws Exception
     * @throws UnexpectedValueException
     */
    private function requiredTimestamp(mixed $value): Timestamp
    {
        $timestamp = $this->timestamp($value);

        if ($timestamp === null) {
            throw new UnexpectedValueException('Persisted Call has no created_at timestamp.');
        }

        return $timestamp;
    }

    /**
     * @throws InvalidArgumentException
     */
    private function clientId(mixed $value): ?ClientId
    {
        return $value === null ? null : ClientId::fromInt((int) $value);
    }

    /**
     * @throws InvalidArgumentException
     */
    private function operatorId(mixed $value): ?OperatorId
    {
        return $value === null ? null : OperatorId::fromInt((int) $value);
    }

    private function raw(CallRecord $record, string $column): mixed
    {
        // Mapper restores persisted primitives instead of depending on Eloquent casts.
        return $record->getRawOriginal($column);
    }
}
