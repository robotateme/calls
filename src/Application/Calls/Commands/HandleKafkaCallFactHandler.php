<?php

declare(strict_types=1);

namespace Application\Calls\Commands;

use Application\Shared\Ports\DeadLetterQueue;
use Application\Shared\Ports\Metrics;
use InvalidArgumentException;
use JsonException;
use Throwable;

final readonly class HandleKafkaCallFactHandler
{
    public function __construct(
        private RegisterIncomingCallHandler $registerIncomingCall,
        private MarkOperatorRingingHandler $markOperatorRinging,
        private MarkCallBridgeEstablishedHandler $markBridgeEstablished,
        private MarkOperatorNoAnswerHandler $markOperatorNoAnswer,
        private MarkOperatorLegDroppedHandler $markOperatorLegDropped,
        private MarkCallHungUpHandler $markCallHungUp,
        private DeadLetterQueue $deadLetters,
        private Metrics $metrics,
    ) {}

    public function handle(HandleKafkaCallFactCommand $command): void
    {
        $startedAt = microtime(true);
        $this->metrics->increment('kafka_consumer.message_received', tags: [
            'source' => $command->source,
            'topic' => $command->topic,
        ]);

        try {
            $decoded = $this->decode($command);
            $type = $this->messageType($command, $decoded);
            $schemaVersion = $this->schemaVersion($command, $decoded);
            $payload = $this->payload($decoded);

            $this->assertMessageKey($command, $payload);
            $this->dispatch($type, $payload, $command);

            $this->metrics->increment('kafka_consumer.message_handled', tags: [
                'source' => $command->source,
                'topic' => $command->topic,
                'type' => $type,
                'schema_version' => (string) $schemaVersion,
            ]);
        } catch (InvalidKafkaCallFact $exception) {
            $this->sendToDlq($command, $exception->reason, $exception->decodedPayload);
        } catch (Throwable $exception) {
            $this->sendToDlq($command, 'handler_failed', null);

            $this->metrics->increment('kafka_consumer.message_failed', tags: [
                'source' => $command->source,
                'topic' => $command->topic,
                'reason' => 'handler_failed',
            ]);
        } finally {
            $this->metrics->timing('kafka_consumer.message_duration_ms', (microtime(true) - $startedAt) * 1000, [
                'source' => $command->source,
                'topic' => $command->topic,
            ]);
        }
    }

    /**
     * @return array<string, mixed>
     *
     * @throws InvalidKafkaCallFact
     */
    private function decode(HandleKafkaCallFactCommand $command): array
    {
        try {
            $decoded = json_decode($command->rawPayload, true, flags: JSON_THROW_ON_ERROR);
        } catch (JsonException) {
            throw new InvalidKafkaCallFact('invalid_json');
        }

        if (! is_array($decoded)) {
            throw new InvalidKafkaCallFact('invalid_payload');
        }

        /** @var array<string, mixed> $decoded */
        return $decoded;
    }

    /**
     * @param  array<string, mixed>  $decoded
     *
     * @throws InvalidKafkaCallFact
     */
    private function messageType(HandleKafkaCallFactCommand $command, array $decoded): string
    {
        $type = $decoded['type'] ?? null;

        if ($type === null && $command->topic === 'incoming-calls') {
            return 'incoming_call_registered';
        }

        if (! is_string($type) || trim($type) === '') {
            throw new InvalidKafkaCallFact('unknown_type', $decoded);
        }

        return trim($type);
    }

    /**
     * @param  array<string, mixed>  $decoded
     * @return array<string, mixed>
     *
     * @throws InvalidKafkaCallFact
     */
    private function payload(array $decoded): array
    {
        $payload = $decoded['payload'] ?? $decoded;

        if (! is_array($payload)) {
            throw new InvalidKafkaCallFact('invalid_payload', $decoded);
        }

        /** @var array<string, mixed> $payload */
        return $payload;
    }

    /**
     * @param  array<string, mixed>  $decoded
     *
     * @throws InvalidKafkaCallFact
     */
    private function schemaVersion(HandleKafkaCallFactCommand $command, array $decoded): int
    {
        $version = $decoded['schema_version'] ?? null;

        if ($version === null && $command->topic === 'incoming-calls') {
            $version = 1;
        }

        if (is_string($version) && ctype_digit($version)) {
            $version = (int) $version;
        }

        if ($version !== 1) {
            throw new InvalidKafkaCallFact('unsupported_schema_version', $decoded);
        }

        return $version;
    }

    /**
     * @param  array<string, mixed>  $payload
     *
     * @throws InvalidKafkaCallFact
     */
    private function assertMessageKey(HandleKafkaCallFactCommand $command, array $payload): void
    {
        $externalCallId = $payload['external_call_id'] ?? null;

        if (! is_string($externalCallId) || trim($externalCallId) === '') {
            throw new InvalidKafkaCallFact('missing_external_call_id', $payload);
        }

        if ($command->messageKey !== null && trim($command->messageKey) !== trim($externalCallId)) {
            throw new InvalidKafkaCallFact('contract_violation', $payload);
        }
    }

    /**
     * @param  array<string, mixed>  $payload
     *
     * @throws InvalidKafkaCallFact
     */
    private function dispatch(string $type, array $payload, HandleKafkaCallFactCommand $sourceCommand): void
    {
        match ($type) {
            'incoming_call_registered' => $this->registerIncomingCall->handle(new RegisterIncomingCallFromKafkaCommand(
                externalCallId: $this->string($payload, 'external_call_id'),
                phone: $this->string($payload, 'phone'),
                kafkaMessageId: $this->kafkaMessageId($payload, $sourceCommand),
                operatorSearchMaxAttempts: $this->optionalInt($payload, 'operator_search_max_attempts', 1),
                operatorSearchRetryDelaySeconds: $this->optionalInt($payload, 'operator_search_retry_delay_seconds', 0),
                operatorSearchHangupPolicy: $this->optionalString($payload, 'operator_search_hangup_policy', 'missed'),
            )),
            'operator_ringing' => $this->markOperatorRinging->handle(new MarkOperatorRingingFromKafkaCommand(
                externalCallId: $this->string($payload, 'external_call_id'),
                operatorId: $this->positiveInt($payload, 'operator_id'),
                assignmentAttempt: $this->positiveInt($payload, 'assignment_attempt'),
                kafkaMessageId: $this->kafkaMessageId($payload, $sourceCommand),
            )),
            'bridge_established' => $this->markBridgeEstablished->handle(new MarkCallBridgeEstablishedFromKafkaCommand(
                externalCallId: $this->string($payload, 'external_call_id'),
                operatorId: $this->positiveInt($payload, 'operator_id'),
                assignmentAttempt: $this->positiveInt($payload, 'assignment_attempt'),
                kafkaMessageId: $this->kafkaMessageId($payload, $sourceCommand),
            )),
            'operator_no_answer' => $this->markOperatorNoAnswer->handle(new MarkOperatorNoAnswerFromKafkaCommand(
                externalCallId: $this->string($payload, 'external_call_id'),
                operatorId: $this->positiveInt($payload, 'operator_id'),
                assignmentAttempt: $this->positiveInt($payload, 'assignment_attempt'),
                kafkaMessageId: $this->kafkaMessageId($payload, $sourceCommand),
            )),
            'operator_leg_dropped' => $this->markOperatorLegDropped->handle(new MarkOperatorLegDroppedFromKafkaCommand(
                externalCallId: $this->string($payload, 'external_call_id'),
                operatorId: $this->positiveInt($payload, 'operator_id'),
                assignmentAttempt: $this->positiveInt($payload, 'assignment_attempt'),
                kafkaMessageId: $this->kafkaMessageId($payload, $sourceCommand),
            )),
            'hangup' => $this->markCallHungUp->handle(new MarkCallHungUpFromKafkaCommand(
                externalCallId: $this->string($payload, 'external_call_id'),
                kafkaMessageId: $this->kafkaMessageId($payload, $sourceCommand),
            )),
            default => throw new InvalidKafkaCallFact('unknown_type', $payload),
        };
    }

    /**
     * @param  array<string, mixed>  $payload
     *
     * @throws InvalidKafkaCallFact
     */
    private function string(array $payload, string $field): string
    {
        $value = $payload[$field] ?? null;

        if (! is_string($value) || trim($value) === '') {
            throw new InvalidKafkaCallFact('invalid_payload', $payload);
        }

        return trim($value);
    }

    /**
     * @param  array<string, mixed>  $payload
     *
     * @throws InvalidKafkaCallFact
     */
    private function optionalString(array $payload, string $field, string $default): string
    {
        if (! array_key_exists($field, $payload)) {
            return $default;
        }

        return $this->string($payload, $field);
    }

    /**
     * @param  array<string, mixed>  $payload
     *
     * @throws InvalidKafkaCallFact
     */
    private function positiveInt(array $payload, string $field): int
    {
        $value = $this->optionalInt($payload, $field, 0);

        if ($value < 1) {
            throw new InvalidKafkaCallFact('invalid_payload', $payload);
        }

        return $value;
    }

    /**
     * @param  array<string, mixed>  $payload
     *
     * @throws InvalidKafkaCallFact
     */
    private function optionalInt(array $payload, string $field, int $default): int
    {
        if (! array_key_exists($field, $payload)) {
            return $default;
        }

        $value = $payload[$field];

        if (is_int($value)) {
            return $value;
        }

        if (is_string($value) && ctype_digit($value)) {
            return (int) $value;
        }

        throw new InvalidKafkaCallFact('invalid_payload', $payload);
    }

    /**
     * @param  array<string, mixed>  $payload
     */
    private function kafkaMessageId(array $payload, HandleKafkaCallFactCommand $command): string
    {
        $value = $payload['kafka_message_id'] ?? null;

        if (is_string($value) && trim($value) !== '') {
            return trim($value);
        }

        return sprintf(
            '%s-%s-%s',
            $command->topic,
            $command->partition === null ? 'unknown' : (string) $command->partition,
            $command->offset === null ? substr(hash('sha256', $command->rawPayload), 0, 12) : (string) $command->offset,
        );
    }

    /**
     * @param  array<string, mixed>|null  $decodedPayload
     */
    private function sendToDlq(HandleKafkaCallFactCommand $command, string $reason, ?array $decodedPayload): void
    {
        $this->deadLetters->record(
            source: $command->source,
            topic: $command->topic,
            partition: $command->partition,
            offset: $command->offset,
            messageKey: $command->messageKey,
            traceId: $command->traceId,
            reason: $reason,
            rawPayload: $command->rawPayload,
            decodedPayload: $decodedPayload,
        );

        $this->metrics->increment('kafka_consumer.message_dlq', tags: [
            'source' => $command->source,
            'topic' => $command->topic,
            'reason' => $reason,
        ]);
    }
}

final class InvalidKafkaCallFact extends InvalidArgumentException
{
    /**
     * @param  array<string, mixed>|null  $decodedPayload
     */
    public function __construct(
        public readonly string $reason,
        public readonly ?array $decodedPayload = null,
    ) {
        parent::__construct($reason);
    }
}
