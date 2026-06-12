<?php

declare(strict_types=1);

namespace Domain\Calls\Events;

use Domain\Shared\DomainEvent;
use Domain\Shared\Timestamp;

final readonly class IncomingCallRegistered implements DomainEvent
{
    private Timestamp $occurredAt;

    public function __construct(
        private int $callId,
        private string $externalCallId,
        private string $phone,
        private string $kafkaMessageId,
        ?Timestamp $occurredAt = null,
    ) {
        $this->occurredAt = $occurredAt ?? Timestamp::now();
    }

    public function eventId(): string
    {
        return sprintf('incoming-call:%s:registered', $this->externalCallId);
    }

    public function name(): string
    {
        return 'incoming_call.registered';
    }

    public function aggregateId(): string
    {
        return (string) $this->callId;
    }

    public function occurredAt(): Timestamp
    {
        return $this->occurredAt;
    }

    /**
     * @return array<string, mixed>
     */
    public function payload(): array
    {
        return [
            'call_id' => $this->callId,
            'external_call_id' => $this->externalCallId,
            'phone' => $this->phone,
            'kafka_message_id' => $this->kafkaMessageId,
        ];
    }
}
