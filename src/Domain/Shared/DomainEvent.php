<?php

declare(strict_types=1);

namespace Domain\Shared;

interface DomainEvent
{
    public function eventId(): string;

    public function name(): string;

    public function aggregateId(): string;

    public function occurredAt(): Timestamp;

    /**
     * @return array<string, mixed>
     */
    public function payload(): array;
}
