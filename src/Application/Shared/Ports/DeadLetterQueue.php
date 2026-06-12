<?php

declare(strict_types=1);

namespace Application\Shared\Ports;

interface DeadLetterQueue
{
    /**
     * @param  array<string, mixed>|null  $decodedPayload
     */
    public function record(
        string $source,
        string $topic,
        ?int $partition,
        ?int $offset,
        ?string $messageKey,
        ?string $traceId,
        string $reason,
        string $rawPayload,
        ?array $decodedPayload = null,
    ): void;
}
