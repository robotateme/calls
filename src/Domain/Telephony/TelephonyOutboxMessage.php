<?php

declare(strict_types=1);

namespace Domain\Telephony;

final readonly class TelephonyOutboxMessage
{
    /**
     * @param  array<string, mixed>  $payload
     */
    public function __construct(
        public int $id,
        public string $commandId,
        public string $idempotencyKey,
        public string $type,
        public string $externalCallId,
        public array $payload,
        public int $attempts,
    ) {}
}
