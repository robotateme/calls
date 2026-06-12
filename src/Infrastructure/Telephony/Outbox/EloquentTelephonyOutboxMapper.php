<?php

declare(strict_types=1);

namespace Infrastructure\Telephony\Outbox;

use Domain\Telephony\TelephonyOutboxMessage;

final readonly class EloquentTelephonyOutboxMapper
{
    /**
     * @param  array<string, mixed>  $record
     */
    public function toDomain(array $record, int $attemptOffset = 1): TelephonyOutboxMessage
    {
        $payload = json_decode((string) $record['payload'], true, flags: JSON_THROW_ON_ERROR);

        return new TelephonyOutboxMessage(
            id: (int) $record['id'],
            commandId: (string) $record['command_id'],
            idempotencyKey: (string) $record['idempotency_key'],
            type: (string) $record['type'],
            externalCallId: (string) $record['external_call_id'],
            payload: is_array($payload) ? $payload : [],
            attempts: ((int) $record['attempts']) + $attemptOffset,
        );
    }
}
