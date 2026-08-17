<?php

declare(strict_types=1);

namespace Tests\Unit;

use Infrastructure\Telephony\Outbox\EloquentTelephonyOutboxMapper;
use Tests\TestCase;

final class EloquentTelephonyOutboxMapperTest extends TestCase
{
    public function test_it_maps_database_record_to_outbox_message_without_attempt_offset(): void
    {
        $message = (new EloquentTelephonyOutboxMapper)->toDomain([
            'id' => 44,
            'command_id' => '3c3aa57c-5029-4759-b30a-951dcf0f7edb',
            'idempotency_key' => 'asterisk-linkedid-4400:call_assignment_requested:2',
            'type' => 'call_assignment_requested',
            'external_call_id' => 'asterisk-linkedid-4400',
            'payload' => '{"external_call_id":"asterisk-linkedid-4400","operator_id":12}',
            'attempts' => 6,
        ]);

        $this->assertSame(44, $message->id);
        $this->assertSame('3c3aa57c-5029-4759-b30a-951dcf0f7edb', $message->commandId);
        $this->assertSame('asterisk-linkedid-4400:call_assignment_requested:2', $message->idempotencyKey);
        $this->assertSame('call_assignment_requested', $message->type);
        $this->assertSame('asterisk-linkedid-4400', $message->externalCallId);
        $this->assertSame([
            'external_call_id' => 'asterisk-linkedid-4400',
            'operator_id' => 12,
        ], $message->payload);
        $this->assertSame(6, $message->attempts);
    }
}
