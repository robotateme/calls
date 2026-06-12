<?php

declare(strict_types=1);

namespace Tests\Unit;

use Domain\Telephony\TelephonyOutboxMessage;
use Infrastructure\Shared\Kafka\RdkafkaKafkaConsumer;
use Infrastructure\Shared\Kafka\RdkafkaRuntime;
use Infrastructure\Telephony\Outbox\RdkafkaTelephonyCommandPublisher;
use RuntimeException;
use Tests\TestCase;

final class RdkafkaAdapterAvailabilityTest extends TestCase
{
    public function test_native_consumer_fails_fast_when_rdkafka_extension_is_missing(): void
    {
        if (class_exists('RdKafka\\Conf')) {
            $this->markTestSkipped('php-rdkafka is installed in this environment.');
        }

        $consumer = new RdkafkaKafkaConsumer(new RdkafkaRuntime);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('php-rdkafka class');

        $consumer->consume('incoming-calls', 'calls', 'test', 1, 100, static function (): void {});
    }

    public function test_native_producer_fails_fast_when_rdkafka_extension_is_missing(): void
    {
        if (class_exists('RdKafka\\Conf')) {
            $this->markTestSkipped('php-rdkafka is installed in this environment.');
        }

        $publisher = new RdkafkaTelephonyCommandPublisher(new RdkafkaRuntime);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('php-rdkafka class');

        $publisher->publish(new TelephonyOutboxMessage(
            id: 1,
            commandId: 'command-1',
            idempotencyKey: 'call-1:command:1',
            type: 'call_assignment_requested',
            externalCallId: 'call-1',
            payload: ['external_call_id' => 'call-1'],
            attempts: 1,
        ));
    }
}
