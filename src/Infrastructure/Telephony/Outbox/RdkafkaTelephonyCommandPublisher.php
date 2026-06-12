<?php

declare(strict_types=1);

namespace Infrastructure\Telephony\Outbox;

use Application\Telephony\Ports\TelephonyCommandPublisher;
use Domain\Telephony\TelephonyOutboxMessage;
use Infrastructure\Shared\Kafka\RdkafkaRuntime;
use RuntimeException;

final readonly class RdkafkaTelephonyCommandPublisher implements TelephonyCommandPublisher
{
    public function __construct(private RdkafkaRuntime $runtime) {}

    public function publish(TelephonyOutboxMessage $message): void
    {
        $conf = $this->runtime->newInstance('RdKafka\\Conf');
        $this->runtime->invoke($conf, 'set', ['metadata.broker.list', (string) config('calls.kafka_brokers')]);

        $producer = $this->runtime->newInstance('RdKafka\\Producer', [$conf]);
        $topic = $this->runtime->invoke($producer, 'newTopic', [(string) config('calls.telephony_commands_topic')]);

        if (! is_object($topic)) {
            throw new RuntimeException('php-rdkafka producer returned a non-object topic.');
        }

        $this->runtime->invoke($topic, 'produce', [
            $this->runtime->intConstant('RD_KAFKA_PARTITION_UA'),
            0,
            $this->payload($message),
            $message->externalCallId,
        ]);

        $this->runtime->invoke($producer, 'poll', [0]);

        $flushResult = $this->runtime->invoke($producer, 'flush', [(int) config('calls.kafka_producer_flush_timeout_ms')]);

        if ($flushResult !== $this->runtime->intConstant('RD_KAFKA_RESP_ERR_NO_ERROR')) {
            throw new RuntimeException(sprintf('Kafka producer flush failed with code %s.', is_scalar($flushResult) ? (string) $flushResult : get_debug_type($flushResult)));
        }
    }

    private function payload(TelephonyOutboxMessage $message): string
    {
        return json_encode([
            'schema_version' => 1,
            'command_id' => $message->commandId,
            'idempotency_key' => $message->idempotencyKey,
            'type' => $message->type,
            'external_call_id' => $message->externalCallId,
            'payload' => $message->payload,
        ], JSON_THROW_ON_ERROR);
    }
}
