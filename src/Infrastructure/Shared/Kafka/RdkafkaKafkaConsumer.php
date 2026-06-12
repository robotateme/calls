<?php

declare(strict_types=1);

namespace Infrastructure\Shared\Kafka;

use Application\Shared\Ports\KafkaConsumer;
use Application\Shared\Ports\KafkaConsumerMessage;
use RuntimeException;

final readonly class RdkafkaKafkaConsumer implements KafkaConsumer
{
    public function __construct(private RdkafkaRuntime $runtime) {}

    public function consume(
        string $topic,
        string $groupId,
        string $source,
        int $limit,
        int $timeoutMs,
        callable $handler,
    ): int {
        $conf = $this->runtime->newInstance('RdKafka\\Conf');
        $this->runtime->invoke($conf, 'set', ['metadata.broker.list', (string) config('calls.kafka_brokers')]);
        $this->runtime->invoke($conf, 'set', ['group.id', $groupId]);
        $this->runtime->invoke($conf, 'set', ['enable.auto.commit', 'false']);
        $this->runtime->invoke($conf, 'set', ['auto.offset.reset', (string) config('calls.kafka_auto_offset_reset')]);

        $consumer = $this->runtime->newInstance('RdKafka\\KafkaConsumer', [$conf]);
        $this->runtime->invoke($consumer, 'subscribe', [[$topic]]);

        $consumed = 0;

        while ($consumed < $limit) {
            $message = $this->runtime->invoke($consumer, 'consume', [$timeoutMs]);

            if (! is_object($message)) {
                throw new RuntimeException('php-rdkafka consume returned a non-object message.');
            }

            $errorCode = $this->intProperty($message, 'err');

            if ($errorCode === $this->runtime->intConstant('RD_KAFKA_RESP_ERR__TIMED_OUT')) {
                break;
            }

            if ($errorCode === $this->runtime->intConstant('RD_KAFKA_RESP_ERR__PARTITION_EOF')) {
                continue;
            }

            if ($errorCode !== $this->runtime->intConstant('RD_KAFKA_RESP_ERR_NO_ERROR')) {
                throw new RuntimeException(sprintf('Kafka consumer error code: %d', $errorCode));
            }

            $handler(new KafkaConsumerMessage(
                source: $source,
                topic: $this->stringProperty($message, 'topic_name') ?? $topic,
                partition: $this->intProperty($message, 'partition'),
                offset: $this->intProperty($message, 'offset'),
                key: $this->nullableStringProperty($message, 'key'),
                traceId: $this->traceId($message),
                payload: $this->stringProperty($message, 'payload') ?? '',
            ));

            $this->runtime->invoke($consumer, 'commit', [$message]);
            $consumed++;
        }

        return $consumed;
    }

    private function intProperty(object $message, string $property): int
    {
        $value = $this->runtime->property($message, $property);

        if (! is_int($value)) {
            throw new RuntimeException(sprintf('Kafka message property "%s" must be int.', $property));
        }

        return $value;
    }

    private function stringProperty(object $message, string $property): ?string
    {
        $value = $this->runtime->property($message, $property);

        if ($value === null) {
            return null;
        }

        if (! is_string($value)) {
            throw new RuntimeException(sprintf('Kafka message property "%s" must be string.', $property));
        }

        return $value;
    }

    private function nullableStringProperty(object $message, string $property): ?string
    {
        $value = $this->stringProperty($message, $property);

        return $value === '' ? null : $value;
    }

    private function traceId(object $message): ?string
    {
        $headers = $this->runtime->property($message, 'headers');

        if (! is_array($headers)) {
            return null;
        }

        $traceId = $headers['trace_id'] ?? $headers['traceparent'] ?? null;

        return is_string($traceId) && trim($traceId) !== '' ? trim($traceId) : null;
    }
}
