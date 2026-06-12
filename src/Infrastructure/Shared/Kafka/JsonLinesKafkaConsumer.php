<?php

declare(strict_types=1);

namespace Infrastructure\Shared\Kafka;

use Application\Shared\Ports\KafkaConsumer;
use Application\Shared\Ports\KafkaConsumerMessage;
use InvalidArgumentException;
use JsonException;
use RuntimeException;

final readonly class JsonLinesKafkaConsumer implements KafkaConsumer
{
    public function consume(
        string $topic,
        string $groupId,
        string $source,
        int $limit,
        int $timeoutMs,
        callable $handler,
    ): int {
        $input = fopen('php://stdin', 'r');

        if ($input === false) {
            throw new RuntimeException('Unable to open stdin.');
        }

        stream_set_blocking($input, false);

        $consumed = 0;
        $deadline = microtime(true) + ($timeoutMs / 1000);

        while ($consumed < $limit && microtime(true) <= $deadline) {
            $line = $this->readLine($input, $deadline);

            if ($line === null) {
                break;
            }

            if (trim($line) === '') {
                continue;
            }

            $handler($this->messageFromJsonLine($line, $topic, $source));
            $consumed++;
        }

        fclose($input);

        return $consumed;
    }

    /**
     * @param  resource  $input
     */
    private function readLine(mixed $input, float $deadline): ?string
    {
        $remainingMicroseconds = max(0, (int) (($deadline - microtime(true)) * 1000000));
        $seconds = intdiv($remainingMicroseconds, 1000000);
        $microseconds = $remainingMicroseconds % 1000000;
        $read = [$input];
        $write = null;
        $except = null;
        $ready = stream_select($read, $write, $except, $seconds, $microseconds);

        if ($ready === false || $ready === 0) {
            return null;
        }

        $line = fgets($input);

        return $line === false ? null : $line;
    }

    private function messageFromJsonLine(string $line, string $topic, string $source): KafkaConsumerMessage
    {
        try {
            $record = json_decode($line, true, flags: JSON_THROW_ON_ERROR);
        } catch (JsonException $exception) {
            throw new InvalidArgumentException('JSONL Kafka record must be valid JSON.', previous: $exception);
        }

        if (! is_array($record)) {
            throw new InvalidArgumentException('JSONL Kafka record must be an object.');
        }

        /** @var array<string, mixed> $record */
        $payload = $record['payload'] ?? null;

        return new KafkaConsumerMessage(
            source: $this->optionalString($record, 'source') ?? $source,
            topic: $this->optionalString($record, 'topic') ?? $topic,
            partition: $this->optionalInt($record, 'partition'),
            offset: $this->optionalInt($record, 'offset'),
            key: $this->optionalString($record, 'key'),
            traceId: $this->optionalString($record, 'trace_id'),
            payload: is_string($payload) ? $payload : json_encode($payload ?? $record, JSON_THROW_ON_ERROR),
        );
    }

    /**
     * @param  array<string, mixed>  $record
     */
    private function optionalString(array $record, string $field): ?string
    {
        $value = $record[$field] ?? null;

        if ($value === null) {
            return null;
        }

        if (! is_scalar($value)) {
            throw new InvalidArgumentException(sprintf('JSONL Kafka record field "%s" must be scalar.', $field));
        }

        $value = trim((string) $value);

        return $value === '' ? null : $value;
    }

    /**
     * @param  array<string, mixed>  $record
     */
    private function optionalInt(array $record, string $field): ?int
    {
        $value = $this->optionalString($record, $field);

        return $value === null ? null : (int) $value;
    }
}
