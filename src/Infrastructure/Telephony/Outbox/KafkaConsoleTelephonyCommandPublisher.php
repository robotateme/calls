<?php

declare(strict_types=1);

namespace Infrastructure\Telephony\Outbox;

use Application\Shared\Ports\ConsoleCommandRunner;
use Application\Telephony\Ports\TelephonyCommandPublisher;
use Domain\Telephony\TelephonyOutboxMessage;
use RuntimeException;

final readonly class KafkaConsoleTelephonyCommandPublisher implements TelephonyCommandPublisher
{
    public function __construct(private ConsoleCommandRunner $console) {}

    public function publish(TelephonyOutboxMessage $message): void
    {
        $topic = (string) config('calls.telephony_commands_topic');
        $brokers = (string) config('calls.kafka_brokers');
        $binary = (string) config('calls.kafka_console_producer_binary');
        $timeoutSeconds = (int) config('calls.kafka_console_producer_timeout_seconds');

        $payload = json_encode([
            'schema_version' => 1,
            'command_id' => $message->commandId,
            'idempotency_key' => $message->idempotencyKey,
            'type' => $message->type,
            'external_call_id' => $message->externalCallId,
            'payload' => $message->payload,
        ], JSON_THROW_ON_ERROR);

        $result = $this->console->run(
            command: [
                $binary,
                '--bootstrap-server',
                $brokers,
                '--topic',
                $topic,
                '--property',
                'parse.key=true',
                '--property',
                "key.separator=\t",
            ],
            stdin: $message->externalCallId."\t".$payload.PHP_EOL,
            timeoutSeconds: $timeoutSeconds,
        );

        if (! $result->successful()) {
            throw new RuntimeException(sprintf(
                'Kafka console producer failed with exit code %d: %s',
                $result->exitCode,
                trim($result->stderr) !== '' ? trim($result->stderr) : trim($result->stdout),
            ));
        }
    }
}
