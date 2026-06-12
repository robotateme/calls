<?php

declare(strict_types=1);

namespace Tests\Unit;

use Application\Shared\Ports\ConsoleCommandResult;
use Application\Shared\Ports\ConsoleCommandRunner;
use Domain\Telephony\TelephonyOutboxMessage;
use Infrastructure\Telephony\Outbox\KafkaConsoleTelephonyCommandPublisher;
use RuntimeException;
use Tests\TestCase;

final class KafkaConsoleTelephonyCommandPublisherTest extends TestCase
{
    public function test_it_publishes_message_with_keyed_kafka_console_producer_input(): void
    {
        config()->set('calls.kafka_brokers', 'kafka:9092');
        config()->set('calls.telephony_commands_topic', 'telephony.commands');
        config()->set('calls.kafka_console_producer_binary', 'kafka-console-producer.sh');
        config()->set('calls.kafka_console_producer_timeout_seconds', 7);

        $runner = new FakeConsoleCommandRunner(new ConsoleCommandResult(0, '', ''));
        $publisher = new KafkaConsoleTelephonyCommandPublisher($runner);

        $publisher->publish($this->message());

        $this->assertSame([
            'kafka-console-producer.sh',
            '--bootstrap-server',
            'kafka:9092',
            '--topic',
            'telephony.commands',
            '--property',
            'parse.key=true',
            '--property',
            "key.separator=\t",
        ], $runner->commands[0]);
        $this->assertSame(7, $runner->timeouts[0]);
        $this->assertStringStartsWith("asterisk-linkedid-6001\t", $runner->stdins[0]);

        $parts = explode("\t", trim($runner->stdins[0]), 2);

        $this->assertSame('asterisk-linkedid-6001', $parts[0]);
        $this->assertCount(2, $parts);

        $json = $parts[1];
        $payload = json_decode($json, true, flags: JSON_THROW_ON_ERROR);

        $this->assertSame(1, $payload['schema_version']);
        $this->assertSame('command-6001', $payload['command_id']);
        $this->assertSame('asterisk-linkedid-6001:call_assignment_requested:1', $payload['idempotency_key']);
        $this->assertSame('call_assignment_requested', $payload['type']);
        $this->assertSame('asterisk-linkedid-6001', $payload['external_call_id']);
        $this->assertSame(15, $payload['payload']['operator_id']);
    }

    public function test_it_throws_when_console_producer_fails(): void
    {
        $runner = new FakeConsoleCommandRunner(new ConsoleCommandResult(1, '', 'connection refused'));
        $publisher = new KafkaConsoleTelephonyCommandPublisher($runner);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Kafka console producer failed');

        $publisher->publish($this->message());
    }

    private function message(): TelephonyOutboxMessage
    {
        return new TelephonyOutboxMessage(
            id: 1,
            commandId: 'command-6001',
            idempotencyKey: 'asterisk-linkedid-6001:call_assignment_requested:1',
            type: 'call_assignment_requested',
            externalCallId: 'asterisk-linkedid-6001',
            payload: [
                'external_call_id' => 'asterisk-linkedid-6001',
                'operator_id' => 15,
                'assignment_attempt' => 1,
            ],
            attempts: 1,
        );
    }
}

final class FakeConsoleCommandRunner implements ConsoleCommandRunner
{
    /**
     * @var list<list<string>>
     */
    public array $commands = [];

    /**
     * @var list<string>
     */
    public array $stdins = [];

    /**
     * @var list<int>
     */
    public array $timeouts = [];

    public function __construct(private readonly ConsoleCommandResult $result) {}

    public function run(array $command, string $stdin, int $timeoutSeconds): ConsoleCommandResult
    {
        $this->commands[] = $command;
        $this->stdins[] = $stdin;
        $this->timeouts[] = $timeoutSeconds;

        return $this->result;
    }
}
