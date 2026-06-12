<?php

declare(strict_types=1);

namespace Application\Calls\Commands;

use Application\Shared\Ports\KafkaConsumer;
use Application\Shared\Ports\KafkaConsumerMessage;

final readonly class ConsumeKafkaCallFactsHandler
{
    public function __construct(
        private KafkaConsumer $consumer,
        private HandleKafkaCallFactHandler $facts,
    ) {}

    public function handle(ConsumeKafkaCallFactsCommand $command): int
    {
        return $this->consumer->consume(
            topic: trim($command->topic),
            groupId: trim($command->groupId),
            source: trim($command->source),
            limit: max(1, $command->limit),
            timeoutMs: max(1, $command->timeoutMs),
            handler: function (KafkaConsumerMessage $message): void {
                $this->facts->handle(new HandleKafkaCallFactCommand(
                    source: $message->source,
                    topic: $message->topic,
                    partition: $message->partition,
                    offset: $message->offset,
                    messageKey: $message->key,
                    traceId: $message->traceId,
                    rawPayload: $message->payload,
                ));
            },
        );
    }
}
