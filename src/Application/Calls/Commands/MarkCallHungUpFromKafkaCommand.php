<?php

declare(strict_types=1);

namespace Application\Calls\Commands;

final readonly class MarkCallHungUpFromKafkaCommand
{
    public function __construct(
        public string $externalCallId,
        public string $kafkaMessageId,
    ) {}
}
