<?php

declare(strict_types=1);

namespace Application\Calls\Commands;

final readonly class MarkOperatorLegDroppedFromKafkaCommand
{
    public function __construct(
        public string $externalCallId,
        public int $operatorId,
        public int $assignmentAttempt,
        public string $kafkaMessageId,
    ) {}
}
