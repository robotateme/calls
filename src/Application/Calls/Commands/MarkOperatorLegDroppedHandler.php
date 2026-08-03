<?php

declare(strict_types=1);

namespace Application\Calls\Commands;

final readonly class MarkOperatorLegDroppedHandler
{
    public function __construct(private HandleFailedOperatorAssignment $failedAssignments) {}

    public function handle(MarkOperatorLegDroppedFromKafkaCommand $command): void
    {
        $this->failedAssignments->handle(
            externalCallId: $command->externalCallId,
            operatorId: $command->operatorId,
            assignmentAttempt: $command->assignmentAttempt,
        );
    }
}
