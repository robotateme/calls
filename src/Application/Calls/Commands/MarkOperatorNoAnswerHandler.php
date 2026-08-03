<?php

declare(strict_types=1);

namespace Application\Calls\Commands;

final readonly class MarkOperatorNoAnswerHandler
{
    public function __construct(private HandleFailedOperatorAssignment $failedAssignments) {}

    public function handle(MarkOperatorNoAnswerFromKafkaCommand $command): void
    {
        $this->failedAssignments->handle(
            externalCallId: $command->externalCallId,
            operatorId: $command->operatorId,
            assignmentAttempt: $command->assignmentAttempt,
        );
    }
}
