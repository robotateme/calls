<?php

declare(strict_types=1);

namespace Application\Calls\Commands;

use Application\Calls\Ports\CallWriteRepository;
use Application\Shared\Ports\TransactionManager;
use Domain\Operators\OperatorId;
use Domain\Shared\Timestamp;

final readonly class MarkOperatorDialingHandler
{
    public function __construct(
        private CallWriteRepository $calls,
        private TransactionManager $transactions,
    ) {}

    public function handle(MarkOperatorDialingFromKafkaCommand $command): void
    {
        $normalizedExternalCallId = trim($command->externalCallId);
        $operatorId = OperatorId::fromInt($command->operatorId);

        $this->transactions->run(function () use ($normalizedExternalCallId, $operatorId, $command): void {
            $call = $this->calls->findForUpdateByExternalCallId($normalizedExternalCallId);

            if ($call === null) {
                return;
            }

            if (! $call->markOperatorDialing(
                operatorId: $operatorId,
                attempt: $command->assignmentAttempt,
                dialingAt: Timestamp::now(),
            )) {
                return;
            }

            $this->calls->save($call);
        });
    }
}
