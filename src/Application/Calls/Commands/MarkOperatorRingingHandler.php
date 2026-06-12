<?php

declare(strict_types=1);

namespace Application\Calls\Commands;

use Application\Calls\Ports\CallWriteRepository;
use Application\Shared\Ports\TransactionManager;
use Domain\Operators\OperatorId;
use Domain\Shared\Timestamp;

final readonly class MarkOperatorRingingHandler
{
    public function __construct(
        private CallWriteRepository $calls,
        private TransactionManager $transactions,
    ) {}

    public function handle(MarkOperatorRingingFromKafkaCommand $command): void
    {
        $this->transactions->run(function () use ($command): void {
            $call = $this->calls->findForUpdateByExternalCallId(trim($command->externalCallId));

            if ($call === null) {
                return;
            }

            if (! $call->markOperatorRinging(
                operatorId: OperatorId::fromInt($command->operatorId),
                attempt: $command->assignmentAttempt,
                ringingAt: Timestamp::now(),
            )) {
                return;
            }

            $this->calls->save($call);
        });
    }
}
