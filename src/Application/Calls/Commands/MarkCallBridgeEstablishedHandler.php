<?php

declare(strict_types=1);

namespace Application\Calls\Commands;

use Application\Calls\Ports\CallWriteRepository;
use Application\Operators\Ports\OperatorReservationRepository;
use Application\Shared\Ports\TransactionManager;
use Domain\Operators\OperatorId;
use Domain\Shared\Timestamp;

final readonly class MarkCallBridgeEstablishedHandler
{
    public function __construct(
        private CallWriteRepository $calls,
        private OperatorReservationRepository $operators,
        private TransactionManager $transactions,
    ) {}

    public function handle(MarkCallBridgeEstablishedFromKafkaCommand $command): void
    {
        $this->transactions->run(function () use ($command): void {
            $call = $this->calls->findForUpdateByExternalCallId(trim($command->externalCallId));

            if ($call === null) {
                return;
            }

            if (! $call->markConnected(
                operatorId: OperatorId::fromInt($command->operatorId),
                attempt: $command->assignmentAttempt,
                connectedAt: Timestamp::now(),
            )) {
                return;
            }

            $this->calls->save($call);
            $this->operators->releaseForCall(OperatorId::fromInt($command->operatorId), $call->callId());
        });
    }
}
