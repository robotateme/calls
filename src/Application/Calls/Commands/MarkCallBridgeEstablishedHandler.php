<?php

declare(strict_types=1);

namespace Application\Calls\Commands;

use Application\Calls\Ports\CallWriteRepository;
use Application\Operators\Ports\OperatorReservationRepository;
use Application\Shared\Ports\Metrics;
use Application\Shared\Ports\TransactionManager;
use Domain\Calls\CallStatus;
use Domain\Operators\OperatorId;
use Domain\Shared\Timestamp;

final readonly class MarkCallBridgeEstablishedHandler
{
    public function __construct(
        private CallWriteRepository $calls,
        private OperatorReservationRepository $operators,
        private TransactionManager $transactions,
        private Metrics $metrics,
    ) {}

    public function handle(MarkCallBridgeEstablishedFromKafkaCommand $command): void
    {
        $normalizedExternalCallId = trim($command->externalCallId);
        $operatorId = OperatorId::fromInt($command->operatorId);

        $connectedAfterSeconds = $this->transactions->run(function () use ($normalizedExternalCallId, $operatorId, $command): ?float {
            $call = $this->calls->findForUpdateByExternalCallId($normalizedExternalCallId);

            if ($call === null) {
                return null;
            }

            $previousStatus = $call->status();
            $connectedAt = Timestamp::now();

            if (! $call->markConnected(
                operatorId: $operatorId,
                attempt: $command->assignmentAttempt,
                connectedAt: $connectedAt,
            )) {
                return null;
            }

            $this->calls->save($call);
            $this->operators->releaseForCall($operatorId, $call->callId());

            if ($previousStatus === CallStatus::Connected) {
                return null;
            }

            return $this->secondsBetween($call->createdTimestamp(), $connectedAt);
        });

        if ($connectedAfterSeconds === null) {
            return;
        }

        $this->metrics->increment('calls_finished_total', tags: [
            'result' => 'connected',
        ]);
        $this->metrics->timing('call_time_to_connect_seconds', $connectedAfterSeconds);
    }

    private function secondsBetween(Timestamp $from, Timestamp $to): float
    {
        return max(0, $to->toDateTimeImmutable()->getTimestamp() - $from->toDateTimeImmutable()->getTimestamp());
    }
}
