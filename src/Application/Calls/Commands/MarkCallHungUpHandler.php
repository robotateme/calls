<?php

declare(strict_types=1);

namespace Application\Calls\Commands;

use Application\Calls\Ports\CallWriteRepository;
use Application\Operators\Ports\OperatorReservationRepository;
use Application\Shared\Ports\Metrics;
use Application\Shared\Ports\TransactionManager;
use Application\Telephony\Ports\TelephonyCommandOutboxReader;
use Application\Telephony\Ports\TelephonyCommandOutboxWriter;

final readonly class MarkCallHungUpHandler
{
    public function __construct(
        private CallWriteRepository $calls,
        private OperatorReservationRepository $operators,
        private TelephonyCommandOutboxWriter $telephonyCommandWriter,
        private TelephonyCommandOutboxReader $telephonyCommandReader,
        private TransactionManager $transactions,
        private Metrics $metrics,
    ) {}

    public function handle(MarkCallHungUpFromKafkaCommand $command): void
    {
        $normalizedExternalCallId = trim($command->externalCallId);

        $finishedStatus = $this->transactions->run(function () use ($normalizedExternalCallId): ?string {
            $call = $this->calls->findForUpdateByExternalCallId($normalizedExternalCallId);

            if ($call === null) {
                return null;
            }

            $operatorId = $call->isAssignmentInProgress() ? $call->assignedOperatorId() : null;
            $attempt = $call->operatorSearchAttempts();

            if (! $call->markHungUp()) {
                return null;
            }

            $this->calls->save($call);

            if ($operatorId !== null) {
                $this->telephonyCommandWriter->cancelPendingAssignmentRequests(
                    externalCallId: $call->externalCallId(),
                    reason: 'call_hung_up',
                );

                if ($this->telephonyCommandReader->hasPublishedAssignmentRequest($call->externalCallId())) {
                    $this->telephonyCommandWriter->recordCallAssignmentCanceled(
                        externalCallId: $call->externalCallId(),
                        operatorId: $operatorId->toInt(),
                        attempt: $attempt,
                        reason: 'call_hung_up',
                    );
                }

                $this->operators->releaseForCall($operatorId, $call->callId());
            }

            return $call->status()->value;
        });

        if ($finishedStatus !== null) {
            $this->metrics->increment('calls_finished_total', tags: [
                'result' => $finishedStatus,
            ]);
        }
    }
}
