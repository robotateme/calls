<?php

declare(strict_types=1);

namespace Application\Calls\Commands;

use Application\Calls\Ports\CallProcessingRetryQueue;
use Application\Calls\Ports\CallWriteRepository;
use Application\Operators\Ports\OperatorReservationRepository;
use Application\Shared\Ports\TransactionManager;
use Application\Telephony\Ports\TelephonyCommandOutboxWriter;
use Domain\Calls\OperatorAssignmentFailure;
use Domain\Operators\OperatorId;
use Domain\Shared\Timestamp;

final readonly class MarkOperatorNoAnswerHandler
{
    public function __construct(
        private CallWriteRepository $calls,
        private OperatorReservationRepository $operators,
        private TelephonyCommandOutboxWriter $telephonyCommands,
        private CallProcessingRetryQueue $retryQueue,
        private TransactionManager $transactions,
    ) {}

    public function handle(MarkOperatorNoAnswerFromKafkaCommand $command): void
    {
        $result = $this->transactions->run(function () use ($command): ?OperatorAssignmentFailure {
            $call = $this->calls->findForUpdateByExternalCallId(trim($command->externalCallId));

            if ($call === null) {
                return null;
            }

            $failure = $call->failPendingOperatorAssignment(
                operatorId: OperatorId::fromInt($command->operatorId),
                attempt: $command->assignmentAttempt,
                now: Timestamp::now(),
            );

            if ($failure === null) {
                return null;
            }

            $this->calls->save($call);
            $this->operators->releaseForCall(OperatorId::fromInt($command->operatorId), $call->callId());

            if ($failure->shouldRetry()) {
                $this->telephonyCommands->recordOperatorSearchRetryScheduled(
                    $call->externalCallId(),
                    $call->operatorSearchAttempts(),
                    $failure->retryDelaySeconds(),
                );

                return $failure;
            }

            $finalStatus = $failure->finalStatus();

            if ($finalStatus === null) {
                return null;
            }

            $this->telephonyCommands->recordOperatorSearchExhausted(
                $call->externalCallId(),
                $call->operatorSearchAttempts(),
                $finalStatus->value,
            );

            return $failure;
        });

        if ($result?->shouldRetry() === true) {
            $this->retryQueue->retryLater($result->callId(), $result->retryDelaySeconds());
        }
    }
}
