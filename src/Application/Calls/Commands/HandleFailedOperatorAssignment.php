<?php

declare(strict_types=1);

namespace Application\Calls\Commands;

use Application\Calls\Ports\CallProcessingRetryQueue;
use Application\Calls\Ports\CallWriteRepository;
use Application\Operators\Ports\OperatorReservationRepository;
use Application\Shared\Ports\Metrics;
use Application\Shared\Ports\TransactionManager;
use Application\Telephony\Ports\TelephonyCommandOutboxWriter;
use Domain\Operators\OperatorId;
use Domain\Shared\Timestamp;

final readonly class HandleFailedOperatorAssignment
{
    public function __construct(
        private CallWriteRepository $calls,
        private OperatorReservationRepository $operators,
        private TelephonyCommandOutboxWriter $telephonyCommands,
        private CallProcessingRetryQueue $retryQueue,
        private TransactionManager $transactions,
        private Metrics $metrics,
    ) {}

    public function handle(string $externalCallId, int $operatorId, int $assignmentAttempt): void
    {
        $normalizedExternalCallId = trim($externalCallId);
        $assignedOperatorId = OperatorId::fromInt($operatorId);

        $result = $this->transactions->run(function () use ($normalizedExternalCallId, $assignedOperatorId, $assignmentAttempt): ?array {
            $call = $this->calls->findForUpdateByExternalCallId($normalizedExternalCallId);

            if ($call === null) {
                return null;
            }

            $previousStatus = $call->status()->value;
            $failure = $call->failPendingOperatorAssignment(
                operatorId: $assignedOperatorId,
                attempt: $assignmentAttempt,
                now: Timestamp::now(),
            );

            if ($failure === null) {
                return null;
            }

            $this->calls->save($call);
            $this->operators->releaseForCall($assignedOperatorId, $call->callId());

            if ($failure->shouldRetry()) {
                $this->telephonyCommands->recordOperatorSearchRetryScheduled(
                    $call->externalCallId(),
                    $call->operatorSearchAttempts(),
                    $failure->retryDelaySeconds(),
                );

                return [
                    'failure' => $failure,
                    'from' => $previousStatus,
                    'to' => $call->status()->value,
                ];
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

            return [
                'failure' => $failure,
                'from' => $previousStatus,
                'to' => $call->status()->value,
            ];
        });

        if ($result === null) {
            return;
        }

        $failure = $result['failure'];

        $this->recordCallTransition($result['from'], $result['to']);

        if ($failure->shouldRetry()) {
            $this->retryQueue->retryLater($failure->callId(), $failure->retryDelaySeconds());
            $this->metrics->increment('retry_scheduled_total', tags: [
                'reason' => 'operator_assignment_failed',
            ]);

            return;
        }

        $finalStatus = $failure->finalStatus();

        if ($finalStatus !== null) {
            $this->metrics->increment('calls_finished_total', tags: [
                'result' => $finalStatus->value,
            ]);
        }
    }

    private function recordCallTransition(string $fromStatus, string $toStatus): void
    {
        if ($fromStatus === $toStatus) {
            return;
        }

        $this->metrics->increment('call_transitions_total', tags: [
            'from' => $fromStatus,
            'to' => $toStatus,
        ]);
    }
}
