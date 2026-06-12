<?php

declare(strict_types=1);

namespace Application\Calls\Commands;

use Application\Calls\Ports\CallProcessingRetryQueue;
use Application\Calls\Ports\CallWriteRepository;
use Application\Operators\Ports\OperatorReservationRepository;
use Application\Shared\Ports\Metrics;
use Application\Shared\Ports\TransactionManager;
use Application\Telephony\Ports\TelephonyCommandOutboxReader;
use Application\Telephony\Ports\TelephonyCommandOutboxWriter;
use Domain\Calls\OperatorAssignmentFailure;
use Domain\Shared\Timestamp;

final readonly class ReleaseExpiredOperatorReservationsHandler
{
    public function __construct(
        private CallWriteRepository $calls,
        private OperatorReservationRepository $operators,
        private TelephonyCommandOutboxWriter $telephonyCommandWriter,
        private TelephonyCommandOutboxReader $telephonyCommandReader,
        private CallProcessingRetryQueue $retryQueue,
        private TransactionManager $transactions,
        private Metrics $metrics,
    ) {}

    public function handle(int $olderThanSeconds, int $limit): int
    {
        $startedAt = microtime(true);
        $failures = $this->transactions->run(function () use ($olderThanSeconds, $limit): array {
            $expiredBefore = Timestamp::now()->minusSeconds(max(0, $olderThanSeconds));
            $now = Timestamp::now();
            $failures = [];

            foreach ($this->calls->findExpiredAssignmentsForUpdate($expiredBefore, max(1, $limit)) as $call) {
                $operatorId = $call->assignedOperatorId();

                if ($operatorId === null) {
                    continue;
                }

                $attempt = $call->operatorSearchAttempts();
                $failure = $call->failPendingOperatorAssignment($operatorId, $attempt, $now);

                if ($failure === null) {
                    continue;
                }

                $this->telephonyCommandWriter->cancelPendingAssignmentRequests(
                    externalCallId: $call->externalCallId(),
                    reason: 'operator_assignment_timeout',
                );

                if ($this->telephonyCommandReader->hasPublishedAssignmentRequest($call->externalCallId())) {
                    $this->telephonyCommandWriter->recordCallAssignmentCanceled(
                        externalCallId: $call->externalCallId(),
                        operatorId: $operatorId->toInt(),
                        attempt: $attempt,
                        reason: 'operator_assignment_timeout',
                    );
                }

                $this->calls->save($call);
                $this->operators->releaseForCall($operatorId, $call->callId());
                $this->recordFailureOutcome($call->externalCallId(), $call->operatorSearchAttempts(), $failure);
                $failures[] = $failure;
            }

            return $failures;
        });

        foreach ($failures as $failure) {
            if ($failure->shouldRetry()) {
                $this->retryQueue->retryLater($failure->callId(), $failure->retryDelaySeconds());
            }
        }

        $released = count($failures);
        $retries = count(array_filter($failures, static fn (OperatorAssignmentFailure $failure): bool => $failure->shouldRetry()));
        $finalized = $released - $retries;

        $this->metrics->increment('operator_reservation.expired_released', $released);
        $this->metrics->increment('operator_reservation.expired_retried', $retries);
        $this->metrics->increment('operator_reservation.expired_finalized', $finalized);
        $this->metrics->timing('operator_reservation.release_expired_duration_ms', (microtime(true) - $startedAt) * 1000);

        return count($failures);
    }

    private function recordFailureOutcome(string $externalCallId, int $attempt, OperatorAssignmentFailure $failure): void
    {
        if ($failure->shouldRetry()) {
            $this->telephonyCommandWriter->recordOperatorSearchRetryScheduled(
                $externalCallId,
                $attempt,
                $failure->retryDelaySeconds(),
            );

            return;
        }

        $finalStatus = $failure->finalStatus();

        if ($finalStatus === null) {
            return;
        }

        $this->telephonyCommandWriter->recordOperatorSearchExhausted(
            $externalCallId,
            $attempt,
            $finalStatus->value,
        );
    }
}
