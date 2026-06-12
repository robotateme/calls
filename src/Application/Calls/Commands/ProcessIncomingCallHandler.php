<?php

declare(strict_types=1);

namespace Application\Calls\Commands;

use Application\Calls\Ports\CallProcessingLogger;
use Application\Calls\Ports\CallProcessingRetryQueue;
use Application\Calls\Ports\CallWriteRepository;
use Application\Clients\Ports\ClientReadRepository;
use Application\Operators\Ports\OperatorReservationRepository;
use Application\Shared\Ports\Metrics;
use Application\Shared\Ports\TransactionManager;
use Application\Telephony\Ports\TelephonyCommandOutboxWriter;
use Domain\Shared\Timestamp;

final readonly class ProcessIncomingCallHandler
{
    public function __construct(
        private CallWriteRepository $calls,
        private ClientReadRepository $clients,
        private OperatorReservationRepository $operators,
        private TransactionManager $transactions,
        private TelephonyCommandOutboxWriter $telephonyCommands,
        private CallProcessingLogger $logger,
        private CallProcessingRetryQueue $retryQueue,
        private Metrics $metrics,
    ) {}

    public function handle(ProcessIncomingCallCommand $command): void
    {
        $startedAt = microtime(true);
        $result = $this->transactions->run(function () use ($command): ?ProcessIncomingCallResult {
            $call = $this->calls->findForUpdate($command->callId);

            if ($call === null) {
                return null;
            }

            if (! $call->isProcessable()) {
                return null;
            }

            $clientId = $this->clients->findIdByPhone($call->phoneNumber());
            $call->attachClient($clientId);

            if (! $call->isProcessable()) {
                return null;
            }

            $operator = $this->operators->reserveAvailableForCall($call->callId());

            if ($operator === null) {
                $outcome = $call->recordFailedOperatorSearchAttempt(
                    Timestamp::now(),
                );
                $this->calls->save($call);

                if ($outcome->shouldRetry()) {
                    $this->telephonyCommands->recordOperatorSearchRetryScheduled(
                        $call->externalCallId(),
                        $call->operatorSearchAttempts(),
                        $outcome->retryDelaySeconds(),
                    );

                    return new ProcessIncomingCallResult(
                        callId: $call->id(),
                        externalCallId: $call->externalCallId(),
                        operatorId: null,
                        clientId: $call->clientId(),
                        waitingForOperator: true,
                        attempt: $call->operatorSearchAttempts(),
                        retryDelaySeconds: $outcome->retryDelaySeconds(),
                        finalStatus: null,
                    );
                }

                $finalStatus = $outcome->finalStatus();

                if ($finalStatus === null) {
                    return null;
                }

                $this->telephonyCommands->recordOperatorSearchExhausted(
                    $call->externalCallId(),
                    $call->operatorSearchAttempts(),
                    $finalStatus->value,
                );

                return new ProcessIncomingCallResult(
                    callId: $call->id(),
                    externalCallId: $call->externalCallId(),
                    operatorId: null,
                    clientId: $call->clientId(),
                    waitingForOperator: false,
                    attempt: $call->operatorSearchAttempts(),
                    retryDelaySeconds: 0,
                    finalStatus: $finalStatus->value,
                );
            }

            $outcome = $call->recordSuccessfulOperatorSearchAttempt($operator->operatorId, Timestamp::now());

            if ($outcome === null) {
                $this->operators->releaseForCall($operator->operatorId, $call->callId());

                return null;
            }

            $this->calls->save($call);
            $this->telephonyCommands->recordCallAssignmentRequested(
                $call->externalCallId(),
                $operator->operatorId->toInt(),
                $call->operatorSearchAttempts(),
            );

            return new ProcessIncomingCallResult(
                callId: $call->id(),
                externalCallId: $call->externalCallId(),
                operatorId: $operator->operatorId->toInt(),
                clientId: $call->clientId(),
                waitingForOperator: false,
                attempt: $call->operatorSearchAttempts(),
                retryDelaySeconds: 0,
                finalStatus: null,
            );
        });

        if ($result === null) {
            $this->metrics->increment('call_processing.skipped');
            $this->metrics->timing('call_processing.duration_ms', (microtime(true) - $startedAt) * 1000, [
                'result' => 'skipped',
            ]);

            return;
        }

        if ($result->waitingForOperator) {
            $this->retryQueue->retryLater($result->callId, $result->retryDelaySeconds);
            $this->metrics->increment('operator_search.retry_scheduled');
            $this->metrics->timing('call_processing.duration_ms', (microtime(true) - $startedAt) * 1000, [
                'result' => 'retry_scheduled',
            ]);

            return;
        }

        if ($result->operatorId === null) {
            $this->metrics->increment('operator_search.exhausted', tags: [
                'final_status' => $result->finalStatus ?? 'unknown',
            ]);
            $this->metrics->timing('call_processing.duration_ms', (microtime(true) - $startedAt) * 1000, [
                'result' => 'exhausted',
            ]);

            return;
        }

        $this->logger->callAssignmentRequested($result->callId, $result->operatorId, $result->clientId);
        $this->metrics->increment('operator_assignment.requested');
        $this->metrics->timing('call_processing.duration_ms', (microtime(true) - $startedAt) * 1000, [
            'result' => 'assignment_requested',
        ]);
    }
}
