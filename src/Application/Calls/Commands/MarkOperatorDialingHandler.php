<?php

declare(strict_types=1);

namespace Application\Calls\Commands;

use Application\Calls\Ports\CallWriteRepository;
use Application\Shared\Ports\Metrics;
use Application\Shared\Ports\TransactionManager;
use Domain\Operators\OperatorId;
use Domain\Shared\Timestamp;

final readonly class MarkOperatorDialingHandler
{
    public function __construct(
        private CallWriteRepository $calls,
        private TransactionManager $transactions,
        private Metrics $metrics,
    ) {}

    public function handle(MarkOperatorDialingFromKafkaCommand $command): void
    {
        $normalizedExternalCallId = trim($command->externalCallId);
        $operatorId = OperatorId::fromInt($command->operatorId);

        $transition = $this->transactions->run(function () use ($normalizedExternalCallId, $operatorId, $command): ?array {
            $call = $this->calls->findForUpdateByExternalCallId($normalizedExternalCallId);

            if ($call === null) {
                return null;
            }

            $previousStatus = $call->status()->value;

            if (! $call->markOperatorDialing(
                operatorId: $operatorId,
                attempt: $command->assignmentAttempt,
                dialingAt: Timestamp::now(),
            )) {
                return null;
            }

            $this->calls->save($call);

            return [
                'from' => $previousStatus,
                'to' => $call->status()->value,
            ];
        });

        if ($transition !== null) {
            $this->recordCallTransition($transition['from'], $transition['to']);
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
