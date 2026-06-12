<?php

declare(strict_types=1);

namespace Application\Calls\Ports;

use Domain\Calls\Call;
use Domain\Calls\CallHangupPolicy;
use Domain\Calls\ExternalCallId;
use Domain\Calls\OperatorSearchMaxAttempts;
use Domain\Calls\OperatorSearchRetryDelay;
use Domain\Calls\PhoneNumber;
use Domain\Shared\Timestamp;

interface CallWriteRepository
{
    public function createIncomingFromKafka(
        ExternalCallId $externalCallId,
        PhoneNumber $phone,
        string $kafkaMessageId,
        OperatorSearchMaxAttempts $operatorSearchMaxAttempts,
        OperatorSearchRetryDelay $operatorSearchRetryDelay,
        CallHangupPolicy $operatorSearchHangupPolicy,
    ): Call;

    public function findForUpdate(int $callId): ?Call;

    public function findForUpdateByExternalCallId(string $externalCallId): ?Call;

    /**
     * @return list<Call>
     */
    public function findExpiredAssignmentsForUpdate(Timestamp $expiredBefore, int $limit): array;

    public function save(Call $call): void;
}
