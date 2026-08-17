<?php

declare(strict_types=1);

namespace Application\Calls\Ports;

use Application\Calls\IncomingCallRegistration;
use Domain\Calls\Call;
use Domain\Shared\Timestamp;

interface CallWriteRepository
{
    public function createIncoming(IncomingCallRegistration $registration): Call;

    public function findForUpdate(int $callId): ?Call;

    public function findForUpdateByExternalCallId(string $externalCallId): ?Call;

    /**
     * @return list<Call>
     */
    public function findExpiredAssignmentsForUpdate(Timestamp $expiredBefore, int $limit): array;

    public function save(Call $call): void;
}
