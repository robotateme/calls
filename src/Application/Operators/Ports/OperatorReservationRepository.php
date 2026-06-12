<?php

declare(strict_types=1);

namespace Application\Operators\Ports;

use Domain\Calls\CallId;
use Domain\Operators\OperatorId;
use Domain\Operators\OperatorReservation;

interface OperatorReservationRepository
{
    public function reserveAvailableForCall(CallId $callId): ?OperatorReservation;

    public function releaseForCall(OperatorId $operatorId, CallId $callId): void;
}
