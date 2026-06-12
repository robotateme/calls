<?php

declare(strict_types=1);

namespace Application\Calls\Ports;

interface CallProcessingLogger
{
    public function callAssignmentRequested(int $callId, int $operatorId, ?int $clientId): void;
}
