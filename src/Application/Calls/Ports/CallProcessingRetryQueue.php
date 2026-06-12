<?php

declare(strict_types=1);

namespace Application\Calls\Ports;

interface CallProcessingRetryQueue
{
    public function retryLater(int $callId, int $delaySeconds): void;
}
