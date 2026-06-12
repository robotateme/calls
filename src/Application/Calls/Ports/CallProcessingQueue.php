<?php

declare(strict_types=1);

namespace Application\Calls\Ports;

interface CallProcessingQueue
{
    public function enqueue(int $callId): void;
}
