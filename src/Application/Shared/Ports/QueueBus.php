<?php

declare(strict_types=1);

namespace Application\Shared\Ports;

interface QueueBus
{
    /**
     * Dispatch an application command/job to the framework queue bus.
     */
    public function dispatch(object $message, ?string $queue = null): void;
}
