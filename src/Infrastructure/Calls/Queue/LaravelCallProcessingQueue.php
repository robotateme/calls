<?php

declare(strict_types=1);

namespace Infrastructure\Calls\Queue;

use App\Jobs\ProcessIncomingCallJob;
use Application\Calls\Ports\CallProcessingQueue;
use Application\Shared\Ports\QueueBus;

final readonly class LaravelCallProcessingQueue implements CallProcessingQueue
{
    public function __construct(private QueueBus $queue) {}

    public function enqueue(int $callId): void
    {
        $this->queue->dispatch(new ProcessIncomingCallJob($callId), 'calls');
    }
}
