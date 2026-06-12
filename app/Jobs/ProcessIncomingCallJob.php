<?php

declare(strict_types=1);

namespace App\Jobs;

use Application\Calls\Commands\ProcessIncomingCallCommand;
use Application\Calls\Commands\ProcessIncomingCallHandler;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Queue\Queueable;

final class ProcessIncomingCallJob implements ShouldQueue
{
    use Queueable;

    public int $tries = 5;

    public int $backoff = 10;

    public function __construct(private readonly int $callId) {}

    public function handle(ProcessIncomingCallHandler $handler): void
    {
        $handler->handle(new ProcessIncomingCallCommand($this->callId));
    }
}
