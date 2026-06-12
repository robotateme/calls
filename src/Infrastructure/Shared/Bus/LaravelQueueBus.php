<?php

declare(strict_types=1);

namespace Infrastructure\Shared\Bus;

use Application\Shared\Ports\QueueBus;
use Illuminate\Contracts\Bus\Dispatcher;

final readonly class LaravelQueueBus implements QueueBus
{
    public function __construct(private Dispatcher $bus) {}

    public function dispatch(object $message, ?string $queue = null): void
    {
        if ($queue !== null && method_exists($message, 'onQueue')) {
            $message->onQueue($queue);
        }

        $this->bus->dispatch($message);
    }
}
