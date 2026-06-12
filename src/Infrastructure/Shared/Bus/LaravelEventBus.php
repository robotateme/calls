<?php

declare(strict_types=1);

namespace Infrastructure\Shared\Bus;

use Application\Shared\Ports\EventBus;
use Domain\Shared\DomainEvent;
use Illuminate\Contracts\Events\Dispatcher;

final readonly class LaravelEventBus implements EventBus
{
    public function __construct(private Dispatcher $events) {}

    public function publish(DomainEvent $event): void
    {
        $this->events->dispatch($event);
    }

    public function publishMany(iterable $events): void
    {
        foreach ($events as $event) {
            $this->publish($event);
        }
    }
}
