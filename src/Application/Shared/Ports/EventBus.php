<?php

declare(strict_types=1);

namespace Application\Shared\Ports;

use Domain\Shared\DomainEvent;

interface EventBus
{
    public function publish(DomainEvent $event): void;

    /**
     * @param  iterable<DomainEvent>  $events
     */
    public function publishMany(iterable $events): void;
}
