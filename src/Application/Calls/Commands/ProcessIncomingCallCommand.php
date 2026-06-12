<?php

declare(strict_types=1);

namespace Application\Calls\Commands;

final readonly class ProcessIncomingCallCommand
{
    public function __construct(public int $callId) {}
}
