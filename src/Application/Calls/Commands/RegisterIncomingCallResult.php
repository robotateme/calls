<?php

declare(strict_types=1);

namespace Application\Calls\Commands;

final readonly class RegisterIncomingCallResult
{
    public function __construct(
        public int $callId,
        public bool $created,
    ) {}
}
