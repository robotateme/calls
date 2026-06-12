<?php

declare(strict_types=1);

namespace Application\Calls\Commands;

final readonly class PublishTelephonyOutboxResult
{
    public function __construct(
        public int $claimed,
        public int $published,
        public int $failed,
    ) {}
}
