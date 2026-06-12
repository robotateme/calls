<?php

declare(strict_types=1);

namespace Application\Calls\Commands;

final readonly class HandleKafkaCallFactCommand
{
    public function __construct(
        public string $source,
        public string $topic,
        public ?int $partition,
        public ?int $offset,
        public ?string $messageKey,
        public ?string $traceId,
        public string $rawPayload,
    ) {}
}
