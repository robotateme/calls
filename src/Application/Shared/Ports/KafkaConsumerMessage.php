<?php

declare(strict_types=1);

namespace Application\Shared\Ports;

final readonly class KafkaConsumerMessage
{
    public function __construct(
        public string $source,
        public string $topic,
        public ?int $partition,
        public ?int $offset,
        public ?string $key,
        public ?string $traceId,
        public string $payload,
    ) {}
}
