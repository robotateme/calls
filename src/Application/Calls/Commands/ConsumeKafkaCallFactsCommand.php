<?php

declare(strict_types=1);

namespace Application\Calls\Commands;

final readonly class ConsumeKafkaCallFactsCommand
{
    public function __construct(
        public string $topic,
        public string $groupId,
        public string $source,
        public int $limit,
        public int $timeoutMs,
    ) {}
}
