<?php

declare(strict_types=1);

namespace Application\Shared\Ports;

interface KafkaConsumer
{
    /**
     * @param  callable(KafkaConsumerMessage): void  $handler
     */
    public function consume(
        string $topic,
        string $groupId,
        string $source,
        int $limit,
        int $timeoutMs,
        callable $handler,
    ): int;
}
