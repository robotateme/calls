<?php

declare(strict_types=1);

namespace Application\Calls\Commands;

final readonly class RegisterIncomingCallFromKafkaCommand
{
    public function __construct(
        public string $externalCallId,
        public string $phone,
        public string $kafkaMessageId,
        public int $operatorSearchMaxAttempts = 1,
        public int $operatorSearchRetryDelaySeconds = 0,
        public string $operatorSearchHangupPolicy = 'missed',
    ) {}
}
