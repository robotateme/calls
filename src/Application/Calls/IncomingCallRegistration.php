<?php

declare(strict_types=1);

namespace Application\Calls;

use Domain\Calls\CallHangupPolicy;
use Domain\Calls\CallStatus;
use Domain\Calls\ExternalCallId;
use Domain\Calls\OperatorSearchMaxAttempts;
use Domain\Calls\OperatorSearchRetryDelay;
use Domain\Calls\PhoneNumber;

final readonly class IncomingCallRegistration
{
    public function __construct(
        public ExternalCallId $externalCallId,
        public PhoneNumber $phone,
        public string $kafkaMessageId,
        public OperatorSearchMaxAttempts $operatorSearchMaxAttempts,
        public OperatorSearchRetryDelay $operatorSearchRetryDelay,
        public CallHangupPolicy $operatorSearchHangupPolicy,
    ) {}

    public function initialStatus(): CallStatus
    {
        return CallStatus::New;
    }
}
