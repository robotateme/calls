<?php

declare(strict_types=1);

namespace Application\Telephony\Ports;

interface TelephonyCommandOutboxReader
{
    public function hasPublishedAssignmentRequest(string $externalCallId): bool;
}
