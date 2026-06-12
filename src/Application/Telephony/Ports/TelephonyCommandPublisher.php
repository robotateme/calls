<?php

declare(strict_types=1);

namespace Application\Telephony\Ports;

use Domain\Telephony\TelephonyOutboxMessage;

interface TelephonyCommandPublisher
{
    public function publish(TelephonyOutboxMessage $message): void;
}
