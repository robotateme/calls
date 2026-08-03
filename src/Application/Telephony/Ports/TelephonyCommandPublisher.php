<?php

declare(strict_types=1);

namespace Application\Telephony\Ports;

use Domain\Telephony\TelephonyOutboxMessage;

interface TelephonyCommandPublisher
{
    /**
     * @throws \JsonException
     * @throws \RuntimeException
     */
    public function publish(TelephonyOutboxMessage $message): void;
}
