<?php

declare(strict_types=1);

namespace Domain\Calls;

enum CallHangupPolicy: string
{
    case Missed = 'missed';
    case CallbackMissed = 'callback_missed';
    case HangupOnRetry = 'hangup_on_retry';

    public function finalStatus(): CallStatus
    {
        return match ($this) {
            self::Missed => CallStatus::Missed,
            self::CallbackMissed => CallStatus::CallbackMissed,
            self::HangupOnRetry => CallStatus::HangupOnRetry,
        };
    }
}
