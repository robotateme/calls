<?php

declare(strict_types=1);

namespace Domain\Calls;

enum CallStatus: string
{
    case New = 'new';
    case Waiting = 'waiting';
    case AssignmentRequested = 'assignment_requested';
    case OperatorDialing = 'operator_dialing';
    case Connected = 'connected';
    case Missed = 'missed';
    case CallbackMissed = 'callback_missed';
    case HangupOnRetry = 'hangup_on_retry';
}
