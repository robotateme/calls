<?php

declare(strict_types=1);

namespace Infrastructure\Calls\Logging;

use Application\Calls\Ports\CallProcessingLogger;
use Illuminate\Support\Facades\Log;

final readonly class LaravelCallProcessingLogger implements CallProcessingLogger
{
    public function callAssignmentRequested(int $callId, int $operatorId, ?int $clientId): void
    {
        Log::info('Call assignment requested', [
            'call_id' => $callId,
            'operator_id' => $operatorId,
            'client_id' => $clientId,
        ]);
    }
}
