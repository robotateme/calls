<?php

declare(strict_types=1);

namespace Application\Calls\Ports;

use Domain\Calls\Call;
use Domain\Calls\ExternalCallId;

interface CallReadRepository
{
    public function findByExternalCallId(ExternalCallId $externalCallId): ?Call;
}
