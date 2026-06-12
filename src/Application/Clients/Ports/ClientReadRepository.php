<?php

declare(strict_types=1);

namespace Application\Clients\Ports;

use Domain\Calls\PhoneNumber;
use Domain\Clients\ClientId;

interface ClientReadRepository
{
    public function findIdByPhone(PhoneNumber $phone): ?ClientId;
}
