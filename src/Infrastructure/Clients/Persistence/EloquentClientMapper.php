<?php

declare(strict_types=1);

namespace Infrastructure\Clients\Persistence;

use Domain\Clients\ClientId;

final readonly class EloquentClientMapper
{
    public function id(mixed $value): ClientId
    {
        return ClientId::fromInt((int) $value);
    }
}
