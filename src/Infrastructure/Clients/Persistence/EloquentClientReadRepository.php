<?php

declare(strict_types=1);

namespace Infrastructure\Clients\Persistence;

use App\Models\Client;
use Application\Clients\Ports\ClientReadRepository;
use Domain\Calls\PhoneNumber;
use Domain\Clients\ClientId;

final readonly class EloquentClientReadRepository implements ClientReadRepository
{
    public function __construct(private EloquentClientMapper $mapper) {}

    public function findIdByPhone(PhoneNumber $phone): ?ClientId
    {
        $id = Client::query()
            ->where('phone', $phone->toString())
            ->value('id');

        return $id === null ? null : $this->mapper->id($id);
    }
}
