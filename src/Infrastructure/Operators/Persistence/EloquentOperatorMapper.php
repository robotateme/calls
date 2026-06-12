<?php

declare(strict_types=1);

namespace Infrastructure\Operators\Persistence;

use App\Models\Operator as OperatorRecord;
use Domain\Operators\OperatorId;
use Domain\Operators\OperatorReservation;

final readonly class EloquentOperatorMapper
{
    public function reservation(OperatorRecord $record): OperatorReservation
    {
        return new OperatorReservation(OperatorId::fromInt((int) $record->id));
    }
}
