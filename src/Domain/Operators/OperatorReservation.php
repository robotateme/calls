<?php

declare(strict_types=1);

namespace Domain\Operators;

final readonly class OperatorReservation
{
    public function __construct(public OperatorId $operatorId) {}
}
