<?php

declare(strict_types=1);

namespace Infrastructure\Operators\Persistence;

use App\Models\Operator;
use Application\Operators\Ports\OperatorReservationRepository;
use Domain\Calls\CallId;
use Domain\Operators\OperatorId;
use Domain\Operators\OperatorReservation;
use Domain\Shared\Timestamp;

final readonly class EloquentOperatorReservationRepository implements OperatorReservationRepository
{
    public function __construct(private EloquentOperatorMapper $mapper) {}

    public function reserveAvailableForCall(CallId $callId): ?OperatorReservation
    {
        $operator = Operator::query()
            ->where('available', true)
            ->where('afk', false)
            ->whereNull('reserved_call_id')
            ->orderByRaw('last_call_at is not null')
            ->orderBy('last_call_at')
            ->orderBy('id')
            ->lockForUpdate()
            ->first();

        if (! $operator instanceof Operator) {
            return null;
        }

        $now = Timestamp::now()->toDatabaseString();

        $operator->forceFill([
            'reserved_call_id' => $callId->toInt(),
            'reserved_at' => $now,
            'last_call_at' => $now,
        ])->save();

        return $this->mapper->reservation($operator);
    }

    public function releaseForCall(OperatorId $operatorId, CallId $callId): void
    {
        Operator::query()
            ->whereKey($operatorId->toInt())
            ->where('reserved_call_id', $callId->toInt())
            ->update([
                'reserved_call_id' => null,
                'reserved_at' => null,
                'updated_at' => now(),
            ]);
    }
}
