<?php

declare(strict_types=1);

namespace Infrastructure\Calls\Persistence;

use App\Models\Call as CallRecord;
use Application\Calls\Ports\CallReadRepository;
use Application\Calls\Ports\CallWriteRepository;
use Domain\Calls\Call;
use Domain\Calls\CallHangupPolicy;
use Domain\Calls\CallStatus;
use Domain\Calls\ExternalCallId;
use Domain\Calls\OperatorSearchMaxAttempts;
use Domain\Calls\OperatorSearchRetryDelay;
use Domain\Calls\PhoneNumber;
use Domain\Shared\Timestamp;
use Illuminate\Support\Facades\DB;

final readonly class EloquentCallRepository implements CallReadRepository, CallWriteRepository
{
    public function __construct(private readonly EloquentCallMapper $mapper) {}

    public function createIncomingFromKafka(
        ExternalCallId $externalCallId,
        PhoneNumber $phone,
        string $kafkaMessageId,
        OperatorSearchMaxAttempts $operatorSearchMaxAttempts,
        OperatorSearchRetryDelay $operatorSearchRetryDelay,
        CallHangupPolicy $operatorSearchHangupPolicy,
    ): Call {
        $record = CallRecord::query()->create([
            'external_call_id' => $externalCallId->toString(),
            'phone' => $phone->toString(),
            'kafka_message_id' => $kafkaMessageId,
            'status' => CallStatus::New->value,
            'operator_search_max_attempts' => $operatorSearchMaxAttempts->toInt(),
            'operator_search_retry_delay_seconds' => $operatorSearchRetryDelay->seconds(),
            'operator_search_hangup_policy' => $operatorSearchHangupPolicy->value,
        ]);

        return $this->mapper->toDomain($record);
    }

    public function findByExternalCallId(ExternalCallId $externalCallId): ?Call
    {
        $record = CallRecord::query()
            ->where('external_call_id', $externalCallId->toString())
            ->first();

        return $record instanceof CallRecord ? $this->mapper->toDomain($record) : null;
    }

    public function findForUpdate(int $callId): ?Call
    {
        $record = CallRecord::query()
            ->whereKey($callId)
            ->lock($this->forUpdateLock())
            ->first();

        return $record instanceof CallRecord ? $this->mapper->toDomain($record) : null;
    }

    public function findForUpdateByExternalCallId(string $externalCallId): ?Call
    {
        $record = CallRecord::query()
            ->where('external_call_id', $externalCallId)
            ->lock($this->forUpdateLock())
            ->first();

        return $record instanceof CallRecord ? $this->mapper->toDomain($record) : null;
    }

    public function findExpiredAssignmentsForUpdate(Timestamp $expiredBefore, int $limit): array
    {
        $records = CallRecord::query()
            ->select('calls.*')
            ->join('operators', 'operators.id', '=', 'calls.operator_id')
            ->whereIn('calls.status', [
                CallStatus::AssignmentRequested->value,
                CallStatus::OperatorRinging->value,
            ])
            ->whereNotNull('calls.operator_id')
            ->whereColumn('operators.reserved_call_id', 'calls.id')
            ->where('operators.reserved_at', '<=', $expiredBefore->toDatabaseString())
            ->orderBy('operators.reserved_at')
            ->orderBy('calls.id')
            ->limit(max(1, $limit))
            ->lock($this->forUpdateLock())
            ->get();

        return array_values($records
            ->map(fn (CallRecord $record): Call => $this->mapper->toDomain($record))
            ->values()
            ->all());
    }

    public function save(Call $call): void
    {
        CallRecord::query()
            ->whereKey($call->id())
            ->update($this->mapper->toDatabase($call));
    }

    private function forUpdateLock(): string|bool
    {
        return in_array(DB::connection()->getDriverName(), ['mysql', 'pgsql'], true)
            ? 'FOR UPDATE SKIP LOCKED'
            : true;
    }
}
