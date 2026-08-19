<?php

use Application\Calls\Commands\ConsumeKafkaCallFactsCommand;
use Application\Calls\Commands\ConsumeKafkaCallFactsHandler;
use Application\Calls\Commands\HandleKafkaCallFactCommand;
use Application\Calls\Commands\HandleKafkaCallFactHandler;
use Application\Calls\Commands\PublishTelephonyOutboxHandler;
use Application\Calls\Commands\ReleaseExpiredOperatorReservationsHandler;
use Application\Calls\Commands\RequeueStaleTelephonyOutboxHandler;
use Application\Shared\Ports\Metrics;
use Illuminate\Contracts\Queue\Factory as QueueFactory;
use Illuminate\Foundation\Inspiring;
use Illuminate\Support\Carbon;
use Illuminate\Support\Facades\Artisan;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use Infrastructure\Shared\Observability\PrometheusMetricsStore;
use Symfony\Component\Console\Command\Command;

$requireConsoleString = static function (mixed $value, string $name, string $source): string {
    if (! is_scalar($value)) {
        throw new InvalidArgumentException(sprintf('%s "%s" must be scalar.', $source, $name));
    }

    return (string) $value;
};
$optionalConsoleString = static function (mixed $value, string $name, ?string $default = null): ?string {
    if ($value === null) {
        return $default;
    }

    if (! is_scalar($value)) {
        throw new InvalidArgumentException(sprintf('Option "%s" must be scalar.', $name));
    }

    $normalizedValue = trim((string) $value);

    return $normalizedValue === '' ? $default : $normalizedValue;
};
$optionalConsoleInt = static function (mixed $value, string $name, ?int $default = null) use ($optionalConsoleString): ?int {
    $normalizedValue = $optionalConsoleString($value, $name);

    return $normalizedValue === null ? $default : (int) $normalizedValue;
};
$positiveConsoleInt = static fn (int $value): int => max(1, $value);

Artisan::command('inspire', function (): void {
    $this->comment(Inspiring::quote());
})->purpose('Display an inspiring quote');

Artisan::command('calls:telephony-outbox:publish
    {--limit= : Maximum records to publish}
    {--retry-delay= : Seconds before retrying failed records}
    {--max-attempts= : Attempts before marking a record failed}', function (PublishTelephonyOutboxHandler $handler) use ($optionalConsoleInt): int {
    $configuredLimit = (int) config('calls.outbox_publish_limit');
    $configuredRetryDelaySeconds = (int) config('calls.outbox_retry_delay_seconds');
    $configuredMaxAttempts = (int) config('calls.outbox_max_attempts');
    $publishLimit = $optionalConsoleInt($this->option('limit'), 'limit', $configuredLimit) ?? $configuredLimit;
    $retryDelaySeconds = $optionalConsoleInt($this->option('retry-delay'), 'retry-delay', $configuredRetryDelaySeconds) ?? $configuredRetryDelaySeconds;
    $maxAttempts = $optionalConsoleInt($this->option('max-attempts'), 'max-attempts', $configuredMaxAttempts) ?? $configuredMaxAttempts;

    $result = $handler->handle(
        limit: $publishLimit,
        retryDelaySeconds: $retryDelaySeconds,
        maxAttempts: $maxAttempts,
    );

    $this->info(sprintf(
        'Telephony outbox: claimed=%d published=%d failed=%d',
        $result->claimed,
        $result->published,
        $result->failed,
    ));

    return Command::SUCCESS;
})->purpose('Publish pending Telephony outbox commands');

Artisan::command('calls:telephony-outbox:requeue-stale
    {--older-than= : Processing age in seconds before requeue}
    {--limit= : Maximum records to requeue}', function (RequeueStaleTelephonyOutboxHandler $handler) use ($optionalConsoleInt): int {
    $configuredProcessingTimeoutSeconds = (int) config('calls.outbox_processing_timeout_seconds');
    $configuredRequeueLimit = (int) config('calls.outbox_requeue_limit');
    $processingTimeoutSeconds = $optionalConsoleInt($this->option('older-than'), 'older-than', $configuredProcessingTimeoutSeconds) ?? $configuredProcessingTimeoutSeconds;
    $requeueLimit = $optionalConsoleInt($this->option('limit'), 'limit', $configuredRequeueLimit) ?? $configuredRequeueLimit;

    $requeued = $handler->handle(
        olderThanSeconds: $processingTimeoutSeconds,
        limit: $requeueLimit,
    );

    $this->info(sprintf('Stale Telephony outbox records requeued: %d', $requeued));

    return Command::SUCCESS;
})->purpose('Requeue Telephony outbox records stuck in processing');

Artisan::command('calls:operator-reservations:release-expired
    {--older-than= : Reservation age in seconds before release}
    {--limit= : Maximum reservations to process}', function (ReleaseExpiredOperatorReservationsHandler $handler) use ($optionalConsoleInt): int {
    $configuredReservationTtlSeconds = (int) config('calls.operator_reservation_ttl_seconds');
    $configuredCleanupLimit = (int) config('calls.operator_reservation_cleanup_limit');
    $reservationTtlSeconds = $optionalConsoleInt($this->option('older-than'), 'older-than', $configuredReservationTtlSeconds) ?? $configuredReservationTtlSeconds;
    $cleanupLimit = $optionalConsoleInt($this->option('limit'), 'limit', $configuredCleanupLimit) ?? $configuredCleanupLimit;

    $released = $handler->handle(
        olderThanSeconds: $reservationTtlSeconds,
        limit: $cleanupLimit,
    );

    $this->info(sprintf('Expired operator reservations released: %d', $released));

    return Command::SUCCESS;
})->purpose('Release expired operator reservations and continue call retry/finalization flow');

Artisan::command('calls:kafka:handle-message
    {topic : Kafka topic}
    {payload : Raw JSON payload}
    {--source=calls-console-consumer : Consumer source name}
    {--partition= : Kafka partition}
    {--offset= : Kafka offset}
    {--key= : Kafka message key}
    {--trace-id= : Trace id}', function (HandleKafkaCallFactHandler $handler) use ($requireConsoleString, $optionalConsoleString, $optionalConsoleInt): int {
    $topic = $requireConsoleString($this->argument('topic'), 'topic', 'Argument');
    $rawPayload = $requireConsoleString($this->argument('payload'), 'payload', 'Argument');
    $source = $optionalConsoleString($this->option('source'), 'source', 'calls-console-consumer') ?? 'calls-console-consumer';
    $partition = $optionalConsoleInt($this->option('partition'), 'partition');
    $offset = $optionalConsoleInt($this->option('offset'), 'offset');
    $messageKey = $optionalConsoleString($this->option('key'), 'key');
    $traceId = $optionalConsoleString($this->option('trace-id'), 'trace-id');

    $handler->handle(new HandleKafkaCallFactCommand(
        source: $source,
        topic: $topic,
        partition: $partition,
        offset: $offset,
        messageKey: $messageKey,
        traceId: $traceId,
        rawPayload: $rawPayload,
    ));

    $this->info('Kafka message handled.');

    return Command::SUCCESS;
})->purpose('Handle one Kafka call fact message through mapper, DLQ and application handlers');

Artisan::command('calls:kafka:consume
    {topic : Kafka topic}
    {--group=calls : Consumer group id}
    {--source=calls-jsonl-consumer : Consumer source name}
    {--limit=100 : Maximum records to consume}
    {--timeout-ms=1000 : Idle timeout in milliseconds}', function (ConsumeKafkaCallFactsHandler $handler, Metrics $metrics) use ($requireConsoleString, $optionalConsoleString, $optionalConsoleInt, $positiveConsoleInt): int {
    $topic = $requireConsoleString($this->argument('topic'), 'topic', 'Argument');
    $groupId = $optionalConsoleString($this->option('group'), 'group', 'calls') ?? 'calls';
    $source = $optionalConsoleString($this->option('source'), 'source', 'calls-jsonl-consumer') ?? 'calls-jsonl-consumer';
    $consumeLimit = $positiveConsoleInt($optionalConsoleInt($this->option('limit'), 'limit', 100) ?? 100);
    $consumeTimeoutMs = $positiveConsoleInt($optionalConsoleInt($this->option('timeout-ms'), 'timeout-ms', 1000) ?? 1000);

    try {
        $consumed = $handler->handle(new ConsumeKafkaCallFactsCommand(
            topic: $topic,
            groupId: $groupId,
            source: $source,
            limit: $consumeLimit,
            timeoutMs: $consumeTimeoutMs,
        ));
    } catch (Throwable $exception) {
        $metrics->increment('kafka_consumer_failures_total', tags: [
            'source' => $source,
            'topic' => $topic,
            'reason' => 'consumer_failed',
        ]);

        $this->error(sprintf('Kafka consumer failed: %s', $exception->getMessage()));

        return Command::FAILURE;
    }

    $this->info(sprintf('Kafka consumer processed records: %d', $consumed));

    return Command::SUCCESS;
})->purpose('Consume Kafka call facts through the configured transport adapter');

Artisan::command('calls:dead-letter:list
    {--reason= : Filter by reason}
    {--include-resolved : Include resolved records}
    {--limit=50 : Maximum records to show}', function () use ($optionalConsoleString, $optionalConsoleInt, $positiveConsoleInt): int {
    if (! Schema::hasTable('dead_letter_messages')) {
        $this->warn('dead_letter_messages table does not exist.');

        return Command::SUCCESS;
    }

    $listLimit = $positiveConsoleInt($optionalConsoleInt($this->option('limit'), 'limit', 50) ?? 50);
    $reason = $optionalConsoleString($this->option('reason'), 'reason');
    $query = DB::table('dead_letter_messages')
        ->select(['id', 'source', 'topic', 'message_partition', 'message_offset', 'message_key', 'trace_id', 'reason', 'resolved_at', 'created_at'])
        ->orderByDesc('id')
        ->limit($listLimit);

    if ($reason !== null) {
        $query->where('reason', $reason);
    }

    if ($this->option('include-resolved') !== true) {
        $query->whereNull('resolved_at');
    }

    $rows = $query->get()->map(static fn (object $row): array => [
        'id' => (string) $row->id,
        'source' => (string) $row->source,
        'topic' => (string) $row->topic,
        'partition' => $row->message_partition === null ? '' : (string) $row->message_partition,
        'offset' => $row->message_offset === null ? '' : (string) $row->message_offset,
        'key' => (string) ($row->message_key ?? ''),
        'trace_id' => (string) ($row->trace_id ?? ''),
        'reason' => (string) $row->reason,
        'resolved_at' => (string) ($row->resolved_at ?? ''),
        'created_at' => (string) ($row->created_at ?? ''),
    ])->all();

    $this->table(
        ['id', 'source', 'topic', 'partition', 'offset', 'key', 'trace_id', 'reason', 'resolved_at', 'created_at'],
        $rows,
    );

    return Command::SUCCESS;
})->purpose('List dead letter records');

Artisan::command('calls:dead-letter:resolve
    {id : Dead letter id}
    {--note= : Resolution note}', function () use ($requireConsoleString, $optionalConsoleString): int {
    $deadLetterId = (int) $requireConsoleString($this->argument('id'), 'id', 'Argument');
    $resolutionNote = $optionalConsoleString($this->option('note'), 'note');
    $updated = DB::table('dead_letter_messages')
        ->where('id', $deadLetterId)
        ->whereNull('resolved_at')
        ->update([
            'resolved_at' => now(),
            'resolution_note' => $resolutionNote,
        ]);

    $this->info(sprintf('Dead letter records resolved: %d', $updated));

    return Command::SUCCESS;
})->purpose('Mark dead letter record as resolved');

Artisan::command('calls:dead-letter:prune-resolved
    {--older-than-days= : Resolved records retention in days}
    {--limit= : Maximum records to delete}', function () use ($optionalConsoleInt, $positiveConsoleInt): int {
    if (! Schema::hasTable('dead_letter_messages')) {
        $this->warn('dead_letter_messages table does not exist.');

        return Command::SUCCESS;
    }

    $configuredRetentionDays = (int) config('calls.dead_letter_retention_days');
    $configuredPruneLimit = (int) config('calls.dead_letter_prune_limit');
    $retentionDays = $positiveConsoleInt($optionalConsoleInt($this->option('older-than-days'), 'older-than-days', $configuredRetentionDays) ?? $configuredRetentionDays);
    $pruneLimit = $positiveConsoleInt($optionalConsoleInt($this->option('limit'), 'limit', $configuredPruneLimit) ?? $configuredPruneLimit);
    $ids = DB::table('dead_letter_messages')
        ->whereNotNull('resolved_at')
        ->where('resolved_at', '<=', now()->subDays($retentionDays))
        ->orderBy('id')
        ->limit($pruneLimit)
        ->pluck('id')
        ->all();

    if ($ids === []) {
        $this->info('Resolved dead letter records pruned: 0');

        return Command::SUCCESS;
    }

    $deleted = DB::table('dead_letter_messages')->whereIn('id', $ids)->delete();

    $this->info(sprintf('Resolved dead letter records pruned: %d', $deleted));

    return Command::SUCCESS;
})->purpose('Prune resolved dead letter records after retention period');

Artisan::command('calls:metrics:snapshot', function (Metrics $metrics, QueueFactory $queues, PrometheusMetricsStore $prometheusMetricsStore): int {
    $reservationTtlSeconds = (int) config('calls.operator_reservation_ttl_seconds');
    $callCurrentStatuses = ['new', 'waiting', 'assignment_requested', 'operator_dialing', 'connected'];
    $telephonyOutboxStatuses = ['pending', 'processing', 'published', 'failed'];
    $secondsSince = static function (mixed $timestamp): int {
        if ($timestamp === null || $timestamp === '') {
            return 0;
        }

        try {
            return max(0, (int) Carbon::parse((string) $timestamp)->diffInSeconds(now(), true));
        } catch (Throwable) {
            return 0;
        }
    };

    foreach ([
        'calls.depth',
        'calls_current',
        'call_time_in_state_seconds',
        'oldest_waiting_call_age_seconds',
        'telephony_outbox.depth',
        'telephony_outbox_current',
        'telephony_outbox_oldest_pending_seconds',
        'oldest_outbox_message_age_seconds',
        'dead_letter.depth',
        'dead_letter_messages_current',
        'dead_letter_current',
        'operator_reservation.active',
        'operator_reservation.expired',
        'operator_reservations_current',
        'operators_reserved_current',
        'queue.depth',
    ] as $metricName) {
        $prometheusMetricsStore->forgetGaugeSeries($metricName);
    }

    if (Schema::hasTable('calls')) {
        $callCounts = DB::table('calls')->select('status', DB::raw('count(*) as total'))->groupBy('status')->get();

        foreach ($callCounts as $row) {
            $metrics->gauge('calls.depth', (int) $row->total, [
                'status' => (string) $row->status,
            ]);
        }

        $currentCounts = array_fill_keys($callCurrentStatuses, 0);

        foreach ($callCounts as $row) {
            $status = (string) $row->status;

            if (array_key_exists($status, $currentCounts)) {
                $currentCounts[$status] = (int) $row->total;
            }
        }

        foreach ($currentCounts as $status => $total) {
            $metrics->gauge('calls_current', $total, [
                'status' => $status,
            ]);
        }

        $stateAges = array_fill_keys($callCurrentStatuses, 0);

        foreach (DB::table('calls')
            ->select('status', DB::raw("min(coalesce(case status when 'new' then created_at when 'waiting' then updated_at when 'assignment_requested' then assignment_requested_at when 'operator_dialing' then operator_dialing_at when 'connected' then connected_at end, updated_at, created_at)) as state_started_at"))
            ->whereIn('status', $callCurrentStatuses)
            ->groupBy('status')
            ->get() as $row) {
            $stateAges[(string) $row->status] = $secondsSince($row->state_started_at ?? null);
        }

        foreach ($stateAges as $status => $ageSeconds) {
            $metrics->gauge('call_time_in_state_seconds', $ageSeconds, [
                'status' => $status,
            ]);
        }

        $metrics->gauge('oldest_waiting_call_age_seconds', $stateAges['waiting']);
    }

    if (Schema::hasTable('telephony_outbox')) {
        $outboxCounts = DB::table('telephony_outbox')->select('status', DB::raw('count(*) as total'))->groupBy('status')->get();

        foreach ($outboxCounts as $row) {
            $metrics->gauge('telephony_outbox.depth', (int) $row->total, [
                'status' => (string) $row->status,
            ]);
        }

        $currentOutboxCounts = array_fill_keys($telephonyOutboxStatuses, 0);

        foreach ($outboxCounts as $row) {
            $status = (string) $row->status;

            if (array_key_exists($status, $currentOutboxCounts)) {
                $currentOutboxCounts[$status] = (int) $row->total;
            }
        }

        foreach ($currentOutboxCounts as $status => $total) {
            $metrics->gauge('telephony_outbox_current', $total, [
                'status' => $status,
            ]);
        }

        $oldestPendingCreatedAt = DB::table('telephony_outbox')
            ->where('status', 'pending')
            ->whereNull('canceled_at')
            ->min('created_at');

        $oldestPendingOutboxAgeSeconds = $secondsSince($oldestPendingCreatedAt);

        $metrics->gauge('telephony_outbox_oldest_pending_seconds', $oldestPendingOutboxAgeSeconds);
        $metrics->gauge('oldest_outbox_message_age_seconds', $oldestPendingOutboxAgeSeconds);
    }

    if (Schema::hasTable('dead_letter_messages')) {
        $deadLetterTotal = 0;

        foreach (DB::table('dead_letter_messages')->select('reason', DB::raw('count(*) as total'))->whereNull('resolved_at')->groupBy('reason')->get() as $row) {
            $reasonTotal = (int) $row->total;
            $deadLetterTotal += $reasonTotal;

            $metrics->gauge('dead_letter.depth', $reasonTotal, [
                'reason' => (string) $row->reason,
            ]);
            $metrics->gauge('dead_letter_messages_current', $reasonTotal, [
                'reason' => (string) $row->reason,
            ]);
        }

        $metrics->gauge('dead_letter_current', $deadLetterTotal);
    }

    if (Schema::hasTable('operators')) {
        $activeReservations = (int) DB::table('operators')->whereNotNull('reserved_call_id')->count();

        $metrics->gauge('operator_reservation.active', $activeReservations);
        $metrics->gauge('operator_reservations_current', $activeReservations);
        $metrics->gauge('operators_reserved_current', $activeReservations);
        $metrics->gauge('operator_reservation.expired', (int) DB::table('operators')
            ->whereNotNull('reserved_call_id')
            ->where('reserved_at', '<=', now()->subSeconds($reservationTtlSeconds))
            ->count());
    }

    $connection = $queues->connection();
    $metrics->gauge('queue.depth', (int) $connection->size('calls'), ['queue' => 'calls']);
    $metrics->gauge('queue.depth', (int) $connection->size('calls-retry'), ['queue' => 'calls-retry']);

    $this->info('Operational metrics snapshot recorded.');

    return Command::SUCCESS;
})->purpose('Record operational gauge metrics for calls, outbox, reservations and queues');
