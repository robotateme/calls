<?php

use Application\Calls\Commands\ConsumeKafkaCallFactsCommand;
use Application\Calls\Commands\ConsumeKafkaCallFactsHandler;
use Application\Calls\Commands\HandleKafkaCallFactCommand;
use Application\Calls\Commands\HandleKafkaCallFactHandler;
use Application\Calls\Commands\PublishTelephonyOutboxHandler;
use Application\Calls\Commands\RequeueStaleTelephonyOutboxHandler;
use Application\Calls\Commands\ReleaseExpiredOperatorReservationsHandler;
use Application\Shared\Ports\Metrics;
use Illuminate\Contracts\Queue\Factory as QueueFactory;
use Illuminate\Foundation\Inspiring;
use Illuminate\Support\Facades\Artisan;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use Symfony\Component\Console\Command\Command;

Artisan::command('inspire', function () {
    $this->comment(Inspiring::quote());
})->purpose('Display an inspiring quote');

Artisan::command('calls:telephony-outbox:publish
    {--limit= : Maximum records to publish}
    {--retry-delay= : Seconds before retrying failed records}
    {--max-attempts= : Attempts before marking a record failed}', function (PublishTelephonyOutboxHandler $handler): int {
    $result = $handler->handle(
        limit: (int) ($this->option('limit') ?? config('calls.outbox_publish_limit')),
        retryDelaySeconds: (int) ($this->option('retry-delay') ?? config('calls.outbox_retry_delay_seconds')),
        maxAttempts: (int) ($this->option('max-attempts') ?? config('calls.outbox_max_attempts')),
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
    {--limit= : Maximum records to requeue}', function (RequeueStaleTelephonyOutboxHandler $handler): int {
    $requeued = $handler->handle(
        olderThanSeconds: (int) ($this->option('older-than') ?? config('calls.outbox_processing_timeout_seconds')),
        limit: (int) ($this->option('limit') ?? config('calls.outbox_requeue_limit')),
    );

    $this->info(sprintf('Stale Telephony outbox records requeued: %d', $requeued));

    return Command::SUCCESS;
})->purpose('Requeue Telephony outbox records stuck in processing');

Artisan::command('calls:operator-reservations:release-expired
    {--older-than= : Reservation age in seconds before release}
    {--limit= : Maximum reservations to process}', function (ReleaseExpiredOperatorReservationsHandler $handler): int {
    $released = $handler->handle(
        olderThanSeconds: (int) ($this->option('older-than') ?? config('calls.operator_reservation_ttl_seconds')),
        limit: (int) ($this->option('limit') ?? config('calls.operator_reservation_cleanup_limit')),
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
    {--trace-id= : Trace id}', function (HandleKafkaCallFactHandler $handler): int {
    $stringArgument = function (string $name): string {
        $value = $this->argument($name);

        if (! is_scalar($value)) {
            throw new InvalidArgumentException(sprintf('Argument "%s" must be scalar.', $name));
        }

        return (string) $value;
    };
    $stringOption = function (string $name, ?string $default = null): ?string {
        $value = $this->option($name);

        if ($value === null) {
            return $default;
        }

        if (! is_scalar($value)) {
            throw new InvalidArgumentException(sprintf('Option "%s" must be scalar.', $name));
        }

        $value = trim((string) $value);

        return $value === '' ? $default : $value;
    };
    $intOption = function (string $name) use ($stringOption): ?int {
        $value = $stringOption($name);

        return $value === null ? null : (int) $value;
    };

    $handler->handle(new HandleKafkaCallFactCommand(
        source: $stringOption('source', 'calls-console-consumer') ?? 'calls-console-consumer',
        topic: $stringArgument('topic'),
        partition: $intOption('partition'),
        offset: $intOption('offset'),
        messageKey: $stringOption('key'),
        traceId: $stringOption('trace-id'),
        rawPayload: $stringArgument('payload'),
    ));

    $this->info('Kafka message handled.');

    return Command::SUCCESS;
})->purpose('Handle one Kafka call fact message through mapper, DLQ and application handlers');

Artisan::command('calls:kafka:consume
    {topic : Kafka topic}
    {--group=calls : Consumer group id}
    {--source=calls-jsonl-consumer : Consumer source name}
    {--limit=100 : Maximum records to consume}
    {--timeout-ms=1000 : Idle timeout in milliseconds}', function (ConsumeKafkaCallFactsHandler $handler): int {
    $stringArgument = function (string $name): string {
        $value = $this->argument($name);

        if (! is_scalar($value)) {
            throw new InvalidArgumentException(sprintf('Argument "%s" must be scalar.', $name));
        }

        return (string) $value;
    };
    $stringOption = function (string $name, string $default): string {
        $value = $this->option($name);

        if ($value === null) {
            return $default;
        }

        if (! is_scalar($value)) {
            throw new InvalidArgumentException(sprintf('Option "%s" must be scalar.', $name));
        }

        $value = trim((string) $value);

        return $value === '' ? $default : $value;
    };

    $consumed = $handler->handle(new ConsumeKafkaCallFactsCommand(
        topic: $stringArgument('topic'),
        groupId: $stringOption('group', 'calls'),
        source: $stringOption('source', 'calls-jsonl-consumer'),
        limit: max(1, (int) $stringOption('limit', '100')),
        timeoutMs: max(1, (int) $stringOption('timeout-ms', '1000')),
    ));

    $this->info(sprintf('Kafka consumer processed records: %d', $consumed));

    return Command::SUCCESS;
})->purpose('Consume Kafka call facts through the configured transport adapter');

Artisan::command('calls:dead-letter:list
    {--reason= : Filter by reason}
    {--include-resolved : Include resolved records}
    {--limit=50 : Maximum records to show}', function (): int {
    if (! Schema::hasTable('dead_letter_messages')) {
        $this->warn('dead_letter_messages table does not exist.');

        return Command::SUCCESS;
    }

    $limit = max(1, (int) ($this->option('limit') ?? 50));
    $query = DB::table('dead_letter_messages')
        ->select(['id', 'source', 'topic', 'message_partition', 'message_offset', 'message_key', 'trace_id', 'reason', 'resolved_at', 'created_at'])
        ->orderByDesc('id')
        ->limit($limit);
    $reason = $this->option('reason');

    if (is_scalar($reason) && trim((string) $reason) !== '') {
        $query->where('reason', trim((string) $reason));
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
    {--note= : Resolution note}', function (): int {
    $id = $this->argument('id');

    if (! is_scalar($id)) {
        throw new InvalidArgumentException('Dead letter id must be scalar.');
    }

    $note = $this->option('note');
    $updated = DB::table('dead_letter_messages')
        ->where('id', (int) $id)
        ->whereNull('resolved_at')
        ->update([
            'resolved_at' => now(),
            'resolution_note' => is_scalar($note) ? trim((string) $note) : null,
        ]);

    $this->info(sprintf('Dead letter records resolved: %d', $updated));

    return Command::SUCCESS;
})->purpose('Mark dead letter record as resolved');

Artisan::command('calls:dead-letter:prune-resolved
    {--older-than-days= : Resolved records retention in days}
    {--limit= : Maximum records to delete}', function (): int {
    if (! Schema::hasTable('dead_letter_messages')) {
        $this->warn('dead_letter_messages table does not exist.');

        return Command::SUCCESS;
    }

    $olderThanDays = (int) ($this->option('older-than-days') ?? config('calls.dead_letter_retention_days'));
    $limit = (int) ($this->option('limit') ?? config('calls.dead_letter_prune_limit'));
    $ids = DB::table('dead_letter_messages')
        ->whereNotNull('resolved_at')
        ->where('resolved_at', '<=', now()->subDays(max(1, $olderThanDays)))
        ->orderBy('id')
        ->limit(max(1, $limit))
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

Artisan::command('calls:metrics:snapshot', function (Metrics $metrics, QueueFactory $queues): int {
    if (Schema::hasTable('calls')) {
        foreach (DB::table('calls')->select('status', DB::raw('count(*) as total'))->groupBy('status')->get() as $row) {
            $metrics->gauge('calls.depth', (int) $row->total, [
                'status' => (string) $row->status,
            ]);
        }
    }

    if (Schema::hasTable('telephony_outbox')) {
        foreach (DB::table('telephony_outbox')->select('status', DB::raw('count(*) as total'))->groupBy('status')->get() as $row) {
            $metrics->gauge('telephony_outbox.depth', (int) $row->total, [
                'status' => (string) $row->status,
            ]);
        }
    }

    if (Schema::hasTable('dead_letter_messages')) {
        foreach (DB::table('dead_letter_messages')->select('reason', DB::raw('count(*) as total'))->whereNull('resolved_at')->groupBy('reason')->get() as $row) {
            $metrics->gauge('dead_letter.depth', (int) $row->total, [
                'reason' => (string) $row->reason,
            ]);
        }
    }

    if (Schema::hasTable('operators')) {
        $metrics->gauge('operator_reservation.active', (int) DB::table('operators')->whereNotNull('reserved_call_id')->count());
        $metrics->gauge('operator_reservation.expired', (int) DB::table('operators')
            ->whereNotNull('reserved_call_id')
            ->where('reserved_at', '<=', now()->subSeconds((int) config('calls.operator_reservation_ttl_seconds')))
            ->count());
    }

    $connection = $queues->connection();
    $metrics->gauge('queue.depth', (int) $connection->size('calls'), ['queue' => 'calls']);
    $metrics->gauge('queue.depth', (int) $connection->size('calls-retry'), ['queue' => 'calls-retry']);

    $this->info('Operational metrics snapshot recorded.');

    return Command::SUCCESS;
})->purpose('Record operational gauge metrics for calls, outbox, reservations and queues');
