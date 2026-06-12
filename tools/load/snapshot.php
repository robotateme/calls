<?php

declare(strict_types=1);

use Illuminate\Contracts\Console\Kernel;
use Illuminate\Contracts\Queue\Factory as QueueFactory;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

require dirname(__DIR__, 2).'/vendor/autoload.php';

$app = require dirname(__DIR__, 2).'/bootstrap/app.php';
$app->make(Kernel::class)->bootstrap();

$queues = $app->make(QueueFactory::class)->connection();

$countsBy = static function (string $table, string $column, ?callable $scope = null): array {
    if (! Schema::hasTable($table)) {
        return [];
    }

    $query = DB::table($table)->select($column, DB::raw('count(*) as total'))->groupBy($column);

    if ($scope !== null) {
        $scope($query);
    }

    return $query
        ->pluck('total', $column)
        ->map(static fn (mixed $count): int => (int) $count)
        ->all();
};

$queueDepth = [
    'calls' => (int) $queues->size('calls'),
    'calls-retry' => (int) $queues->size('calls-retry'),
];

$outboxByStatus = $countsBy('telephony_outbox', 'status');
$deadLettersByReason = $countsBy('dead_letter_messages', 'reason', static function ($query): void {
    $query->whereNull('resolved_at');
});

$snapshot = [
    'captured_at' => now()->toISOString(),
    'queue_depth' => $queueDepth,
    'calls_by_status' => $countsBy('calls', 'status'),
    'telephony_outbox_by_status' => $outboxByStatus,
    'dead_letters_by_reason' => $deadLettersByReason,
    'operators' => Schema::hasTable('operators') ? [
        'total' => (int) DB::table('operators')->count(),
        'reserved' => (int) DB::table('operators')->whereNotNull('reserved_call_id')->count(),
        'available_unreserved' => (int) DB::table('operators')
            ->where('available', true)
            ->where('afk', false)
            ->whereNull('reserved_call_id')
            ->count(),
    ] : [],
];

$queueWork = array_sum($queueDepth);
$outboxWork = (int) ($outboxByStatus['pending'] ?? 0) + (int) ($outboxByStatus['processing'] ?? 0);
$deadLetterCount = array_sum($deadLettersByReason);

if (in_array('--queue-work', $argv, true)) {
    echo $queueWork.PHP_EOL;

    return;
}

if (in_array('--outbox-work', $argv, true)) {
    echo $outboxWork.PHP_EOL;

    return;
}

if (in_array('--dead-letters', $argv, true)) {
    echo $deadLetterCount.PHP_EOL;

    return;
}

echo json_encode($snapshot, JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT).PHP_EOL;
