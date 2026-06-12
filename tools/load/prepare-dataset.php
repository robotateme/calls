<?php

declare(strict_types=1);

use Illuminate\Contracts\Console\Kernel;
use Illuminate\Support\Facades\DB;

require dirname(__DIR__, 2).'/vendor/autoload.php';

$app = require dirname(__DIR__, 2).'/bootstrap/app.php';
$app->make(Kernel::class)->bootstrap();

$operators = max(0, (int) ($argv[1] ?? getenv('LOAD_OPERATORS') ?: 100));
$clients = max(0, (int) ($argv[2] ?? getenv('LOAD_CLIENTS') ?: 0));
$chunkSize = 1000;

DB::table('operators')
    ->where('name', 'like', 'load-operator-%')
    ->delete();

for ($offset = 0; $offset < $operators; $offset += $chunkSize) {
    $rows = [];
    $limit = min($chunkSize, $operators - $offset);

    for ($i = 1; $i <= $limit; $i++) {
        $number = $offset + $i;
        $rows[] = [
            'name' => sprintf('load-operator-%06d', $number),
            'available' => true,
            'afk' => false,
            'reserved_call_id' => null,
            'reserved_at' => null,
            'last_call_at' => null,
            'created_at' => now(),
            'updated_at' => now(),
        ];
    }

    DB::table('operators')->insert($rows);
}

if ($clients > 0) {
    DB::table('clients')
        ->where('phone', 'like', '+1555%')
        ->delete();

    for ($offset = 0; $offset < $clients; $offset += $chunkSize) {
        $rows = [];
        $limit = min($chunkSize, $clients - $offset);

        for ($i = 1; $i <= $limit; $i++) {
            $number = $offset + $i;
            $rows[] = [
                'phone' => sprintf('+1555%07d', $number),
                'created_at' => now(),
                'updated_at' => now(),
            ];
        }

        DB::table('clients')->insert($rows);
    }
}

printf("Prepared load dataset: operators=%d clients=%d\n", $operators, $clients);
