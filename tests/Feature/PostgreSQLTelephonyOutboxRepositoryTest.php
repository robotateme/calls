<?php

declare(strict_types=1);

namespace Tests\Feature;

use Illuminate\Support\Facades\Artisan;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Str;
use Infrastructure\Telephony\Outbox\EloquentTelephonyOutboxMapper;
use Infrastructure\Telephony\Outbox\EloquentTelephonyOutboxRepository;
use Tests\TestCase;
use Throwable;

final class PostgreSQLTelephonyOutboxRepositoryTest extends TestCase
{
    private const string CONNECTION = 'pgsql_integration';

    protected function setUp(): void
    {
        parent::setUp();

        $this->configurePostgreSQLConnection(self::CONNECTION);
        $this->configurePostgreSQLConnection('pgsql_lock_probe');

        if (! $this->postgresIsAvailable()) {
            $this->markTestSkipped('PostgreSQL integration database is not available.');
        }

        Artisan::call('migrate:fresh', [
            '--database' => self::CONNECTION,
            '--force' => true,
        ]);
    }

    protected function tearDown(): void
    {
        DB::disconnect(self::CONNECTION);
        DB::disconnect('pgsql_lock_probe');

        parent::tearDown();
    }

    public function test_postgresql_claim_returns_incremented_attempts_from_returning(): void
    {
        $id = $this->insertOutbox([
            'external_call_id' => 'asterisk-linkedid-pg-5001',
            'idempotency_key' => 'asterisk-linkedid-pg-5001:call_assignment_requested:5',
            'attempts' => 4,
        ]);

        $messages = $this->repository()->claimDue(1);

        $this->assertCount(1, $messages);
        $this->assertSame($id, $messages[0]->id);
        $this->assertSame(5, $messages[0]->attempts);
        $this->assertDatabaseHas('telephony_outbox', [
            'id' => $id,
            'status' => 'processing',
            'attempts' => 5,
        ], self::CONNECTION);
    }

    public function test_postgresql_claim_skips_rows_locked_by_another_transaction(): void
    {
        $id = $this->insertOutbox([
            'external_call_id' => 'asterisk-linkedid-pg-5002',
            'idempotency_key' => 'asterisk-linkedid-pg-5002:call_assignment_requested:1',
        ]);

        $lockConnection = DB::connection('pgsql_lock_probe');
        $lockConnection->beginTransaction();

        try {
            $lockConnection->selectOne('SELECT id FROM telephony_outbox WHERE id = ? FOR UPDATE', [$id]);

            $messages = $this->repository()->claimDue(1);

            $this->assertSame([], $messages);
            $this->assertDatabaseHas('telephony_outbox', [
                'id' => $id,
                'status' => 'pending',
                'attempts' => 0,
            ], self::CONNECTION);
        } finally {
            $lockConnection->rollBack();
        }

        $messages = $this->repository()->claimDue(1);

        $this->assertCount(1, $messages);
        $this->assertSame($id, $messages[0]->id);
    }

    public function test_two_parallel_postgresql_claim_workers_do_not_claim_the_same_record(): void
    {
        if (! function_exists('proc_open')) {
            $this->markTestSkipped('proc_open is required for parallel PostgreSQL claim test.');
        }

        $id = $this->insertOutbox([
            'external_call_id' => 'asterisk-linkedid-pg-5003',
            'idempotency_key' => 'asterisk-linkedid-pg-5003:call_assignment_requested:1',
        ]);
        $directory = sys_get_temp_dir().'/calls-pg-claim-'.bin2hex(random_bytes(8));
        $startFile = $directory.'/start';
        $script = $directory.'/claim-worker.php';

        mkdir($directory);

        try {
            file_put_contents($script, $this->claimWorkerScript());
            $workers = [];

            foreach ([1, 2] as $worker) {
                $resultFile = $directory.'/worker-'.$worker.'.json';
                $workers[] = $this->startClaimWorker($script, $worker, $startFile, $resultFile);
            }

            touch($startFile);

            $this->waitForClaimWorkers($workers);

            $results = [
                $this->readWorkerResult($directory.'/worker-1.json'),
                $this->readWorkerResult($directory.'/worker-2.json'),
            ];
        } finally {
            foreach (glob($directory.'/*') ?: [] as $file) {
                unlink($file);
            }

            rmdir($directory);
        }

        $claimedByWorker = array_values(array_filter(
            $results,
            static fn (array $result): bool => $result['claimed'] === 1,
        ));

        if (count($claimedByWorker) !== 1) {
            $this->fail('Expected exactly one PostgreSQL claim worker to receive the outbox record.');
        }

        $claimed = $claimedByWorker[0];

        $this->assertSame([$id], $claimed['ids']);
        $this->assertSame(1, array_sum(array_column($results, 'claimed')));
        $this->assertDatabaseHas('telephony_outbox', [
            'id' => $id,
            'status' => 'processing',
            'attempts' => 1,
        ], self::CONNECTION);
    }

    private function configurePostgreSQLConnection(string $name): void
    {
        config()->set("database.connections.{$name}", [
            'driver' => 'pgsql',
            'host' => $this->postgresEnv('PG_INTEGRATION_HOST', '127.0.0.1'),
            'port' => $this->postgresEnv('PG_INTEGRATION_PORT', '5432'),
            'database' => $this->postgresEnv('PG_INTEGRATION_DATABASE', 'calls_testing'),
            'username' => $this->postgresEnv('PG_INTEGRATION_USERNAME', 'sail'),
            'password' => $this->postgresEnv('PG_INTEGRATION_PASSWORD', 'password'),
            'charset' => 'utf8',
            'prefix' => '',
            'prefix_indexes' => true,
            'search_path' => 'public',
            'sslmode' => 'prefer',
        ]);

        config()->set('database.default', self::CONNECTION);
        DB::setDefaultConnection(self::CONNECTION);
        DB::purge($name);
    }

    private function postgresIsAvailable(): bool
    {
        try {
            DB::connection(self::CONNECTION)->getPdo();

            return true;
        } catch (Throwable) {
            return false;
        }
    }

    /**
     * @param  array<string, mixed>  $attributes
     */
    private function insertOutbox(array $attributes): int
    {
        $now = now();

        return (int) DB::connection(self::CONNECTION)->table('telephony_outbox')->insertGetId(array_merge([
            'command_id' => (string) Str::uuid(),
            'idempotency_key' => 'asterisk-linkedid-pg-default:call_assignment_requested:1',
            'type' => 'call_assignment_requested',
            'external_call_id' => 'asterisk-linkedid-pg-default',
            'payload' => json_encode([
                'external_call_id' => 'asterisk-linkedid-pg-default',
            ], JSON_THROW_ON_ERROR),
            'status' => 'pending',
            'attempts' => 0,
            'available_at' => null,
            'processing_started_at' => null,
            'published_at' => null,
            'canceled_at' => null,
            'cancel_reason' => null,
            'last_error' => null,
            'created_at' => $now,
            'updated_at' => $now,
        ], [
            ...$attributes,
            'payload' => json_encode($attributes['payload'] ?? [
                'external_call_id' => $attributes['external_call_id'] ?? 'asterisk-linkedid-pg-default',
            ], JSON_THROW_ON_ERROR),
        ]));
    }

    private function repository(): EloquentTelephonyOutboxRepository
    {
        return new EloquentTelephonyOutboxRepository(new EloquentTelephonyOutboxMapper);
    }

    /**
     * @return array{process: resource, pipes: array<int, resource>, result_file: string}
     */
    private function startClaimWorker(string $script, int $worker, string $startFile, string $resultFile): array
    {
        $process = proc_open(
            [
                PHP_BINARY,
                $script,
                (string) $worker,
                $startFile,
                $resultFile,
            ],
            [
                1 => ['pipe', 'w'],
                2 => ['pipe', 'w'],
            ],
            $pipes,
            base_path(),
            [
                ...$this->currentEnvironment(),
                'APP_ENV' => 'testing',
                'APP_MAINTENANCE_DRIVER' => 'file',
                'CACHE_STORE' => 'array',
                'QUEUE_CONNECTION' => 'sync',
                'SESSION_DRIVER' => 'array',
                'PG_INTEGRATION_HOST' => $this->postgresEnv('PG_INTEGRATION_HOST', '127.0.0.1'),
                'PG_INTEGRATION_PORT' => $this->postgresEnv('PG_INTEGRATION_PORT', '5432'),
                'PG_INTEGRATION_DATABASE' => $this->postgresEnv('PG_INTEGRATION_DATABASE', 'calls_testing'),
                'PG_INTEGRATION_USERNAME' => $this->postgresEnv('PG_INTEGRATION_USERNAME', 'sail'),
                'PG_INTEGRATION_PASSWORD' => $this->postgresEnv('PG_INTEGRATION_PASSWORD', 'password'),
            ],
        );

        if (! is_resource($process)) {
            $this->fail('Failed to start PostgreSQL claim worker process.');
        }

        return [
            'process' => $process,
            'pipes' => $pipes,
            'result_file' => $resultFile,
        ];
    }

    /**
     * @return array<string, string>
     */
    private function currentEnvironment(): array
    {
        return getenv();
    }

    /**
     * @param  list<array{process: resource, pipes: array<int, resource>, result_file: string}>  $workers
     */
    private function waitForClaimWorkers(array $workers): void
    {
        $deadline = microtime(true) + 10.0;

        foreach ($workers as $worker) {
            do {
                $status = proc_get_status($worker['process']);

                if (! $status['running']) {
                    break;
                }

                usleep(10000);
            } while (microtime(true) < $deadline);

            if ($status['running']) {
                proc_terminate($worker['process']);
                $this->fail('PostgreSQL claim worker timed out.');
            }

            $stdout = stream_get_contents($worker['pipes'][1]);
            $stderr = stream_get_contents($worker['pipes'][2]);

            foreach ($worker['pipes'] as $pipe) {
                fclose($pipe);
            }

            proc_close($worker['process']);

            if (! file_exists($worker['result_file'])) {
                $this->fail(sprintf(
                    'PostgreSQL claim worker produced no result. stdout=%s stderr=%s',
                    $stdout,
                    $stderr,
                ));
            }
        }
    }

    private function claimWorkerScript(): string
    {
        $basePath = var_export(base_path(), true);

        return <<<PHP
            <?php

            declare(strict_types=1);

            use Domain\Telephony\TelephonyOutboxMessage;
            use Illuminate\Contracts\Console\Kernel;
            use Illuminate\Support\Facades\DB;
            use Infrastructure\Telephony\Outbox\EloquentTelephonyOutboxMapper;
            use Infrastructure\Telephony\Outbox\EloquentTelephonyOutboxRepository;

            require {$basePath}.'/vendor/autoload.php';

            \$app = require {$basePath}.'/bootstrap/app.php';
            \$app->make(Kernel::class)->bootstrap();

            \$worker = (int) \$argv[1];
            \$startFile = (string) \$argv[2];
            \$resultFile = (string) \$argv[3];

            try {
                config()->set('database.connections.pgsql_integration', [
                    'driver' => 'pgsql',
                    'host' => getenv('PG_INTEGRATION_HOST') ?: '127.0.0.1',
                    'port' => getenv('PG_INTEGRATION_PORT') ?: '5432',
                    'database' => getenv('PG_INTEGRATION_DATABASE') ?: 'calls_testing',
                    'username' => getenv('PG_INTEGRATION_USERNAME') ?: 'sail',
                    'password' => getenv('PG_INTEGRATION_PASSWORD') ?: 'password',
                    'charset' => 'utf8',
                    'prefix' => '',
                    'prefix_indexes' => true,
                    'search_path' => 'public',
                    'sslmode' => 'prefer',
                ]);
                config()->set('database.default', 'pgsql_integration');
                DB::setDefaultConnection('pgsql_integration');
                DB::purge('pgsql_integration');

                while (! file_exists(\$startFile)) {
                    usleep(1000);
                }

                \$repository = new EloquentTelephonyOutboxRepository(new EloquentTelephonyOutboxMapper);
                \$messages = \$repository->claimDue(1);

                file_put_contents(\$resultFile, json_encode([
                    'worker' => \$worker,
                    'claimed' => count(\$messages),
                    'ids' => array_map(
                        static fn (TelephonyOutboxMessage \$message): int => \$message->id,
                        \$messages,
                    ),
                ], JSON_THROW_ON_ERROR));
            } catch (Throwable \$exception) {
                file_put_contents(\$resultFile, json_encode([
                    'worker' => \$worker,
                    'error' => \$exception->getMessage(),
                ], JSON_THROW_ON_ERROR));

                exit(1);
            }
            PHP;
    }

    /**
     * @return array{worker: int, claimed: int, ids: list<int>}
     */
    private function readWorkerResult(string $file): array
    {
        $payload = json_decode((string) file_get_contents($file), true, flags: JSON_THROW_ON_ERROR);

        if (! is_array($payload) || isset($payload['error'])) {
            $this->fail(sprintf('PostgreSQL claim worker failed: %s', json_encode($payload, JSON_THROW_ON_ERROR)));
        }

        return [
            'worker' => (int) $payload['worker'],
            'claimed' => (int) $payload['claimed'],
            'ids' => array_values(array_map('intval', $payload['ids'] ?? [])),
        ];
    }

    private function postgresEnv(string $key, string $default): string
    {
        $value = getenv($key);

        return is_string($value) && $value !== '' ? $value : $default;
    }
}
