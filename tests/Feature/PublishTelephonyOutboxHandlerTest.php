<?php

declare(strict_types=1);

namespace Tests\Feature;

use Application\Calls\Commands\PublishTelephonyOutboxHandler;
use Application\Calls\Commands\RequeueStaleTelephonyOutboxHandler;
use Application\Shared\Ports\ConsoleCommandResult;
use Application\Shared\Ports\ConsoleCommandRunner;
use Application\Telephony\Ports\TelephonyCommandPublisher;
use Domain\Telephony\TelephonyOutboxMessage;
use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Str;
use Illuminate\Testing\PendingCommand;
use RuntimeException;
use Tests\TestCase;

final class PublishTelephonyOutboxHandlerTest extends TestCase
{
    use RefreshDatabase;

    public function test_it_publishes_due_outbox_records_and_marks_them_published(): void
    {
        $publisher = new FakeTelephonyCommandPublisher;
        $this->app->instance(TelephonyCommandPublisher::class, $publisher);
        $id = $this->insertOutbox([
            'external_call_id' => 'asterisk-linkedid-5001',
            'type' => 'call_assignment_requested',
            'idempotency_key' => 'asterisk-linkedid-5001:call_assignment_requested:1',
            'payload' => [
                'external_call_id' => 'asterisk-linkedid-5001',
                'operator_id' => 10,
                'assignment_attempt' => 1,
            ],
        ]);

        $result = $this->handler()->handle(limit: 10, retryDelaySeconds: 5, maxAttempts: 3);

        $this->assertSame(1, $result->claimed);
        $this->assertSame(1, $result->published);
        $this->assertSame(0, $result->failed);
        $this->assertCount(1, $publisher->published);
        $this->assertSame('asterisk-linkedid-5001', $publisher->published[0]->externalCallId);
        $this->assertDatabaseHas('telephony_outbox', [
            'id' => $id,
            'status' => 'published',
            'attempts' => 1,
            'last_error' => null,
        ]);
        $this->assertNotNull(DB::table('telephony_outbox')->where('id', $id)->value('published_at'));
    }

    public function test_it_reschedules_failed_publish_before_max_attempts(): void
    {
        $publisher = new FakeTelephonyCommandPublisher(throws: true);
        $this->app->instance(TelephonyCommandPublisher::class, $publisher);
        $id = $this->insertOutbox([
            'external_call_id' => 'asterisk-linkedid-5002',
            'idempotency_key' => 'asterisk-linkedid-5002:operator_search_retry_scheduled:1',
        ]);

        $result = $this->handler()->handle(limit: 10, retryDelaySeconds: 30, maxAttempts: 3);

        $this->assertSame(1, $result->claimed);
        $this->assertSame(0, $result->published);
        $this->assertSame(1, $result->failed);
        $this->assertDatabaseHas('telephony_outbox', [
            'id' => $id,
            'status' => 'pending',
            'attempts' => 1,
            'last_error' => 'Kafka unavailable',
        ]);
        $this->assertNotNull(DB::table('telephony_outbox')->where('id', $id)->value('available_at'));
    }

    public function test_it_marks_failed_after_max_attempts(): void
    {
        $publisher = new FakeTelephonyCommandPublisher(throws: true);
        $this->app->instance(TelephonyCommandPublisher::class, $publisher);
        $id = $this->insertOutbox([
            'external_call_id' => 'asterisk-linkedid-5003',
            'idempotency_key' => 'asterisk-linkedid-5003:operator_search_exhausted:10',
            'attempts' => 9,
        ]);

        $result = $this->handler()->handle(limit: 10, retryDelaySeconds: 30, maxAttempts: 10);

        $this->assertSame(1, $result->claimed);
        $this->assertSame(0, $result->published);
        $this->assertSame(1, $result->failed);
        $this->assertDatabaseHas('telephony_outbox', [
            'id' => $id,
            'status' => 'failed',
            'attempts' => 10,
            'last_error' => 'Kafka unavailable',
        ]);
    }

    public function test_it_does_not_claim_records_that_are_not_due_yet(): void
    {
        $publisher = new FakeTelephonyCommandPublisher;
        $this->app->instance(TelephonyCommandPublisher::class, $publisher);
        $this->insertOutbox([
            'external_call_id' => 'asterisk-linkedid-5004',
            'idempotency_key' => 'asterisk-linkedid-5004:operator_search_retry_scheduled:1',
            'available_at' => now()->addMinute(),
        ]);

        $result = $this->handler()->handle(limit: 10, retryDelaySeconds: 30, maxAttempts: 3);

        $this->assertSame(0, $result->claimed);
        $this->assertSame([], $publisher->published);
    }

    public function test_it_does_not_claim_canceled_records(): void
    {
        $publisher = new FakeTelephonyCommandPublisher;
        $this->app->instance(TelephonyCommandPublisher::class, $publisher);
        $this->insertOutbox([
            'external_call_id' => 'asterisk-linkedid-5006',
            'idempotency_key' => 'asterisk-linkedid-5006:call_assignment_requested:1',
            'canceled_at' => now(),
            'cancel_reason' => 'call_hung_up',
        ]);

        $result = $this->handler()->handle(limit: 10, retryDelaySeconds: 30, maxAttempts: 3);

        $this->assertSame(0, $result->claimed);
        $this->assertSame([], $publisher->published);
    }

    public function test_artisan_command_publishes_outbox_once(): void
    {
        $this->app->instance(ConsoleCommandRunner::class, new FakeOutboxConsoleCommandRunner(
            new ConsoleCommandResult(0, '', ''),
        ));
        $id = $this->insertOutbox([
            'external_call_id' => 'asterisk-linkedid-5005',
            'idempotency_key' => 'asterisk-linkedid-5005:call_assignment_requested:1',
        ]);

        $command = $this->artisan('calls:telephony-outbox:publish', [
            '--limit' => 10,
            '--retry-delay' => 5,
            '--max-attempts' => 3,
        ]);

        if (! $command instanceof PendingCommand) {
            $this->fail('Expected a pending artisan command.');
        }

        $command
            ->expectsOutputToContain('Telephony outbox: claimed=1 published=1 failed=0')
            ->assertSuccessful()
            ->run();

        $this->assertDatabaseHas('telephony_outbox', [
            'id' => $id,
            'status' => 'published',
            'attempts' => 1,
        ]);
    }

    public function test_it_requeues_stale_processing_records(): void
    {
        $staleId = $this->insertOutbox([
            'external_call_id' => 'asterisk-linkedid-5007',
            'idempotency_key' => 'asterisk-linkedid-5007:call_assignment_requested:1',
            'status' => 'processing',
            'attempts' => 1,
            'processing_started_at' => now()->subMinutes(5),
        ]);
        $freshId = $this->insertOutbox([
            'external_call_id' => 'asterisk-linkedid-5008',
            'idempotency_key' => 'asterisk-linkedid-5008:call_assignment_requested:1',
            'status' => 'processing',
            'attempts' => 1,
            'processing_started_at' => now(),
        ]);

        $requeued = $this->requeueHandler()->handle(olderThanSeconds: 120, limit: 10);

        $this->assertSame(1, $requeued);
        $this->assertDatabaseHas('telephony_outbox', [
            'id' => $staleId,
            'status' => 'pending',
            'attempts' => 1,
            'processing_started_at' => null,
            'last_error' => 'Processing timeout: publisher did not finish claimed record.',
        ]);
        $this->assertNotNull(DB::table('telephony_outbox')->where('id', $staleId)->value('available_at'));
        $this->assertDatabaseHas('telephony_outbox', [
            'id' => $freshId,
            'status' => 'processing',
            'attempts' => 1,
        ]);
    }

    public function test_artisan_command_requeues_stale_outbox_once(): void
    {
        $id = $this->insertOutbox([
            'external_call_id' => 'asterisk-linkedid-5009',
            'idempotency_key' => 'asterisk-linkedid-5009:call_assignment_requested:1',
            'status' => 'processing',
            'attempts' => 1,
            'processing_started_at' => now()->subMinutes(5),
        ]);

        $command = $this->artisan('calls:telephony-outbox:requeue-stale', [
            '--older-than' => 120,
            '--limit' => 10,
        ]);

        if (! $command instanceof PendingCommand) {
            $this->fail('Expected a pending artisan command.');
        }

        $command
            ->expectsOutputToContain('Stale Telephony outbox records requeued: 1')
            ->assertSuccessful()
            ->run();

        $this->assertDatabaseHas('telephony_outbox', [
            'id' => $id,
            'status' => 'pending',
            'attempts' => 1,
        ]);
    }

    /**
     * @param  array<string, mixed>  $attributes
     */
    private function insertOutbox(array $attributes): int
    {
        return (int) DB::table('telephony_outbox')->insertGetId(array_merge([
            'command_id' => (string) Str::uuid(),
            'idempotency_key' => 'asterisk-linkedid-default:call_assignment_requested:1',
            'type' => 'call_assignment_requested',
            'external_call_id' => 'asterisk-linkedid-default',
            'payload' => json_encode([
                'external_call_id' => 'asterisk-linkedid-default',
            ], JSON_THROW_ON_ERROR),
            'status' => 'pending',
            'attempts' => 0,
            'available_at' => null,
            'processing_started_at' => null,
            'published_at' => null,
            'canceled_at' => null,
            'cancel_reason' => null,
            'last_error' => null,
            'created_at' => now(),
            'updated_at' => now(),
        ], [
            ...$attributes,
            'payload' => json_encode($attributes['payload'] ?? [
                'external_call_id' => $attributes['external_call_id'] ?? 'asterisk-linkedid-default',
            ], JSON_THROW_ON_ERROR),
        ]));
    }

    private function handler(): PublishTelephonyOutboxHandler
    {
        return $this->app->make(PublishTelephonyOutboxHandler::class);
    }

    private function requeueHandler(): RequeueStaleTelephonyOutboxHandler
    {
        return $this->app->make(RequeueStaleTelephonyOutboxHandler::class);
    }
}

final class FakeTelephonyCommandPublisher implements TelephonyCommandPublisher
{
    /**
     * @var list<TelephonyOutboxMessage>
     */
    public array $published = [];

    public function __construct(private readonly bool $throws = false) {}

    public function publish(TelephonyOutboxMessage $message): void
    {
        if ($this->throws) {
            throw new RuntimeException('Kafka unavailable');
        }

        $this->published[] = $message;
    }
}

final class FakeOutboxConsoleCommandRunner implements ConsoleCommandRunner
{
    public function __construct(private readonly ConsoleCommandResult $result) {}

    public function run(array $command, string $stdin, int $timeoutSeconds): ConsoleCommandResult
    {
        return $this->result;
    }
}
