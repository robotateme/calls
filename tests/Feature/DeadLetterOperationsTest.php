<?php

declare(strict_types=1);

namespace Tests\Feature;

use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Str;
use Illuminate\Testing\PendingCommand;
use Tests\TestCase;

final class DeadLetterOperationsTest extends TestCase
{
    use RefreshDatabase;

    public function test_it_resolves_dead_letter_record(): void
    {
        $id = $this->insertDeadLetter();

        $command = $this->artisan('calls:dead-letter:resolve', [
            'id' => (string) $id,
            '--note' => 'manually checked',
        ]);

        if (! $command instanceof PendingCommand) {
            $this->fail('Expected a pending artisan command.');
        }

        $command
            ->expectsOutputToContain('Dead letter records resolved: 1')
            ->assertSuccessful()
            ->run();

        $row = DB::table('dead_letter_messages')->where('id', $id)->first();

        $this->assertNotNull($row);
        $this->assertNotNull($row->resolved_at);
        $this->assertSame('manually checked', $row->resolution_note);
    }

    public function test_it_prunes_only_old_resolved_dead_letters(): void
    {
        $oldResolved = $this->insertDeadLetter(resolvedAt: now()->subDays(40));
        $freshResolved = $this->insertDeadLetter(resolvedAt: now()->subDays(2));
        $unresolved = $this->insertDeadLetter();

        $command = $this->artisan('calls:dead-letter:prune-resolved', [
            '--older-than-days' => '30',
            '--limit' => '100',
        ]);

        if (! $command instanceof PendingCommand) {
            $this->fail('Expected a pending artisan command.');
        }

        $command
            ->expectsOutputToContain('Resolved dead letter records pruned: 1')
            ->assertSuccessful()
            ->run();

        $this->assertDatabaseMissing('dead_letter_messages', ['id' => $oldResolved]);
        $this->assertDatabaseHas('dead_letter_messages', ['id' => $freshResolved]);
        $this->assertDatabaseHas('dead_letter_messages', ['id' => $unresolved]);
    }

    private function insertDeadLetter(mixed $resolvedAt = null): int
    {
        return (int) DB::table('dead_letter_messages')->insertGetId([
            'source' => 'test-consumer',
            'topic' => 'incoming-calls',
            'message_partition' => 0,
            'message_offset' => random_int(1, 1000000),
            'message_key' => (string) Str::uuid(),
            'trace_id' => (string) Str::uuid(),
            'reason' => 'invalid_payload',
            'raw_payload' => '{}',
            'decoded_payload' => json_encode([], JSON_THROW_ON_ERROR),
            'message_hash' => (string) Str::uuid(),
            'resolved_at' => $resolvedAt,
            'created_at' => now(),
        ]);
    }
}
