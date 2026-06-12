<?php

declare(strict_types=1);

namespace Tests\Feature;

use Illuminate\Testing\PendingCommand;
use Tests\TestCase;

final class ScheduleTest extends TestCase
{
    public function test_operational_recovery_commands_are_scheduled(): void
    {
        config()->set('cache.default', 'array');
        $this->app->forgetInstance('cache');
        $this->app->forgetInstance('cache.store');

        $command = $this->artisan('schedule:list');

        if (! $command instanceof PendingCommand) {
            $this->fail('Expected a pending artisan command.');
        }

        $command
            ->expectsOutputToContain('calls:telephony-outbox:publish')
            ->expectsOutputToContain('calls:telephony-outbox:requeue-stale')
            ->expectsOutputToContain('calls:operator-reservations:release-expired')
            ->expectsOutputToContain('calls:metrics:snapshot')
            ->expectsOutputToContain('calls:dead-letter:prune-resolved')
            ->assertSuccessful()
            ->run();
    }
}
