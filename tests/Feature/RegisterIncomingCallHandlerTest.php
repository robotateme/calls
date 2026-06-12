<?php

declare(strict_types=1);

namespace Tests\Feature;

use Application\Calls\Commands\RegisterIncomingCallFromKafkaCommand;
use Application\Calls\Commands\RegisterIncomingCallHandler;
use Application\Calls\Ports\CallProcessingQueue;
use Domain\Calls\Events\IncomingCallRegistered;
use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Support\Facades\Event;
use Tests\TestCase;

final class RegisterIncomingCallHandlerTest extends TestCase
{
    use RefreshDatabase;

    public function test_it_registers_kafka_call_enqueues_processing_and_publishes_domain_event(): void
    {
        Event::fake();

        $queue = new FakeCallProcessingQueue;
        $this->app->instance(CallProcessingQueue::class, $queue);

        $result = $this->handler()->handle(new RegisterIncomingCallFromKafkaCommand(
            externalCallId: 'asterisk-linkedid-1001',
            phone: '+15550001001',
            kafkaMessageId: 'calls-0-1001',
            operatorSearchMaxAttempts: 5,
            operatorSearchRetryDelaySeconds: 12,
            operatorSearchHangupPolicy: 'hangup_on_retry',
        ));

        $this->assertTrue($result->created);
        $this->assertDatabaseHas('calls', [
            'id' => $result->callId,
            'external_call_id' => 'asterisk-linkedid-1001',
            'phone' => '+15550001001',
            'kafka_message_id' => 'calls-0-1001',
            'status' => 'new',
            'operator_search_attempts' => 0,
            'operator_search_max_attempts' => 5,
            'operator_search_retry_delay_seconds' => 12,
            'operator_search_hangup_policy' => 'hangup_on_retry',
        ]);
        $this->assertSame([$result->callId], $queue->callIds);

        Event::assertDispatched(IncomingCallRegistered::class, static function (IncomingCallRegistered $event) use ($result): bool {
            return $event->eventId() === 'incoming-call:asterisk-linkedid-1001:registered'
                && $event->payload() === [
                    'call_id' => $result->callId,
                    'external_call_id' => 'asterisk-linkedid-1001',
                    'phone' => '+15550001001',
                    'kafka_message_id' => 'calls-0-1001',
                ];
        });
    }

    public function test_it_reuses_existing_call_for_duplicate_external_call_id(): void
    {
        Event::fake();

        $queue = new FakeCallProcessingQueue;
        $this->app->instance(CallProcessingQueue::class, $queue);

        $first = $this->handler()->handle(new RegisterIncomingCallFromKafkaCommand(
            externalCallId: 'asterisk-linkedid-1003',
            phone: '+15550001003',
            kafkaMessageId: 'calls-0-1003',
        ));
        $second = $this->handler()->handle(new RegisterIncomingCallFromKafkaCommand(
            externalCallId: 'asterisk-linkedid-1003',
            phone: '+15550001003',
            kafkaMessageId: 'calls-0-1004',
        ));

        $this->assertTrue($first->created);
        $this->assertFalse($second->created);
        $this->assertSame($first->callId, $second->callId);
        $this->assertSame([$first->callId], $queue->callIds);
        Event::assertDispatched(IncomingCallRegistered::class, 1);
    }

    private function handler(): RegisterIncomingCallHandler
    {
        return $this->app->make(RegisterIncomingCallHandler::class);
    }
}

final class FakeCallProcessingQueue implements CallProcessingQueue
{
    /**
     * @var list<int>
     */
    public array $callIds = [];

    public function enqueue(int $callId): void
    {
        $this->callIds[] = $callId;
    }
}
