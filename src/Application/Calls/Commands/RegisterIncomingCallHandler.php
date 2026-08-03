<?php

declare(strict_types=1);

namespace Application\Calls\Commands;

use Application\Calls\Ports\CallProcessingQueue;
use Application\Calls\Ports\CallReadRepository;
use Application\Calls\Ports\CallWriteRepository;
use Application\Shared\Ports\EventBus;
use Application\Shared\Ports\TransactionManager;
use Domain\Calls\CallHangupPolicy;
use Domain\Calls\Events\IncomingCallRegistered;
use Domain\Calls\ExternalCallId;
use Domain\Calls\OperatorSearchMaxAttempts;
use Domain\Calls\OperatorSearchRetryDelay;
use Domain\Calls\PhoneNumber;

final readonly class RegisterIncomingCallHandler
{
    public function __construct(
        private CallReadRepository $callReader,
        private CallWriteRepository $callWriter,
        private TransactionManager $transactions,
        private CallProcessingQueue $processingQueue,
        private EventBus $events,
    ) {}

    public function handle(RegisterIncomingCallFromKafkaCommand $command): RegisterIncomingCallResult
    {
        $normalizedExternalCallId = trim($command->externalCallId);
        $normalizedPhone = trim($command->phone);
        $normalizedKafkaMessageId = trim($command->kafkaMessageId);
        $normalizedHangupPolicy = trim($command->operatorSearchHangupPolicy);

        $result = $this->transactions->run(function () use (
            $normalizedExternalCallId,
            $normalizedPhone,
            $normalizedKafkaMessageId,
            $normalizedHangupPolicy,
            $command,
        ): RegisterIncomingCallResult {
            $externalCallId = ExternalCallId::fromString($normalizedExternalCallId);
            $phone = PhoneNumber::fromString($normalizedPhone);
            $hangupPolicy = CallHangupPolicy::from($normalizedHangupPolicy);
            $operatorSearchMaxAttempts = max(1, $command->operatorSearchMaxAttempts);
            $operatorSearchRetryDelaySeconds = max(0, $command->operatorSearchRetryDelaySeconds);
            $existingCall = $this->callReader->findByExternalCallId($externalCallId);

            if ($existingCall !== null) {
                return new RegisterIncomingCallResult($existingCall->id(), false);
            }

            $createdCall = $this->callWriter->createIncomingFromKafka(
                externalCallId: $externalCallId,
                phone: $phone,
                kafkaMessageId: $normalizedKafkaMessageId,
                operatorSearchMaxAttempts: OperatorSearchMaxAttempts::fromInt($operatorSearchMaxAttempts),
                operatorSearchRetryDelay: OperatorSearchRetryDelay::fromSeconds($operatorSearchRetryDelaySeconds),
                operatorSearchHangupPolicy: $hangupPolicy,
            );

            return new RegisterIncomingCallResult(
                callId: $createdCall->id(),
                created: true,
            );
        });

        if ($result->created) {
            $this->events->publish(new IncomingCallRegistered(
                callId: $result->callId,
                externalCallId: $normalizedExternalCallId,
                phone: $normalizedPhone,
                kafkaMessageId: $normalizedKafkaMessageId,
            ));
            $this->processingQueue->enqueue($result->callId);
        }

        return $result;
    }
}
