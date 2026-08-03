<?php

declare(strict_types=1);

namespace Domain\Calls;

use DateTimeImmutable;
use Domain\Clients\ClientId;
use Domain\Operators\OperatorId;
use Domain\Shared\Timestamp;

final class Call
{
    private function __construct(
        private readonly CallId $id,
        private readonly ExternalCallId $externalCallId,
        private readonly PhoneNumber $phone,
        private CallStatus $status,
        private ?ClientId $clientId,
        private ?OperatorId $operatorId,
        private OperatorSearchAttempts $operatorSearchAttempts,
        private readonly OperatorSearchMaxAttempts $operatorSearchMaxAttempts,
        private readonly OperatorSearchRetryDelay $operatorSearchRetryDelay,
        private readonly CallHangupPolicy $operatorSearchHangupPolicy,
        private ?Timestamp $nextOperatorSearchAt,
        private ?Timestamp $assignmentRequestedAt,
        private ?Timestamp $operatorDialingAt,
        private ?Timestamp $connectedAt,
        private readonly Timestamp $createdAt,
    ) {}

    public static function restore(
        CallId $id,
        ExternalCallId $externalCallId,
        PhoneNumber $phone,
        CallStatus $status,
        ?ClientId $clientId,
        ?OperatorId $operatorId,
        OperatorSearchAttempts $operatorSearchAttempts,
        OperatorSearchMaxAttempts $operatorSearchMaxAttempts,
        OperatorSearchRetryDelay $operatorSearchRetryDelay,
        CallHangupPolicy $operatorSearchHangupPolicy,
        ?Timestamp $nextOperatorSearchAt,
        ?Timestamp $assignmentRequestedAt,
        ?Timestamp $operatorDialingAt,
        ?Timestamp $connectedAt,
        Timestamp $createdAt,
    ): self {
        return new self(
            id: $id,
            externalCallId: $externalCallId,
            phone: $phone,
            status: $status,
            clientId: $clientId,
            operatorId: $operatorId,
            operatorSearchAttempts: $operatorSearchAttempts,
            operatorSearchMaxAttempts: $operatorSearchMaxAttempts,
            operatorSearchRetryDelay: $operatorSearchRetryDelay,
            operatorSearchHangupPolicy: $operatorSearchHangupPolicy,
            nextOperatorSearchAt: $nextOperatorSearchAt,
            assignmentRequestedAt: $assignmentRequestedAt,
            operatorDialingAt: $operatorDialingAt,
            connectedAt: $connectedAt,
            createdAt: $createdAt,
        );
    }

    public function id(): int
    {
        return $this->id->toInt();
    }

    public function callId(): CallId
    {
        return $this->id;
    }

    public function externalCallId(): string
    {
        return $this->externalCallId->toString();
    }

    public function phone(): string
    {
        return $this->phone->toString();
    }

    public function phoneNumber(): PhoneNumber
    {
        return $this->phone;
    }

    public function status(): CallStatus
    {
        return $this->status;
    }

    public function clientId(): ?int
    {
        return $this->clientId?->toInt();
    }

    public function operatorId(): ?int
    {
        return $this->operatorId?->toInt();
    }

    public function assignedOperatorId(): ?OperatorId
    {
        return $this->operatorId;
    }

    public function operatorSearchAttempts(): int
    {
        return $this->operatorSearchAttempts->toInt();
    }

    public function operatorSearchMaxAttempts(): int
    {
        return $this->operatorSearchMaxAttempts->toInt();
    }

    public function operatorSearchRetryDelaySeconds(): int
    {
        return $this->operatorSearchRetryDelay->seconds();
    }

    public function operatorSearchHangupPolicy(): CallHangupPolicy
    {
        return $this->operatorSearchHangupPolicy;
    }

    public function nextOperatorSearchAt(): ?DateTimeImmutable
    {
        return $this->nextOperatorSearchAt?->toDateTimeImmutable();
    }

    public function nextOperatorSearchTimestamp(): ?Timestamp
    {
        return $this->nextOperatorSearchAt;
    }

    public function assignmentRequestedAt(): ?DateTimeImmutable
    {
        return $this->assignmentRequestedAt?->toDateTimeImmutable();
    }

    public function assignmentRequestedTimestamp(): ?Timestamp
    {
        return $this->assignmentRequestedAt;
    }

    public function operatorDialingAt(): ?DateTimeImmutable
    {
        return $this->operatorDialingAt?->toDateTimeImmutable();
    }

    public function operatorDialingTimestamp(): ?Timestamp
    {
        return $this->operatorDialingAt;
    }

    public function connectedAt(): ?DateTimeImmutable
    {
        return $this->connectedAt?->toDateTimeImmutable();
    }

    public function connectedTimestamp(): ?Timestamp
    {
        return $this->connectedAt;
    }

    public function createdTimestamp(): Timestamp
    {
        return $this->createdAt;
    }

    public function isNew(): bool
    {
        return $this->status === CallStatus::New;
    }

    public function isProcessable(): bool
    {
        return $this->status === CallStatus::New || $this->status === CallStatus::Waiting;
    }

    public function isAssignmentInProgress(): bool
    {
        return $this->status === CallStatus::AssignmentRequested || $this->status === CallStatus::OperatorDialing;
    }

    public function attachClient(?ClientId $clientId): void
    {
        $this->clientId = $clientId;
    }

    public function recordFailedOperatorSearchAttempt(Timestamp $now): OperatorSearchOutcome
    {
        $this->recordOperatorSearchAttempt();

        if ($this->hasOperatorSearchAttemptsLeft()) {
            $this->markWaitingForOperator($this->operatorSearchRetryDelay->nextAttemptFrom($now));

            return OperatorSearchOutcome::retryScheduled($this->operatorSearchRetryDelay->seconds());
        }

        $this->finishOperatorSearchByPolicy();

        return OperatorSearchOutcome::exhausted($this->status);
    }

    public function recordSuccessfulOperatorSearchAttempt(
        OperatorId $operatorId,
        Timestamp $requestedAt,
    ): ?OperatorSearchOutcome {
        if (! $this->isProcessable()) {
            return null;
        }

        $this->recordOperatorSearchAttempt();
        $this->requestOperatorAssignment($operatorId, $requestedAt);

        return OperatorSearchOutcome::assignmentRequested();
    }

    public function markOperatorDialing(OperatorId $operatorId, int $attempt, Timestamp $dialingAt): bool
    {
        if (! $this->matchesCurrentAssignment($operatorId, $attempt)) {
            return false;
        }

        if ($this->status === CallStatus::OperatorDialing) {
            return true;
        }

        if ($this->status !== CallStatus::AssignmentRequested) {
            return false;
        }

        $this->status = CallStatus::OperatorDialing;
        $this->operatorDialingAt = $dialingAt;

        return true;
    }

    public function markConnected(OperatorId $operatorId, int $attempt, Timestamp $connectedAt): bool
    {
        if (! $this->matchesCurrentAssignment($operatorId, $attempt)) {
            return false;
        }

        if ($this->status === CallStatus::Connected) {
            return true;
        }

        if (! $this->isAssignmentInProgress()) {
            return false;
        }

        $this->status = CallStatus::Connected;
        $this->connectedAt = $connectedAt;
        $this->nextOperatorSearchAt = null;

        return true;
    }

    public function failPendingOperatorAssignment(
        OperatorId $operatorId,
        int $attempt,
        Timestamp $now,
    ): ?OperatorAssignmentFailure {
        if (! $this->matchesCurrentAssignment($operatorId, $attempt) || ! $this->isAssignmentInProgress()) {
            return null;
        }

        if ($this->hasOperatorSearchAttemptsLeft()) {
            $this->markWaitingForOperator($this->operatorSearchRetryDelay->nextAttemptFrom($now));

            return OperatorAssignmentFailure::retryScheduled($this->id, $this->operatorSearchRetryDelay->seconds());
        } else {
            $this->finishOperatorSearchByPolicy();
        }

        return OperatorAssignmentFailure::exhausted($this->id, $this->status);
    }

    public function markHungUp(): bool
    {
        if (! $this->isProcessable() && ! $this->isAssignmentInProgress()) {
            return false;
        }

        $this->finishOperatorSearchByPolicy();

        return true;
    }

    private function matchesCurrentAssignment(OperatorId $operatorId, int $attempt): bool
    {
        return $this->operatorId?->toInt() === $operatorId->toInt()
            && $this->operatorSearchAttempts->toInt() === $attempt;
    }

    private function requestOperatorAssignment(OperatorId $operatorId, Timestamp $requestedAt): void
    {
        $this->operatorId = $operatorId;
        $this->status = CallStatus::AssignmentRequested;
        $this->nextOperatorSearchAt = null;
        $this->assignmentRequestedAt = $requestedAt;
        $this->operatorDialingAt = null;
        $this->connectedAt = null;
    }

    private function recordOperatorSearchAttempt(): void
    {
        $this->operatorSearchAttempts = $this->operatorSearchAttempts->increment();
    }

    private function hasOperatorSearchAttemptsLeft(): bool
    {
        return $this->operatorSearchAttempts->isLessThan($this->operatorSearchMaxAttempts);
    }

    private function markWaitingForOperator(Timestamp $nextAttemptAt): void
    {
        $this->status = CallStatus::Waiting;
        $this->operatorId = null;
        $this->nextOperatorSearchAt = $nextAttemptAt;
    }

    private function finishOperatorSearchByPolicy(): void
    {
        $this->status = $this->operatorSearchHangupPolicy->finalStatus();
        $this->operatorId = null;
        $this->nextOperatorSearchAt = null;
    }
}
