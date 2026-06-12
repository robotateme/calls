<?php

declare(strict_types=1);

namespace Tests\Unit;

use Domain\Calls\CallId;
use Domain\Calls\ExternalCallId;
use Domain\Calls\OperatorSearchAttempts;
use Domain\Calls\OperatorSearchMaxAttempts;
use Domain\Calls\OperatorSearchRetryDelay;
use Domain\Calls\PhoneNumber;
use Domain\Clients\ClientId;
use Domain\Operators\OperatorId;
use Domain\Shared\Timestamp;
use InvalidArgumentException;
use Tests\TestCase;

final class DomainValueObjectsTest extends TestCase
{
    public function test_it_wraps_database_scalars_in_domain_value_objects(): void
    {
        $this->assertSame(1, CallId::fromInt(1)->toInt());
        $this->assertSame(2, ClientId::fromInt(2)->toInt());
        $this->assertSame(3, OperatorId::fromInt(3)->toInt());
        $this->assertSame('external-1', ExternalCallId::fromString(' external-1 ')->toString());
        $this->assertSame('+15550000000', PhoneNumber::fromString(' +15550000000 ')->toString());

        $attempts = OperatorSearchAttempts::fromInt(1)->increment();
        $this->assertSame(2, $attempts->toInt());
        $this->assertTrue($attempts->isLessThan(OperatorSearchMaxAttempts::fromInt(3)));

        $nextAttempt = OperatorSearchRetryDelay::fromSeconds(15)
            ->nextAttemptFrom(Timestamp::fromString('2026-06-11T10:20:30+00:00'));

        $this->assertSame('2026-06-11T10:20:45+00:00', $nextAttempt->toAtom());
    }

    public function test_it_rejects_invalid_domain_values(): void
    {
        $this->expectException(InvalidArgumentException::class);

        CallId::fromInt(0);
    }
}
