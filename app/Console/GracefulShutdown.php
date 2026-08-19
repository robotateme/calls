<?php

declare(strict_types=1);

namespace App\Console;

final class GracefulShutdown
{
    private bool $requested = false;

    public function installSignalHandlers(): void
    {
        if (! function_exists('pcntl_signal') || ! function_exists('pcntl_async_signals')) {
            return;
        }

        pcntl_async_signals(true);

        $handler = function (): void {
            $this->requested = true;
        };

        pcntl_signal(SIGTERM, $handler);
        pcntl_signal(SIGINT, $handler);
    }

    public function requested(): bool
    {
        return $this->requested;
    }

    public function sleepInterruptibly(int $seconds): void
    {
        for ($second = 0; $second < $seconds; $second++) {
            if ($this->requested) {
                return;
            }

            sleep(1);
        }
    }
}
