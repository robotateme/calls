<?php

use Illuminate\Foundation\Application;
use Illuminate\Foundation\Configuration\Exceptions;
use Illuminate\Foundation\Configuration\Middleware;
use Illuminate\Http\Request;
use Illuminate\Console\Scheduling\Schedule;

return Application::configure(basePath: dirname(__DIR__))
    ->withRouting(
        web: __DIR__.'/../routes/web.php',
        commands: __DIR__.'/../routes/console.php',
        health: '/up',
    )
    ->withSchedule(function (Schedule $schedule): void {
        $schedule
            ->command('calls:telephony-outbox:publish')
            ->everyMinute()
            ->withoutOverlapping();

        $schedule
            ->command('calls:telephony-outbox:requeue-stale')
            ->everyMinute()
            ->withoutOverlapping();

        $schedule
            ->command('calls:operator-reservations:release-expired')
            ->everyMinute()
            ->withoutOverlapping();

        $schedule
            ->command('calls:metrics:snapshot')
            ->everyMinute()
            ->withoutOverlapping();

        $schedule
            ->command('calls:dead-letter:prune-resolved')
            ->daily()
            ->withoutOverlapping();
    })
    ->withMiddleware(function (Middleware $middleware): void {
        //
    })
    ->withExceptions(function (Exceptions $exceptions): void {
        $exceptions->shouldRenderJsonWhen(
            fn (Request $request) => $request->is('api/*'),
        );
    })->create();
