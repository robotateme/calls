<?php

use App\Http\Controllers\MetricsController;
use Illuminate\Support\Facades\Route;

Route::get('/metrics', MetricsController::class);
Route::fallback(static fn () => response('', 404));
