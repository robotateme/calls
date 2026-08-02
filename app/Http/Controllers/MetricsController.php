<?php

declare(strict_types=1);

namespace App\Http\Controllers;

use Illuminate\Http\Response;
use Infrastructure\Shared\Observability\PrometheusMetricsRenderer;

final class MetricsController extends Controller
{
    public function __invoke(PrometheusMetricsRenderer $renderer): Response
    {
        return response($renderer->render(), 200, [
            'Content-Type' => 'text/plain; version=0.0.4; charset=utf-8',
        ]);
    }
}
