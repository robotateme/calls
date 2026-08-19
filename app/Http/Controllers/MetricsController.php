<?php

declare(strict_types=1);

namespace App\Http\Controllers;

use Illuminate\Http\Request;
use Illuminate\Http\Response;
use Infrastructure\Shared\Observability\PrometheusMetricsRenderer;

final class MetricsController extends Controller
{
    public function __invoke(Request $request, PrometheusMetricsRenderer $renderer): Response
    {
        if (! $this->isAuthorized($request)) {
            return response('Unauthorized', 401, [
                'WWW-Authenticate' => 'Basic realm="Calls metrics"',
            ]);
        }

        return response($renderer->render(), 200, [
            'Content-Type' => 'text/plain; version=0.0.4; charset=utf-8',
        ]);
    }

    private function isAuthorized(Request $request): bool
    {
        $configuredUser = $this->optionalConfigString('calls.metrics_basic_auth_user');
        $configuredPassword = $this->optionalConfigString('calls.metrics_basic_auth_password');

        if ($configuredUser === null && $configuredPassword === null) {
            return true;
        }

        if ($configuredUser === null || $configuredPassword === null) {
            return false;
        }

        $requestUser = $request->getUser();
        $requestPassword = $request->getPassword();

        if ($requestUser === null || $requestPassword === null) {
            return false;
        }

        return hash_equals($configuredUser, $requestUser)
            && hash_equals($configuredPassword, $requestPassword);
    }

    private function optionalConfigString(string $key): ?string
    {
        $value = config($key);

        if (! is_scalar($value)) {
            return null;
        }

        $normalized = trim((string) $value);

        return $normalized === '' ? null : $normalized;
    }
}
