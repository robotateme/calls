<?php

declare(strict_types=1);

namespace App\Http\Controllers;

use Illuminate\Http\Request;
use Illuminate\Http\Response;
use Infrastructure\Shared\Observability\PrometheusMetricsRenderer;
use Symfony\Component\HttpFoundation\IpUtils;

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
        if (! $this->isAllowedIp($request)) {
            return false;
        }

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

    private function isAllowedIp(Request $request): bool
    {
        $allowedIps = $this->optionalConfigStringList('calls.metrics_allowed_ips');

        if ($allowedIps === []) {
            return true;
        }

        $requestIp = $request->ip();

        if ($requestIp === null) {
            return false;
        }

        return IpUtils::checkIp($requestIp, $allowedIps);
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

    /**
     * @return list<string>
     */
    private function optionalConfigStringList(string $key): array
    {
        $value = config($key);

        if (! is_scalar($value)) {
            return [];
        }

        $parts = array_map('trim', explode(',', (string) $value));

        return array_values(array_filter($parts, static fn (string $part): bool => $part !== ''));
    }
}
