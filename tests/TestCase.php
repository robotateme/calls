<?php

namespace Tests;

use Illuminate\Contracts\Console\Kernel;
use Illuminate\Foundation\Application;
use Illuminate\Foundation\Testing\TestCase as BaseTestCase;

abstract class TestCase extends BaseTestCase
{
    public function createApplication()
    {
        $this->forceTestingEnvironment();

        $app = require Application::inferBasePath().'/bootstrap/app.php';

        $app->make(Kernel::class)->bootstrap();

        return $app;
    }

    protected function setUp(): void
    {
        parent::setUp();

        config()->set('session.driver', 'array');
    }

    private function forceTestingEnvironment(): void
    {
        $this->setEnvironmentValue('APP_ENV', 'testing');
        $this->setEnvironmentValue('CACHE_STORE', 'array');
        $this->setEnvironmentValue('DB_CONNECTION', 'sqlite');
        $this->setEnvironmentValue('DB_DATABASE', ':memory:');
        $this->setEnvironmentValue('DB_URL', '');
        $this->setEnvironmentValue('LOG_CHANNEL', 'null');
        $this->setEnvironmentValue('QUEUE_CONNECTION', 'sync');
        $this->setEnvironmentValue('SESSION_DRIVER', 'array');
    }

    private function setEnvironmentValue(string $key, string $value): void
    {
        putenv(sprintf('%s=%s', $key, $value));
        $_ENV[$key] = $value;
        $_SERVER[$key] = $value;
    }
}
