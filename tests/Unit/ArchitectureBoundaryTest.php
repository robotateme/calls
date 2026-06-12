<?php

declare(strict_types=1);

namespace Tests\Unit;

use Application\Shared\Ports\DeadLetterQueue;
use Application\Shared\Ports\EventBus;
use Application\Shared\Ports\KafkaConsumer;
use Application\Shared\Ports\Metrics;
use Application\Shared\Ports\QueueBus;
use Application\Shared\Ports\TransactionManager;
use Infrastructure\Shared\Bus\LaravelEventBus;
use Infrastructure\Shared\Bus\LaravelQueueBus;
use Infrastructure\Shared\Kafka\EloquentDeadLetterQueue;
use Infrastructure\Shared\Kafka\JsonLinesKafkaConsumer;
use Infrastructure\Shared\Observability\LaravelLogMetrics;
use Infrastructure\Shared\Persistence\DatabaseTransactionManager;
use RecursiveDirectoryIterator;
use RecursiveIteratorIterator;
use ReflectionClass;
use ReflectionMethod;
use ReflectionNamedType;
use ReflectionType;
use SplFileInfo;
use Tests\TestCase;

final class ArchitectureBoundaryTest extends TestCase
{
    public function test_domain_does_not_depend_on_outer_layers(): void
    {
        $this->assertNoForbiddenImports('src/Domain', [
            'Application\\',
            'App\\',
            'Illuminate\\',
            'Infrastructure\\',
        ]);
    }

    public function test_application_does_not_depend_on_framework_or_infrastructure(): void
    {
        $this->assertNoForbiddenImports('src/Application', [
            'App\\',
            'Illuminate\\',
            'Infrastructure\\',
        ]);
    }

    public function test_domain_and_application_do_not_read_runtime_config(): void
    {
        $this->assertNoForbiddenPatterns('src/Domain', [
            '/\bconfig\s*\(/',
            '/\benv\s*\(/',
        ]);
        $this->assertNoForbiddenPatterns('src/Application', [
            '/\bconfig\s*\(/',
            '/\benv\s*\(/',
        ]);
    }

    public function test_shared_ports_are_bound_to_laravel_adapters(): void
    {
        $this->assertInstanceOf(EloquentDeadLetterQueue::class, $this->app->make(DeadLetterQueue::class));
        $this->assertInstanceOf(LaravelEventBus::class, $this->app->make(EventBus::class));
        $this->assertInstanceOf(JsonLinesKafkaConsumer::class, $this->app->make(KafkaConsumer::class));
        $this->assertInstanceOf(LaravelLogMetrics::class, $this->app->make(Metrics::class));
        $this->assertInstanceOf(LaravelQueueBus::class, $this->app->make(QueueBus::class));
        $this->assertInstanceOf(DatabaseTransactionManager::class, $this->app->make(TransactionManager::class));
    }

    public function test_repository_ports_return_domain_models_or_nothing(): void
    {
        $violations = [];

        foreach ($this->phpFiles(base_path('src/Application')) as $file) {
            if (! str_ends_with($file->getFilename(), 'Repository.php')) {
                continue;
            }

            $className = $this->className($file);

            if ($className === null) {
                continue;
            }

            if (! class_exists($className) && ! interface_exists($className)) {
                continue;
            }

            $reflection = new ReflectionClass($className);

            foreach ($reflection->getMethods(ReflectionMethod::IS_PUBLIC) as $method) {
                if ($method->getDeclaringClass()->getName() !== $className) {
                    continue;
                }

                if (! $this->repositoryReturnTypeIsAllowed($method, $file)) {
                    $violations[] = sprintf(
                        '%s::%s() returns %s',
                        $className,
                        $method->getName(),
                        $this->returnTypeName($method->getReturnType()),
                    );
                }
            }
        }

        $this->assertSame([], $violations);
    }

    /**
     * @param  array<int, string>  $forbiddenImports
     */
    private function assertNoForbiddenImports(string $directory, array $forbiddenImports): void
    {
        $violations = [];

        foreach ($this->phpFiles(base_path($directory)) as $file) {
            $imports = $this->imports($file);

            foreach ($imports as $import) {
                foreach ($forbiddenImports as $forbiddenImport) {
                    if (str_starts_with($import, $forbiddenImport)) {
                        $violations[] = sprintf(
                            '%s imports %s',
                            str_replace(base_path().'/', '', $file->getPathname()),
                            $import,
                        );
                    }
                }
            }
        }

        $this->assertSame([], $violations);
    }

    /**
     * @param  array<int, string>  $patterns
     */
    private function assertNoForbiddenPatterns(string $directory, array $patterns): void
    {
        $violations = [];

        foreach ($this->phpFiles(base_path($directory)) as $file) {
            $contents = (string) file_get_contents($file->getPathname());

            foreach ($patterns as $pattern) {
                if (preg_match($pattern, $contents) === 1) {
                    $violations[] = sprintf(
                        '%s matches %s',
                        str_replace(base_path().'/', '', $file->getPathname()),
                        $pattern,
                    );
                }
            }
        }

        $this->assertSame([], $violations);
    }

    /**
     * @return iterable<int, SplFileInfo>
     */
    private function phpFiles(string $directory): iterable
    {
        $files = new RecursiveIteratorIterator(new RecursiveDirectoryIterator($directory));

        foreach ($files as $file) {
            if ($file instanceof SplFileInfo && $file->isFile() && $file->getExtension() === 'php') {
                yield $file;
            }
        }
    }

    /**
     * @return array<int, string>
     */
    private function imports(SplFileInfo $file): array
    {
        preg_match_all('/^use\s+([^;]+);/m', (string) file_get_contents($file->getPathname()), $matches);

        return $matches[1];
    }

    private function className(SplFileInfo $file): ?string
    {
        $contents = (string) file_get_contents($file->getPathname());

        if (
            preg_match('/^namespace\s+([^;]+);/m', $contents, $namespace) !== 1
            || preg_match('/^(?:interface|final\s+class|class)\s+([A-Za-z0-9_]+)/m', $contents, $class) !== 1
        ) {
            return null;
        }

        return $namespace[1].'\\'.$class[1];
    }

    private function repositoryReturnTypeIsAllowed(ReflectionMethod $method, SplFileInfo $file): bool
    {
        $type = $method->getReturnType();

        if (! $type instanceof ReflectionNamedType) {
            return false;
        }

        $name = $type->getName();

        if ($name === 'void') {
            return true;
        }

        if ($name === 'array') {
            $docComment = $method->getDocComment();
            $contents = (string) file_get_contents($file->getPathname());

            return $docComment !== false
                && str_contains($docComment, '@return list<')
                && str_contains($contents, 'use Domain\\');
        }

        return str_starts_with($name, 'Domain\\');
    }

    private function returnTypeName(?ReflectionType $type): string
    {
        if ($type instanceof ReflectionNamedType) {
            return ($type->allowsNull() ? '?' : '').$type->getName();
        }

        return 'none';
    }
}
