<?php

declare(strict_types=1);

namespace Infrastructure\Shared\Kafka;

use ReflectionClass;
use ReflectionException;
use ReflectionObject;
use RuntimeException;

final readonly class RdkafkaRuntime
{
    /**
     * @param  list<mixed>  $arguments
     *
     * @throws RuntimeException
     */
    public function newInstance(string $className, array $arguments = []): object
    {
        if (! class_exists($className)) {
            throw new RuntimeException(sprintf('php-rdkafka class "%s" is not available.', $className));
        }

        try {
            return (new ReflectionClass($className))->newInstanceArgs($arguments);
        } catch (ReflectionException $exception) {
            throw new RuntimeException(sprintf('Unable to instantiate "%s".', $className), previous: $exception);
        }
    }

    /**
     * @param  list<mixed>  $arguments
     *
     * @throws RuntimeException
     */
    public function invoke(object $object, string $method, array $arguments = []): mixed
    {
        try {
            return (new ReflectionObject($object))->getMethod($method)->invokeArgs($object, $arguments);
        } catch (ReflectionException $exception) {
            throw new RuntimeException(sprintf('Unable to call "%s::%s".', $object::class, $method), previous: $exception);
        }
    }

    /**
     * @throws RuntimeException
     */
    public function property(object $object, string $property): mixed
    {
        try {
            return (new ReflectionObject($object))->getProperty($property)->getValue($object);
        } catch (ReflectionException $exception) {
            throw new RuntimeException(sprintf('Unable to read "%s::$%s".', $object::class, $property), previous: $exception);
        }
    }

    /**
     * @throws RuntimeException
     */
    public function intConstant(string $name): int
    {
        if (! defined($name)) {
            throw new RuntimeException(sprintf('php-rdkafka constant "%s" is not available.', $name));
        }

        $value = constant($name);

        if (! is_int($value)) {
            throw new RuntimeException(sprintf('php-rdkafka constant "%s" must be int.', $name));
        }

        return $value;
    }
}
