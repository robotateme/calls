<?php

declare(strict_types=1);

namespace App\Providers;

use Application\Calls\Ports\CallProcessingLogger;
use Application\Calls\Ports\CallProcessingQueue;
use Application\Calls\Ports\CallProcessingRetryQueue;
use Application\Calls\Ports\CallReadRepository;
use Application\Calls\Ports\CallWriteRepository;
use Application\Clients\Ports\ClientReadRepository;
use Application\Operators\Ports\OperatorReservationRepository;
use Application\Shared\Ports\ConsoleCommandRunner;
use Application\Shared\Ports\DeadLetterQueue;
use Application\Shared\Ports\EventBus;
use Application\Shared\Ports\KafkaConsumer;
use Application\Shared\Ports\Metrics;
use Application\Shared\Ports\QueueBus;
use Application\Shared\Ports\TransactionManager;
use Application\Telephony\Ports\TelephonyCommandOutboxReader;
use Application\Telephony\Ports\TelephonyCommandOutboxWriter;
use Application\Telephony\Ports\TelephonyCommandPublisher;
use Application\Telephony\Ports\TelephonyOutboxWriteRepository;
use Illuminate\Contracts\Cache\Factory as CacheFactory;
use Illuminate\Contracts\Foundation\Application;
use Illuminate\Support\ServiceProvider;
use Infrastructure\Calls\Logging\LaravelCallProcessingLogger;
use Infrastructure\Calls\Persistence\EloquentCallRepository;
use Infrastructure\Calls\Queue\LaravelCallProcessingQueue;
use Infrastructure\Calls\Queue\LaravelCallProcessingRetryQueue;
use Infrastructure\Clients\Persistence\EloquentClientReadRepository;
use Infrastructure\Operators\Persistence\EloquentOperatorReservationRepository;
use Infrastructure\Shared\Bus\LaravelEventBus;
use Infrastructure\Shared\Bus\LaravelQueueBus;
use Infrastructure\Shared\Console\ProcOpenConsoleCommandRunner;
use Infrastructure\Shared\Kafka\EloquentDeadLetterQueue;
use Infrastructure\Shared\Kafka\JsonLinesKafkaConsumer;
use Infrastructure\Shared\Kafka\RdkafkaKafkaConsumer;
use Infrastructure\Shared\Observability\CompositeMetrics;
use Infrastructure\Shared\Observability\LaravelLogMetrics;
use Infrastructure\Shared\Observability\PrometheusMetricsRenderer;
use Infrastructure\Shared\Observability\PrometheusMetricsStore;
use Infrastructure\Shared\Persistence\DatabaseTransactionManager;
use Infrastructure\Telephony\Outbox\EloquentTelephonyCommandOutbox;
use Infrastructure\Telephony\Outbox\EloquentTelephonyOutboxRepository;
use Infrastructure\Telephony\Outbox\KafkaConsoleTelephonyCommandPublisher;
use Infrastructure\Telephony\Outbox\RdkafkaTelephonyCommandPublisher;
use Psr\Log\LoggerInterface;

final class ArchitectureServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        $this->app->bind(CallReadRepository::class, EloquentCallRepository::class);
        $this->app->bind(CallWriteRepository::class, EloquentCallRepository::class);
        $this->app->bind(CallProcessingQueue::class, LaravelCallProcessingQueue::class);
        $this->app->bind(CallProcessingRetryQueue::class, LaravelCallProcessingRetryQueue::class);
        $this->app->bind(ClientReadRepository::class, EloquentClientReadRepository::class);
        $this->app->bind(OperatorReservationRepository::class, EloquentOperatorReservationRepository::class);
        $this->app->bind(CallProcessingLogger::class, LaravelCallProcessingLogger::class);
        $this->app->bind(TelephonyCommandOutboxReader::class, EloquentTelephonyCommandOutbox::class);
        $this->app->bind(TelephonyCommandOutboxWriter::class, EloquentTelephonyCommandOutbox::class);
        $this->app->bind(TelephonyOutboxWriteRepository::class, EloquentTelephonyOutboxRepository::class);
        $this->app->bind(TelephonyCommandPublisher::class, $this->telephonyCommandPublisherAdapter());

        $this->app->bind(ConsoleCommandRunner::class, ProcOpenConsoleCommandRunner::class);
        $this->app->bind(DeadLetterQueue::class, EloquentDeadLetterQueue::class);
        $this->app->bind(EventBus::class, LaravelEventBus::class);
        $this->app->bind(KafkaConsumer::class, $this->kafkaConsumerAdapter());
        $this->app->bind(QueueBus::class, LaravelQueueBus::class);
        $this->app->bind(TransactionManager::class, DatabaseTransactionManager::class);

        $this->app->singleton(PrometheusMetricsStore::class, function (Application $app): PrometheusMetricsStore {
            $configuredStore = config('calls.metrics_cache_store');
            $storeName = is_string($configuredStore) && $configuredStore !== ''
                ? $configuredStore
                : null;

            return new PrometheusMetricsStore(
                $app->make(CacheFactory::class),
                $storeName,
                (string) config('calls.metrics_cache_prefix'),
            );
        });
        $this->app->singleton(PrometheusMetricsRenderer::class, fn (Application $app): PrometheusMetricsRenderer => new PrometheusMetricsRenderer(
            $app->make(PrometheusMetricsStore::class),
        ));
        $this->app->bind(Metrics::class, fn (Application $app): Metrics => new CompositeMetrics([
            new LaravelLogMetrics($app->make(LoggerInterface::class)),
            $app->make(PrometheusMetricsStore::class),
        ]));
    }

    private function kafkaConsumerAdapter(): string
    {
        return match ((string) config('calls.kafka_consumer_adapter')) {
            'rdkafka' => RdkafkaKafkaConsumer::class,
            default => JsonLinesKafkaConsumer::class,
        };
    }

    private function telephonyCommandPublisherAdapter(): string
    {
        return match ((string) config('calls.kafka_producer_adapter')) {
            'rdkafka' => RdkafkaTelephonyCommandPublisher::class,
            default => KafkaConsoleTelephonyCommandPublisher::class,
        };
    }
}
