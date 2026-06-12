FROM php:8.4-cli-bookworm

WORKDIR /var/www/html

ENV APP_ENV=production \
    APP_DEBUG=false \
    LOG_CHANNEL=stderr \
    QUEUE_CONNECTION=redis \
    KAFKA_CONSUMER_ADAPTER=rdkafka \
    KAFKA_PRODUCER_ADAPTER=rdkafka

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        git \
        librdkafka-dev \
        libpq-dev \
        supervisor \
        unzip \
    && docker-php-ext-install \
        opcache \
        pcntl \
        pdo_pgsql \
        sockets \
    && pecl install redis rdkafka \
    && docker-php-ext-enable redis rdkafka opcache \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY --from=composer:2 /usr/bin/composer /usr/bin/composer
COPY docker/production/php.ini /usr/local/etc/php/conf.d/99-calls-production.ini
COPY docker/production/supervisord.conf /etc/supervisor/conf.d/calls.conf

COPY . .

RUN composer install \
        --no-dev \
        --prefer-dist \
        --no-interaction \
        --optimize-autoloader \
    && mkdir -p storage/logs storage/framework/cache storage/framework/sessions storage/framework/views bootstrap/cache \
    && chown -R www-data:www-data storage bootstrap/cache

USER www-data

CMD ["supervisord", "-c", "/etc/supervisor/conf.d/calls.conf"]
