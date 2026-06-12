<?php

declare(strict_types=1);

namespace Application\Shared\Ports;

use Closure;

interface TransactionManager
{
    /**
     * @template TReturn
     *
     * @param  Closure(): TReturn  $callback
     * @return TReturn
     */
    public function run(Closure $callback): mixed;
}
