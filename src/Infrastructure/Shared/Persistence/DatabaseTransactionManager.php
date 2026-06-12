<?php

declare(strict_types=1);

namespace Infrastructure\Shared\Persistence;

use Application\Shared\Ports\TransactionManager;
use Closure;
use Illuminate\Support\Facades\DB;

final readonly class DatabaseTransactionManager implements TransactionManager
{
    /**
     * @template TReturn
     *
     * @param  Closure(): TReturn  $callback
     * @return TReturn
     */
    public function run(Closure $callback): mixed
    {
        return DB::transaction($callback);
    }
}
