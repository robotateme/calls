<?php

declare(strict_types=1);

namespace Application\Shared\Ports;

interface ConsoleCommandRunner
{
    /**
     * @param  list<string>  $command
     */
    public function run(array $command, string $stdin, int $timeoutSeconds): ConsoleCommandResult;
}
