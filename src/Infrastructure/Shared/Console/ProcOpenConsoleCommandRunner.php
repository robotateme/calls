<?php

declare(strict_types=1);

namespace Infrastructure\Shared\Console;

use Application\Shared\Ports\ConsoleCommandResult;
use Application\Shared\Ports\ConsoleCommandRunner;
use RuntimeException;

final readonly class ProcOpenConsoleCommandRunner implements ConsoleCommandRunner
{
    public function run(array $command, string $stdin, int $timeoutSeconds): ConsoleCommandResult
    {
        $descriptorSpec = [
            0 => ['pipe', 'r'],
            1 => ['pipe', 'w'],
            2 => ['pipe', 'w'],
        ];

        $process = proc_open($command, $descriptorSpec, $pipes);

        if (! is_resource($process)) {
            throw new RuntimeException('Unable to start console command.');
        }

        fwrite($pipes[0], $stdin);
        fclose($pipes[0]);

        $stdout = stream_get_contents($pipes[1]);
        fclose($pipes[1]);

        $stderr = stream_get_contents($pipes[2]);
        fclose($pipes[2]);

        $exitCode = proc_close($process);

        return new ConsoleCommandResult(
            exitCode: $exitCode,
            stdout: is_string($stdout) ? $stdout : '',
            stderr: is_string($stderr) ? $stderr : '',
        );
    }
}
