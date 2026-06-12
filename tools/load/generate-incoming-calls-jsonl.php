<?php

declare(strict_types=1);

$count = max(1, (int) ($argv[1] ?? 1000));
$prefix = preg_replace('/[^A-Za-z0-9_-]/', '-', (string) ($argv[2] ?? date('YmdHis')));

for ($i = 1; $i <= $count; $i++) {
    $externalCallId = sprintf('load-%s-%06d', $prefix, $i);

    echo json_encode([
        'topic' => 'incoming-calls',
        'partition' => 0,
        'offset' => $i,
        'key' => $externalCallId,
        'trace_id' => 'trace-'.$externalCallId,
        'payload' => [
            'schema_version' => 1,
            'external_call_id' => $externalCallId,
            'phone' => sprintf('+1555%07d', $i),
            'operator_search_max_attempts' => 3,
            'operator_search_retry_delay_seconds' => 10,
            'operator_search_hangup_policy' => 'missed',
        ],
    ], JSON_THROW_ON_ERROR).PHP_EOL;
}
