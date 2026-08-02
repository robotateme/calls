<?php

declare(strict_types=1);

namespace Infrastructure\Shared\Observability;

final readonly class PrometheusMetricsRenderer
{
    public function __construct(private PrometheusMetricsStore $store) {}

    public function render(): string
    {
        $lines = [];
        $snapshot = $this->store->snapshot();
        $declared = [];

        foreach ($snapshot as $series) {
            $metricName = $series['name'];

            if (! isset($declared[$metricName])) {
                $lines[] = '# HELP '.$metricName.' Application metric '.$metricName;
                $lines[] = '# TYPE '.$metricName.' '.$series['type'];
                $declared[$metricName] = true;
            }

            $labels = $this->renderLabels($series['labels']);

            if ($series['type'] === 'summary') {
                $lines[] = $metricName.'_sum'.$labels.' '.$this->formatValue((float) $series['sum']);
                $lines[] = $metricName.'_count'.$labels.' '.$this->formatValue((int) $series['count']);

                continue;
            }

            $lines[] = $metricName.$labels.' '.$this->formatValue($series['value']);
        }

        return $lines === [] ? '' : implode("\n", $lines)."\n";
    }

    /**
     * @param  array<string, string>  $labels
     */
    private function renderLabels(array $labels): string
    {
        if ($labels === []) {
            return '';
        }

        $parts = [];

        foreach ($labels as $key => $value) {
            $parts[] = $key.'="'.$this->escapeLabelValue($value).'"';
        }

        return '{'.implode(',', $parts).'}';
    }

    private function escapeLabelValue(string $value): string
    {
        return str_replace(
            ['\\', "\n", '"'],
            ['\\\\', '\n', '\"'],
            $value,
        );
    }

    private function formatValue(int|float $value): string
    {
        if (is_int($value)) {
            return (string) $value;
        }

        $formatted = number_format($value, 6, '.', '');

        return rtrim(rtrim($formatted, '0'), '.');
    }
}
