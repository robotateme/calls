#!/usr/bin/env bash
set -Eeuo pipefail

profile="${LOAD_PROFILE:-custom}"

case "${profile}" in
    smoke)
        export LOAD_MODE="${LOAD_MODE:-stress}"
        export LOAD_COUNT="${LOAD_COUNT:-100}"
        export LOAD_RATE_PER_SECOND="${LOAD_RATE_PER_SECOND:-0}"
        export LOAD_BATCH_SIZE="${LOAD_BATCH_SIZE:-50}"
        export LOAD_WORKERS="${LOAD_WORKERS:-1}"
        export LOAD_RETRY_WORKERS="${LOAD_RETRY_WORKERS:-1}"
        export LOAD_OUTBOX_PUBLISHERS="${LOAD_OUTBOX_PUBLISHERS:-1}"
        export LOAD_OUTBOX_LIMIT="${LOAD_OUTBOX_LIMIT:-100}"
        export LOAD_OPERATORS="${LOAD_OPERATORS:-150}"
        export LOAD_DRAIN_TIMEOUT_SECONDS="${LOAD_DRAIN_TIMEOUT_SECONDS:-60}"
        export LOAD_SNAPSHOT_INTERVAL_SECONDS="${LOAD_SNAPSHOT_INTERVAL_SECONDS:-0}"
        export LOAD_FAKE_TELEPHONY_PRODUCER="${LOAD_FAKE_TELEPHONY_PRODUCER:-1}"
        ;;
    stress-large)
        export LOAD_MODE="${LOAD_MODE:-stress}"
        export LOAD_COUNT="${LOAD_COUNT:-250000}"
        export LOAD_RATE_PER_SECOND="${LOAD_RATE_PER_SECOND:-0}"
        export LOAD_BATCH_SIZE="${LOAD_BATCH_SIZE:-5000}"
        export LOAD_WORKERS="${LOAD_WORKERS:-16}"
        export LOAD_RETRY_WORKERS="${LOAD_RETRY_WORKERS:-2}"
        export LOAD_OUTBOX_PUBLISHERS="${LOAD_OUTBOX_PUBLISHERS:-4}"
        export LOAD_OUTBOX_LIMIT="${LOAD_OUTBOX_LIMIT:-2000}"
        export LOAD_OPERATORS="${LOAD_OPERATORS:-300000}"
        export LOAD_DRAIN_TIMEOUT_SECONDS="${LOAD_DRAIN_TIMEOUT_SECONDS:-900}"
        export LOAD_SNAPSHOT_INTERVAL_SECONDS="${LOAD_SNAPSHOT_INTERVAL_SECONDS:-60}"
        export LOAD_FAKE_TELEPHONY_PRODUCER="${LOAD_FAKE_TELEPHONY_PRODUCER:-1}"
        ;;
    soak-large)
        export LOAD_MODE="${LOAD_MODE:-soak}"
        export LOAD_DURATION_SECONDS="${LOAD_DURATION_SECONDS:-10800}"
        export LOAD_RATE_PER_SECOND="${LOAD_RATE_PER_SECOND:-50}"
        export LOAD_BATCH_SIZE="${LOAD_BATCH_SIZE:-500}"
        export LOAD_WORKERS="${LOAD_WORKERS:-8}"
        export LOAD_RETRY_WORKERS="${LOAD_RETRY_WORKERS:-2}"
        export LOAD_OUTBOX_PUBLISHERS="${LOAD_OUTBOX_PUBLISHERS:-2}"
        export LOAD_OUTBOX_LIMIT="${LOAD_OUTBOX_LIMIT:-1000}"
        export LOAD_OPERATORS="${LOAD_OPERATORS:-750000}"
        export LOAD_DRAIN_TIMEOUT_SECONDS="${LOAD_DRAIN_TIMEOUT_SECONDS:-1200}"
        export LOAD_SNAPSHOT_INTERVAL_SECONDS="${LOAD_SNAPSHOT_INTERVAL_SECONDS:-60}"
        export LOAD_FAKE_TELEPHONY_PRODUCER="${LOAD_FAKE_TELEPHONY_PRODUCER:-1}"
        ;;
    custom)
        ;;
    *)
        echo "Unsupported LOAD_PROFILE=${profile}. Expected smoke, stress-large, soak-large or custom." >&2
        exit 2
        ;;
esac

mode="${LOAD_MODE:-stress}"
count="${LOAD_COUNT:-10000}"
duration_seconds="${LOAD_DURATION_SECONDS:-0}"
rate_per_second="${LOAD_RATE_PER_SECOND:-0}"
batch_size="${LOAD_BATCH_SIZE:-1000}"
workers="${LOAD_WORKERS:-4}"
retry_workers="${LOAD_RETRY_WORKERS:-1}"
operators="${LOAD_OPERATORS:-20000}"
clients="${LOAD_CLIENTS:-0}"
outbox_publishers="${LOAD_OUTBOX_PUBLISHERS:-1}"
outbox_limit="${LOAD_OUTBOX_LIMIT:-500}"
outbox_interval_seconds="${LOAD_OUTBOX_INTERVAL_SECONDS:-1}"
drain_timeout_seconds="${LOAD_DRAIN_TIMEOUT_SECONDS:-180}"
snapshot_interval_seconds="${LOAD_SNAPSHOT_INTERVAL_SECONDS:-0}"
progress_interval_seconds="${LOAD_PROGRESS_INTERVAL_SECONDS:-30}"
fail_on_dlq="${LOAD_FAIL_ON_DLQ:-1}"
prefix="${LOAD_PREFIX:-$(date -u +%Y%m%d%H%M%S)-${profile}}"
report_dir="${LOAD_REPORT_DIR:-storage/load-reports/${prefix}}"

if [[ "${mode}" != "stress" && "${mode}" != "soak" ]]; then
    echo "Unsupported LOAD_MODE=${mode}. Expected stress or soak." >&2
    exit 2
fi

if [[ "${mode}" == "soak" && "${duration_seconds}" -le 0 ]]; then
    duration_seconds=900
fi

mkdir -p "${report_dir}"

log() {
    printf '[%s] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"
}

pids=()
cleanup() {
    for pid in "${pids[@]:-}"; do
        if kill -0 "${pid}" >/dev/null 2>&1; then
            kill "${pid}" >/dev/null 2>&1 || true
        fi
    done
}
trap cleanup EXIT

if [[ "${LOAD_FAKE_TELEPHONY_PRODUCER:-0}" == "1" ]]; then
    export KAFKA_CONSOLE_PRODUCER_BINARY="${KAFKA_CONSOLE_PRODUCER_BINARY:-$(pwd)/tools/load/fake-kafka-console-producer.sh}"
fi

max_worker_time=$((duration_seconds + drain_timeout_seconds + 120))
if [[ "${mode}" == "stress" ]]; then
    estimated_stress_seconds=600
    if [[ "${rate_per_second}" -gt 0 ]]; then
        estimated_stress_seconds=$((count / rate_per_second + 60))
    fi

    max_worker_time=$((estimated_stress_seconds + drain_timeout_seconds + 120))
fi

log "Preparing dataset: operators=${operators} clients=${clients}"
php tools/load/prepare-dataset.php "${operators}" "${clients}" | tee "${report_dir}/prepare.log"

php tools/load/snapshot.php >"${report_dir}/snapshot-before.json"

log "Starting workers: calls=${workers} retry=${retry_workers} outbox_publishers=${outbox_publishers}"
for i in $(seq 1 "${workers}"); do
    php artisan queue:work redis --queue=calls --tries=1 --timeout=0 --sleep=1 --max-time="${max_worker_time}" \
        >"${report_dir}/calls-worker-${i}.log" 2>&1 &
    pids+=("$!")
done

for i in $(seq 1 "${retry_workers}"); do
    php artisan queue:work redis --queue=calls-retry --tries=1 --timeout=0 --sleep=1 --max-time="${max_worker_time}" \
        >"${report_dir}/retry-worker-${i}.log" 2>&1 &
    pids+=("$!")
done

for i in $(seq 1 "${outbox_publishers}"); do
    (
        while true; do
            php artisan calls:telephony-outbox:publish --limit="${outbox_limit}" --retry-delay=1 --max-attempts=3
            sleep "${outbox_interval_seconds}"
        done
    ) >"${report_dir}/outbox-publisher-${i}.log" 2>&1 &
    pids+=("$!")
done

if [[ "${snapshot_interval_seconds}" -gt 0 ]]; then
    (
        while true; do
            timestamp="$(date -u +%Y%m%d%H%M%S)"
            php tools/load/snapshot.php >"${report_dir}/snapshot-${timestamp}.json" || true
            sleep "${snapshot_interval_seconds}"
        done
    ) >"${report_dir}/snapshot-loop.log" 2>&1 &
    pids+=("$!")
fi

sent=0
started_at="$(date +%s)"
last_progress_at="${started_at}"
log "Producing JSONL load: profile=${profile} mode=${mode} count=${count} duration_seconds=${duration_seconds} rate_per_second=${rate_per_second} batch_size=${batch_size}"

while true; do
    now="$(date +%s)"

    if [[ "${mode}" == "stress" && "${sent}" -ge "${count}" ]]; then
        break
    fi

    if [[ "${mode}" == "soak" && $((now - started_at)) -ge "${duration_seconds}" ]]; then
        break
    fi

    current_batch="${batch_size}"
    if [[ "${mode}" == "stress" ]]; then
        remaining=$((count - sent))
        if [[ "${remaining}" -lt "${current_batch}" ]]; then
            current_batch="${remaining}"
        fi
    fi

    batch_prefix="${prefix}-${sent}"
    php tools/load/generate-incoming-calls-jsonl.php "${current_batch}" "${batch_prefix}" \
        | php artisan calls:kafka:consume incoming-calls --limit="${current_batch}" --timeout-ms=5000 \
        >>"${report_dir}/consumer.log" 2>&1

    sent=$((sent + current_batch))
    printf 'sent=%d\nelapsed_seconds=%d\n' "${sent}" "$(( $(date +%s) - started_at ))" >"${report_dir}/progress.env"

    now="$(date +%s)"
    if [[ $((now - last_progress_at)) -ge "${progress_interval_seconds}" ]]; then
        log "Progress: sent=${sent} elapsed_seconds=$((now - started_at))"
        last_progress_at="${now}"
    fi

    if [[ "${rate_per_second}" -gt 0 ]]; then
        expected_elapsed=$((sent / rate_per_second))
        actual_elapsed=$(( $(date +%s) - started_at ))
        if [[ "${expected_elapsed}" -gt "${actual_elapsed}" ]]; then
            sleep $((expected_elapsed - actual_elapsed))
        fi
    fi
done

finished_producing_at="$(date +%s)"
log "Finished producing: sent=${sent} elapsed_seconds=$((finished_producing_at - started_at))"

deadline=$(( $(date +%s) + drain_timeout_seconds ))
while true; do
    queue_work="$(php tools/load/snapshot.php --queue-work)"
    outbox_work=0
    if [[ "${outbox_publishers}" -gt 0 ]]; then
        outbox_work="$(php tools/load/snapshot.php --outbox-work)"
    fi

    if [[ "${queue_work}" -eq 0 && "${outbox_work}" -eq 0 ]]; then
        break
    fi

    if [[ "$(date +%s)" -ge "${deadline}" ]]; then
        log "Drain timeout reached: queue_work=${queue_work} outbox_work=${outbox_work}"
        break
    fi

    sleep 2
done

php artisan calls:metrics:snapshot >"${report_dir}/metrics-snapshot.log" 2>&1 || true
php tools/load/snapshot.php >"${report_dir}/snapshot-after.json"

queue_work="$(php tools/load/snapshot.php --queue-work)"
outbox_work=0
if [[ "${outbox_publishers}" -gt 0 ]]; then
    outbox_work="$(php tools/load/snapshot.php --outbox-work)"
fi
dead_letters="$(php tools/load/snapshot.php --dead-letters)"

cat >"${report_dir}/summary.txt" <<EOF
profile=${profile}
mode=${mode}
sent=${sent}
producer_elapsed_seconds=$((finished_producing_at - started_at))
queue_work=${queue_work}
outbox_work=${outbox_work}
dead_letters=${dead_letters}
report_dir=${report_dir}
EOF

cat "${report_dir}/summary.txt"

if [[ "${queue_work}" -ne 0 ]]; then
    echo "Load test failed: queue backlog remains." >&2
    exit 1
fi

if [[ "${outbox_publishers}" -gt 0 && "${outbox_work}" -ne 0 ]]; then
    echo "Load test failed: outbox backlog remains." >&2
    exit 1
fi

if [[ "${fail_on_dlq}" == "1" && "${dead_letters}" -ne 0 ]]; then
    echo "Load test failed: unresolved dead letters exist." >&2
    exit 1
fi

log "Load test completed successfully."
