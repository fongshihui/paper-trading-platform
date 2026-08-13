#!/usr/bin/env bash
set -euo pipefail

JOBMANAGER_HOST="${FLINK_JOBMANAGER_HOST:-jobmanager}"
JOBMANAGER_PORT="${FLINK_JOBMANAGER_PORT:-8081}"
JOB_CLASS="${FLINK_JOB_CLASS:-com.example.trading.SignalJob}"
JOB_JAR="${FLINK_JOB_JAR:-/opt/flink/usrlib/flink-jobs.jar}"

echo "Waiting for Flink JobManager at ${JOBMANAGER_HOST}:${JOBMANAGER_PORT}..."
for _ in $(seq 1 60); do
  if curl -fsS "http://${JOBMANAGER_HOST}:${JOBMANAGER_PORT}/overview" >/dev/null; then
    break
  fi
  sleep 2
done

if ! curl -fsS "http://${JOBMANAGER_HOST}:${JOBMANAGER_PORT}/overview" >/dev/null; then
  echo "Flink JobManager did not become ready in time." >&2
  exit 1
fi

echo "Submitting ${JOB_CLASS} from ${JOB_JAR}..."
flink run \
  -d \
  -m "${JOBMANAGER_HOST}:8081" \
  -c "${JOB_CLASS}" \
  "${JOB_JAR}"
