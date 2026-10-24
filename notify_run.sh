#!/usr/bin/env bash
# Run a command and, if it fails, email the tail of its output via the
# datamailer CLI. Output is still streamed to stdout/stderr so cron logs
# keep everything.
#
# Usage:
#   ./notify_run.sh <label> <command...>
#
# Example (crontab):
#   ./notify_run.sh cmp /home/alexey/.local/bin/uv run run_pipeline.py --db cmp --schema prod
#
# Config (env, with sensible defaults):
#   BACKUP_ALERT_EMAIL   recipient        (default: alexey.s.grigoriev@gmail.com)
#   DATAMAILER_BIN       datamailer path  (default: $HOME/.local/bin/datamailer)
# Relay client sender id alerts must resolve to
# DTC Alerts <alerts@dtcdev.click>; do not rely on the client's default sender.

set -uo pipefail

if [ "$#" -lt 2 ]; then
    echo "usage: notify_run.sh <label> <command...>" >&2
    exit 2
fi

label="$1"
shift

recipient="${BACKUP_ALERT_EMAIL:-alexey.s.grigoriev@gmail.com}"
datamailer_bin="${DATAMAILER_BIN:-$HOME/.local/bin/datamailer}"
started="$(date '+%Y-%m-%d %H:%M:%S %Z')"
out="$(mktemp)"

# Run the command, mirroring output to the log and to a temp file.
"$@" 2>&1 | tee "$out"
status="${PIPESTATUS[0]}"

if [ "$status" -ne 0 ]; then
    {
        echo "Something didn't work — the '$label' job failed."
        echo
        echo "Host:     $(hostname)"
        echo "Started:  $started"
        echo "Exit code: $status"
        echo "Command:  $*"
        echo
        echo "----- last 50 lines of output -----"
        tail -n 50 "$out"
    } | "$datamailer_bin" send \
        --from alerts \
        --to "$recipient" \
        --subject "Backup failure: $label (exit $status)"
fi

rm -f "$out"
exit "$status"
