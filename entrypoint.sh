#!/usr/bin/env bash
set -euo pipefail

CLI="/app/.venv/bin/opencti-country-merger"

# ── Resolve which command to run ─────────────────────────────────
# MERGER_COMMAND: "merge", "fix-names", or any valid subcommand.
# Defaults to "merge".
MERGER_COMMAND="${MERGER_COMMAND:-merge}"

# ── Pass environment variables into cron ─────────────────────────
# Cron jobs run in a minimal environment and don't inherit the container's
# env vars. Dump them so the cron job can source them before running.
printenv | grep -v '^no_proxy' > /etc/environment

# ── Write the cron schedule ──────────────────────────────────────
CRON_SCHEDULE="${CRON_SCHEDULE:-0 2 * * *}"

cat > /etc/cron.d/merger <<EOF
${CRON_SCHEDULE} root . /etc/environment; ${CLI} ${MERGER_COMMAND} --force >> /proc/1/fd/1 2>> /proc/1/fd/2
EOF

chmod 0644 /etc/cron.d/merger
crontab /etc/cron.d/merger

# ── Optional: run immediately on startup ─────────────────────────
if [ "${RUN_NOW:-false}" = "true" ]; then
    echo "[entrypoint] RUN_NOW=true — running '${MERGER_COMMAND}' immediately..."
    ${CLI} ${MERGER_COMMAND} --force
fi

# ── Start cron in foreground ─────────────────────────────────────
echo "[entrypoint] Command: ${MERGER_COMMAND} | Cron: ${CRON_SCHEDULE}"
exec cron -f
