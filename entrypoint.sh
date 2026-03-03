#!/usr/bin/env bash
set -euo pipefail

# ── Pass environment variables into cron ──────────────────────────────
# Cron jobs run in a minimal environment and don't inherit the container's
# env vars. Dump them so the cron job can source them before running.
printenv | grep -v '^no_proxy' > /etc/environment

# ── Write the cron schedule ───────────────────────────────────────────
CRON_SCHEDULE="${CRON_SCHEDULE:-0 2 * * *}"

cat > /etc/cron.d/merger <<EOF
${CRON_SCHEDULE} root . /etc/environment; /app/.venv/bin/opencti-country-merger --force >> /proc/1/fd/1 2>> /proc/1/fd/2
EOF

chmod 0644 /etc/cron.d/merger
crontab /etc/cron.d/merger

# ── Optional: run immediately on startup ──────────────────────────────
if [ "${RUN_NOW:-false}" = "true" ]; then
    echo "[entrypoint] RUN_NOW=true — running merger immediately..."
    /app/.venv/bin/opencti-country-merger --force
fi

# ── Start cron in foreground ──────────────────────────────────────────
echo "[entrypoint] Cron scheduled: ${CRON_SCHEDULE}"
exec cron -f
