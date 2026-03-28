#!/bin/bash
# Noon health check: verify 8:30 AM pipeline ran today
LOG="$HOME/Desktop/Bio Lakehouse/logs/daily-ingestion.log"
TODAY=$(date +%Y-%m-%d)

if ! grep -q "\[$TODAY" "$LOG" 2>/dev/null; then
    osascript -e "display notification \"8:30 AM ingestion did not run today. Check launchd.\" with title \"Bio Lakehouse\" subtitle \"Missed Run\" sound name \"Basso\"" 2>/dev/null || true
fi
