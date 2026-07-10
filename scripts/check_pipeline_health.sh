#!/bin/bash
# Noon health check: verify the 8:30 AM pipeline ran today, and that the
# weekly report is not stale (> 8 days old).
LOG_DIR="$HOME/.local/share/bio-lakehouse-logs"
PROJECT_DIR="$HOME/Projects/bio-lakehouse"
TODAY=$(date +%Y-%m-%d)

notify() {
    osascript -e "display notification \"$1\" with title \"Bio Lakehouse\" subtitle \"$2\" sound name \"Basso\"" 2>/dev/null || true
}

# 1. Daily pipeline ran today?
if ! grep -q "\[$TODAY" "$LOG_DIR/daily-ingestion.log" 2>/dev/null; then
    notify "8:30 AM ingestion did not run today. Check launchd." "Missed Run"
fi

# 2. Weekly report fresh? (should regenerate every Monday 7 AM; alert past 8 days)
REPORT="$PROJECT_DIR/reports_output/weekly-report.html"
if [ -f "$REPORT" ]; then
    AGE_DAYS=$(( ( $(date +%s) - $(stat -f %m "$REPORT") ) / 86400 ))
    if [ "$AGE_DAYS" -gt 8 ]; then
        notify "Weekly report is ${AGE_DAYS} days old. Check com.bio-lakehouse.weekly-report." "Stale Report"
    fi
else
    notify "Weekly report has never been generated." "Missing Report"
fi
