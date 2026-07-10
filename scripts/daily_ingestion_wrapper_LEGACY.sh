#!/bin/bash
set -euo pipefail

PROJECT_DIR="$HOME/Desktop/Bio Lakehouse"
LOG="$PROJECT_DIR/logs/daily-ingestion.log"
ERR_LOG="$PROJECT_DIR/logs/daily-ingestion-error.log"

mkdir -p "$PROJECT_DIR/logs"

if bash "$PROJECT_DIR/run_daily_ingestion.sh" >> "$LOG" 2>> "$ERR_LOG"; then
    echo "[$(date)] Pipeline succeeded" >> "$LOG"
else
    EXIT_CODE=$?
    echo "[$(date)] Pipeline FAILED with exit code $EXIT_CODE" >> "$ERR_LOG"
    osascript -e "display notification \"Daily ingestion failed (exit $EXIT_CODE). Check logs.\" with title \"Bio Lakehouse\" subtitle \"Pipeline Failure\" sound name \"Basso\"" 2>/dev/null || true
    aws sns publish \
        --topic-arn "arn:aws:sns:us-east-1:069899605581:bio-lakehouse-alerts" \
        --subject "Bio Lakehouse: Local Pipeline Failed" \
        --message "Daily ingestion failed at $(date) with exit code $EXIT_CODE. Check $ERR_LOG." \
        --region us-east-1 2>/dev/null || true
    exit $EXIT_CODE
fi
