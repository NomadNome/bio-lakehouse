#!/bin/bash
# Moves matching bio exports from ~/Downloads to inbox/ (only files < 25h old)
INBOX="$HOME/Projects/bio-lakehouse/inbox"
LOG="$HOME/.local/share/bio-lakehouse-logs/inbox-mover.log"
NOW=$(date +%s)
MAX_AGE=90000  # 25 hours in seconds
mkdir -p "$INBOX"

for f in ~/Downloads/export*.zip ~/Downloads/KnownasNoma_workouts*.csv ~/Downloads/Nutrition-Summary*.csv ~/Downloads/File-Export-*/Nutrition-Summary*.csv; do
    [ -f "$f" ] || continue
    FILE_AGE=$(( NOW - $(stat -f %m "$f") ))
    [ "$FILE_AGE" -gt "$MAX_AGE" ] && continue
    mv "$f" "$INBOX/"
    echo "[$(date)] Moved $(basename "$f") to inbox" >> "$LOG"
done
