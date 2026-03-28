#!/bin/bash
PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
pkill -f streamlit 2>/dev/null
sleep 1

# Venv + source code live outside ~/Desktop to avoid macOS Spotlight I/O overhead.
VENV="$HOME/.local/share/bio-lakehouse-venv"
SRC="$HOME/.local/share/bio-lakehouse-src"

# Sync .env to the fast path in the background (Desktop reads are slow).
# The file persists across restarts, so first launch uses whatever's already there.
cp -f "$PROJECT_DIR/.env" "$SRC/.env" 2>/dev/null &

export BIO_PROJECT_ROOT="$SRC"
export PYTHONPATH="$SRC"
# Launch from the fast path dir so CWD (sys.path[0]) doesn't shadow PYTHONPATH
cd "$SRC"
"$VENV/bin/streamlit" run "$SRC/insights_engine/app.py" --server.port 8501
