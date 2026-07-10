#!/bin/bash
# Bio Lakehouse — Streamlit launcher
# Project lives at ~/Projects/bio-lakehouse (moved off Desktop 2026-07-10 to
# avoid iCloud/Spotlight I/O overhead and macOS TCC launchd restrictions).
PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV="$HOME/.local/share/bio-lakehouse-venv"

# Kill only the process bound to our port (not every "streamlit" on the box)
/usr/sbin/lsof -ti :8501 | xargs kill 2>/dev/null
sleep 1

export BIO_PROJECT_ROOT="$PROJECT_DIR"
export PYTHONPATH="$PROJECT_DIR"
cd "$PROJECT_DIR"
"$VENV/bin/streamlit" run "$PROJECT_DIR/insights_engine/app.py" --server.port 8501 --server.address 127.0.0.1
