#!/bin/bash
# Bio Lakehouse — Streamlit launcher
# Project lives at ~/Projects/bio-lakehouse (moved off Desktop 2026-07-10 to
# avoid iCloud/Spotlight I/O overhead and macOS TCC launchd restrictions).
PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV="$HOME/.local/share/bio-lakehouse-venv"

# Kill only the LISTENING process on our port (not browser sockets connected to it)
/usr/sbin/lsof -ti :8501 -sTCP:LISTEN | xargs kill 2>/dev/null
# Wait for the port to actually free (up to ~5s)
for _ in 1 2 3 4 5 6 7 8 9 10; do
    /usr/sbin/lsof -i :8501 -sTCP:LISTEN >/dev/null 2>&1 || break
    sleep 0.5
done

export BIO_PROJECT_ROOT="$PROJECT_DIR"
export PYTHONPATH="$PROJECT_DIR"
cd "$PROJECT_DIR"
"$VENV/bin/streamlit" run "$PROJECT_DIR/insights_engine/app.py" --server.port 8501 --server.address 127.0.0.1
