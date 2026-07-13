#!/bin/bash
# Bio Lakehouse — Streamlit launcher (multi-instance aware)
# Project lives at ~/Projects/bio-lakehouse (moved off Desktop 2026-07-10 to
# avoid iCloud/Spotlight I/O overhead and macOS TCC launchd restrictions).
#
# Default launch serves the primary instance (.env, port 8501).
# A second instance: ENV_FILE=.env.diego bash run_streamlit.sh  → port from
# that file's STREAMLIT_PORT (e.g. 8502). Both can run simultaneously.
PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV="$HOME/.local/share/bio-lakehouse-venv"

# Load the instance's env file so BIO_* vars are set before config.py runs.
# (config.py's load_dotenv does not override already-exported vars.)
ENV_FILE="${ENV_FILE:-$PROJECT_DIR/.env}"
if [ -f "$ENV_FILE" ]; then
    set -a
    source "$ENV_FILE"
    set +a
fi

PORT="${STREAMLIT_PORT:-8501}"

# Kill only the LISTENING process on our port (not browser sockets connected to it)
/usr/sbin/lsof -ti :"$PORT" -sTCP:LISTEN | xargs kill 2>/dev/null
# Wait for the port to actually free (up to ~5s)
for _ in 1 2 3 4 5 6 7 8 9 10; do
    /usr/sbin/lsof -i :"$PORT" -sTCP:LISTEN >/dev/null 2>&1 || break
    sleep 0.5
done

export BIO_PROJECT_ROOT="$PROJECT_DIR"
export PYTHONPATH="$PROJECT_DIR"
cd "$PROJECT_DIR"
"$VENV/bin/streamlit" run "$PROJECT_DIR/insights_engine/app.py" --server.port "$PORT" --server.address 127.0.0.1
