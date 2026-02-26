#!/bin/bash
cd "$(dirname "$0")"
pkill -f streamlit 2>/dev/null
sleep 1
export PYTHONPATH="$(pwd)"
.venv/bin/streamlit run insights_engine/app.py --server.port 8501
