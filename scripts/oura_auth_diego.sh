#!/bin/bash
# One-shot Oura OAuth authorization for Diego's instance.
# Reads the client credentials from .env.diego (gitignored); opens the
# browser for Diego to log in; stores the token bundle in SSM.
set -euo pipefail
PROJECT_DIR="$HOME/Projects/bio-lakehouse"
set -a; source "$PROJECT_DIR/.env.diego"; set +a
exec "$HOME/.local/share/bio-lakehouse-venv/bin/python" \
    "$PROJECT_DIR/scripts/oura_oauth_authorize.py" \
    --client-id "$OURA_CLIENT_ID" \
    --ssm-param /bio-lakehouse-diego/oura-oauth
