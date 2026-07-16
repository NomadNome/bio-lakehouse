#!/usr/bin/env python3
"""One-shot Oura OAuth2 authorizer.

Oura deprecated Personal Access Tokens (2026); API access for new users goes
through an OAuth2 app. This script performs the authorization-code flow once
and stores the resulting token bundle in SSM for the ingest Lambda:

    python scripts/oura_oauth_authorize.py \
        --ssm-param /bio-lakehouse-diego/oura-oauth \
        --client-id <id> [--client-secret-env OURA_CLIENT_SECRET]

Flow: opens the browser to Oura's authorize page (the account owner logs in
themselves), catches the redirect on http://localhost:8080/callback,
exchanges the code, and writes JSON {client_id, client_secret, access_token,
refresh_token, expires_at} to the SSM SecureString.

The registered redirect URI on the Oura app MUST be exactly
http://localhost:8080/callback.
"""

import argparse
import http.server
import json
import os
import sys
import threading
import time
import urllib.parse
import urllib.request
import webbrowser

AUTHORIZE_URL = "https://cloud.ouraring.com/oauth/authorize"
TOKEN_URL = "https://api.ouraring.com/oauth/token"
REDIRECT_URI = "http://localhost:8080/callback"
SCOPES = "email personal daily heartrate workout tag session spo2"

_auth_code = {}


class _CallbackHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        query = urllib.parse.urlparse(self.path).query
        params = urllib.parse.parse_qs(query)
        if "code" in params:
            _auth_code["code"] = params["code"][0]
            body = b"<h2>Authorized! You can close this tab and return to the terminal.</h2>"
        else:
            _auth_code["error"] = params.get("error", ["unknown"])[0]
            body = b"<h2>Authorization failed - check the terminal.</h2>"
        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--client-id", required=True)
    ap.add_argument(
        "--client-secret-env",
        default="OURA_CLIENT_SECRET",
        help="Env var holding the client secret (avoids shell-history leaks)",
    )
    ap.add_argument("--ssm-param", required=True)
    ap.add_argument("--region", default="us-east-1")
    args = ap.parse_args()

    client_secret = os.environ.get(args.client_secret_env, "")
    if not client_secret:
        print(f"Set {args.client_secret_env} in the environment first.")
        sys.exit(1)

    server = http.server.HTTPServer(("127.0.0.1", 8080), _CallbackHandler)
    threading.Thread(target=server.handle_request, daemon=True).start()

    authorize = (
        f"{AUTHORIZE_URL}?response_type=code"
        f"&client_id={urllib.parse.quote(args.client_id)}"
        f"&redirect_uri={urllib.parse.quote(REDIRECT_URI)}"
        f"&scope={urllib.parse.quote(SCOPES)}"
    )
    print("Opening browser for Oura login (the ring's owner should sign in)...")
    print(f"If it doesn't open, visit:\n  {authorize}\n")
    webbrowser.open(authorize)

    for _ in range(300):  # wait up to 5 minutes
        if _auth_code:
            break
        time.sleep(1)
    server.server_close()

    if "code" not in _auth_code:
        print(f"No authorization code received ({_auth_code.get('error', 'timeout')}).")
        sys.exit(1)

    print("Code received — exchanging for tokens...")
    data = urllib.parse.urlencode(
        {
            "grant_type": "authorization_code",
            "code": _auth_code["code"],
            "redirect_uri": REDIRECT_URI,
            "client_id": args.client_id,
            "client_secret": client_secret,
        }
    ).encode()
    with urllib.request.urlopen(urllib.request.Request(TOKEN_URL, data=data)) as resp:
        tokens = json.loads(resp.read().decode())

    bundle = {
        "client_id": args.client_id,
        "client_secret": client_secret,
        "access_token": tokens["access_token"],
        "refresh_token": tokens.get("refresh_token", ""),
        "expires_at": int(time.time()) + int(tokens.get("expires_in", 86400)),
    }

    import boto3

    boto3.client("ssm", region_name=args.region).put_parameter(
        Name=args.ssm_param,
        Type="SecureString",
        Value=json.dumps(bundle),
        Overwrite=True,
    )
    print(f"Token bundle stored in SSM: {args.ssm_param}")
    print("Done — the ingest Lambda will refresh tokens automatically from here.")


if __name__ == "__main__":
    main()
