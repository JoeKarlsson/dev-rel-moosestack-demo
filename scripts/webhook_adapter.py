#!/usr/bin/env python3
"""
GitHub webhook adapter.

Sits between GitHub and MooseStack. GitHub's webhook payload is a complex
nested object with event type in the X-GitHub-Event header (not the body).
This adapter flattens it to the GithubEvent interface shape that MooseStack
expects.

Usage:
  pip install flask requests
  python3 scripts/webhook_adapter.py

Then point your GitHub webhook at:
  https://<ngrok-id>.ngrok.io/github-webhook
"""

import json
import uuid
from datetime import datetime, timezone

try:
    from flask import Flask, request, jsonify
    import requests
except ImportError:
    print("Missing dependencies. Run: pip install flask requests")
    raise

app = Flask(__name__)

MOOSE_INGEST_URL = "http://localhost:4000/ingest/GithubEvent"


def extract_timestamp(payload: dict, event_type: str) -> str:
    """Best-effort timestamp extraction from various webhook payloads."""
    candidates = [
        payload.get("starred_at"),
        payload.get("created_at"),
        (payload.get("issue") or {}).get("created_at"),
        (payload.get("issue") or {}).get("updated_at"),
        (payload.get("pull_request") or {}).get("created_at"),
        (payload.get("forkee") or {}).get("created_at"),
    ]
    for ts in candidates:
        if ts:
            return ts
    return datetime.now(timezone.utc).isoformat()


@app.route("/github-webhook", methods=["POST"])
def github_webhook():
    delivery_id = request.headers.get("X-GitHub-Delivery", str(uuid.uuid4()))
    event_type = request.headers.get("X-GitHub-Event", "unknown")

    try:
        payload = request.get_json(force=True) or {}
    except Exception:
        payload = {}

    repo = (payload.get("repository") or {}).get("full_name", "unknown/unknown")
    actor = (payload.get("sender") or {}).get("login", "unknown")
    action = payload.get("action", "")
    timestamp = extract_timestamp(payload, event_type)

    moose_event = {
        "deliveryId": delivery_id,
        "timestamp": timestamp,
        "eventType": event_type,
        "repo": repo,
        "actor": actor,
        "action": action,
        "rawPayload": json.dumps(payload),
    }

    try:
        resp = requests.post(MOOSE_INGEST_URL, json=moose_event, timeout=5)
        print(f"[{event_type}] {repo} by {actor} → MooseStack {resp.status_code}")
        return jsonify({"status": "ok", "moose_status": resp.status_code})
    except Exception as e:
        print(f"[{event_type}] Failed to forward to MooseStack: {e}")
        return jsonify({"status": "error", "detail": str(e)}), 500


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    print("GitHub webhook adapter running on :3001")
    print(f"Forwarding to: {MOOSE_INGEST_URL}")
    print()
    print("Point your GitHub webhook at: https://devrel-webhook.joekarlsson.io/github-webhook")
    app.run(host="0.0.0.0", port=3001, debug=False)
