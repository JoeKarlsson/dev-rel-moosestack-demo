#!/usr/bin/env python3
"""
GitHub webhook adapter + SSE event stream.

Sits between GitHub and MooseStack. GitHub's webhook payload is a complex
nested object with event type in the X-GitHub-Event header (not the body).
This adapter flattens it to the GithubEvent interface shape that MooseStack
expects, and simultaneously broadcasts each event to connected SSE clients
so the dashboard updates in real time without polling.

Usage:
  pip install flask requests
  python3 scripts/webhook_adapter.py

Endpoints:
  POST /github-webhook  — GitHub sends webhooks here
  GET  /events          — SSE stream for the dashboard
  GET  /health          — Health check
"""

import json
import queue
import threading
import uuid
from datetime import datetime, timezone

try:
    from flask import Flask, request, jsonify, Response
    import requests
except ImportError:
    print("Missing dependencies. Run: pip install flask requests")
    raise

app = Flask(__name__)

MOOSE_INGEST_URL = "http://localhost:4000/ingest/GithubEvent"

# SSE client registry — one Queue per connected browser tab
_clients: list[queue.Queue] = []
_clients_lock = threading.Lock()


def broadcast(event_data: dict):
    """Push an event to all connected SSE clients."""
    msg = json.dumps(event_data)
    with _clients_lock:
        dead = []
        for q in _clients:
            try:
                q.put_nowait(msg)
            except queue.Full:
                dead.append(q)
        for q in dead:
            _clients.remove(q)


def extract_timestamp(payload: dict) -> str:
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
    timestamp = extract_timestamp(payload)

    moose_event = {
        "deliveryId": delivery_id,
        "timestamp": timestamp,
        "eventType": event_type,
        "repo": repo,
        "actor": actor,
        "action": action,
        "rawPayload": json.dumps(payload),
    }

    # Forward to MooseStack
    moose_status = 0
    try:
        resp = requests.post(MOOSE_INGEST_URL, json=moose_event, timeout=5)
        moose_status = resp.status_code
        print(f"[{event_type}] {repo} by {actor} → MooseStack {moose_status}")
    except Exception as e:
        print(f"[{event_type}] Failed to forward to MooseStack: {e}")

    # Broadcast to SSE clients immediately (don't wait for MooseStack)
    broadcast({
        "eventId":   delivery_id,
        "timestamp": timestamp,
        "eventType": event_type,
        "repo":      repo,
        "actor":     actor,
        "action":    action,
    })

    return jsonify({"status": "ok", "moose_status": moose_status})


@app.route("/events")
def sse_stream():
    """Server-Sent Events endpoint — streams GitHub events to the dashboard."""
    def generate():
        q: queue.Queue = queue.Queue(maxsize=50)
        with _clients_lock:
            _clients.append(q)
        print(f"[SSE] client connected ({len(_clients)} total)")
        try:
            yield "data: {\"type\":\"connected\"}\n\n"
            while True:
                try:
                    data = q.get(timeout=25)
                    yield f"data: {data}\n\n"
                except queue.Empty:
                    yield ": heartbeat\n\n"
        finally:
            with _clients_lock:
                if q in _clients:
                    _clients.remove(q)
            print(f"[SSE] client disconnected ({len(_clients)} remaining)")

    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",      # tell NPM not to buffer
            "Access-Control-Allow-Origin": "*",
        },
    )


@app.route("/health")
def health():
    return jsonify({"status": "ok", "sse_clients": len(_clients)})


if __name__ == "__main__":
    print("GitHub webhook adapter running on :3001")
    print(f"Forwarding to: {MOOSE_INGEST_URL}")
    print()
    print("Webhook URL: https://devrel-webhook.joekarlsson.io/github-webhook")
    print("SSE stream:  http://localhost:3001/events")
    app.run(host="0.0.0.0", port=3001, debug=False, threaded=True)
