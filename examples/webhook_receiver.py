"""Example merchant server that receives LedgerFlow webhooks and verifies them.

    WEBHOOK_SECRET=whsec_... python examples/webhook_receiver.py     # listens on port 9000

Verification, which every receiver should do:
  1. Recompute HMAC-SHA256("<t>.<raw body>") with your secret and compare it in constant time.
  2. Reject timestamps older than 5 minutes, so a captured request can't be replayed later.
  3. Deduplicate on the event "id": delivery is at-least-once, so an event can arrive twice.
"""
import hashlib
import hmac
import json
import os
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

TOLERANCE_SECONDS = 300


def verify_signature(secret: str, header: str, body: bytes, now: float | None = None) -> bool:
    try:
        parts = dict(item.split("=", 1) for item in header.split(","))
        timestamp = int(parts["t"])
    except (ValueError, KeyError):
        return False
    if abs((now or time.time()) - timestamp) > TOLERANCE_SECONDS:
        return False
    expected = hmac.new(secret.encode(), f"{timestamp}.".encode() + body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, parts.get("v1", ""))


class Handler(BaseHTTPRequestHandler):
    seen: set[str] = set()

    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        if not verify_signature(os.environ["WEBHOOK_SECRET"], self.headers.get("LedgerFlow-Signature", ""), body):
            self.send_response(400)
            self.end_headers()
            print("Rejected: bad signature", flush=True)
            return
        event = json.loads(body)
        if event["id"] not in self.seen:  # a real server would store seen IDs in its database
            self.seen.add(event["id"])
            print(f"Verified {event['type']}: {event['data']}", flush=True)
        self.send_response(200)
        self.end_headers()


if __name__ == "__main__":
    HTTPServer(("0.0.0.0", int(os.getenv("PORT", "9000"))), Handler).serve_forever()
