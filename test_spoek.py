"""Quick test script for the SpoekHttp channel (Windows TCP fallback)."""

import json
import urllib.request

BASE = "http://127.0.0.1:18791"


def get(path: str) -> dict:
    with urllib.request.urlopen(f"{BASE}{path}") as r:
        return json.loads(r.read())


def post(path: str, payload: dict) -> dict:
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        f"{BASE}{path}",
        data=data,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read())


if __name__ == "__main__":
    print("Health:", get("/health"))
    print("Message:", post("/message", {
        "text": "Hello, what is 2+2?",
        "from_user_id": "1",
        "from_user_name": "TestUser",
        "chat_id": "test",
    }))
