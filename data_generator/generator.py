import uuid
import random
import time
from datetime import datetime, timedelta, timezone
import json

# -----------------------------
# Configuration
# -----------------------------
EVENT_TYPES = ["app_open", "button_click", "page_view", "purchase"]
DEVICES = ["mobile", "web"]
COUNTRIES = ["in", "us", "uk", "jp"]

EVENTS_PER_SECOND = 5
DUPLICATE_PROBABILITY = 0.05
LATE_EVENT_PROBABILITY = 0.10


def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def generate_event():
    event_time = datetime.now(timezone.utc)

    # simulate late events
    if random.random() < LATE_EVENT_PROBABILITY:
        event_time -= timedelta(seconds=random.randint(5, 180))

    return {
        "event_id": str(uuid.uuid4()),
        "user_id": random.randint(1, 10000),  # int
        "event_type": random.choice(EVENT_TYPES),
        "event_time": event_time.isoformat(timespec="milliseconds"),
        "ingest_time": utc_now(),
        "device_type": random.choice(DEVICES),
        "country": random.choice(COUNTRIES),
        "session_id": f"s-{random.randint(1000, 9999)}",
        "metadata": {
            "app_version": f"{random.randint(1,5)}.{random.randint(0,9)}",
            "screen": random.choice(
                ["home", "search", "profile", "shop", "settings"]
            )
        }
    }


def event_stream():
    last_event = None

    while True:
        event = generate_event()

        # simulate duplicates
        if last_event and random.random() < DUPLICATE_PROBABILITY:
            event = last_event

        print(json.dumps(event))
        last_event = event
        time.sleep(1 / EVENTS_PER_SECOND)


if __name__ == "__main__":
    event_stream()
