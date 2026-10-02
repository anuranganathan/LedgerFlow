"""Health checks for the background processes (relay, worker, notifier, webhooks, reconciler).

They have no HTTP port, so each one touches a file on every loop. Docker's healthcheck runs
`python heartbeat.py <max age>` and marks the container unhealthy if the file is older than
that, which catches a process that is stuck as well as one that crashed. deploy.sh waits for
every container to be healthy before calling a deploy successful.
"""
import os
import sys
import time

HEARTBEAT_FILE = os.getenv("HEARTBEAT_FILE", "/tmp/heartbeat")


def beat() -> None:
    try:
        with open(HEARTBEAT_FILE, "w") as file:
            file.write(str(time.time()))
    except OSError:
        pass  # a health signal must never break the process itself


def is_fresh(max_age_seconds: float) -> bool:
    try:
        return time.time() - os.path.getmtime(HEARTBEAT_FILE) <= max_age_seconds
    except OSError:
        return False


if __name__ == "__main__":
    sys.exit(0 if is_fresh(float(sys.argv[1]) if len(sys.argv) > 1 else 60) else 1)
