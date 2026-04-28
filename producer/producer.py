import json, subprocess, uuid, os, sys
from datetime import datetime, timezone
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from kafka import KafkaProducer

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import KAFKA_BROKER, KAFKA_TOPIC

WATCH_DIR = "./wafer_outputs"

os.makedirs(WATCH_DIR, exist_ok=True)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def get_git_sha():
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"]
        ).decode().strip()
    except Exception:
        return "unknown"

def publish_file(filepath):
    run_id = f"run_{uuid.uuid4().hex[:8]}"
    git_sha = get_git_sha()
    count = 0

    with open(filepath) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            record["run_id"]      = run_id
            record["git_sha"]     = git_sha
            record["ingested_at"] = datetime.now(timezone.utc).isoformat()
            producer.send(KAFKA_TOPIC, value=record)
            count += 1

    producer.flush()
    print(f"Published {count} die records | run_id={run_id} git_sha={git_sha}")

class WaferOutputHandler(FileSystemEventHandler):
    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith(".json"):
            publish_file(event.src_path)

if __name__ == "__main__":
    print(f"Watching {WATCH_DIR} for new wafer test files...")
    observer = Observer()
    observer.schedule(WaferOutputHandler(), path=WATCH_DIR, recursive=False)
    observer.start()
    try:
        observer.join()
    except KeyboardInterrupt:
        observer.stop()
        producer.close()
        print("Producer stopped.")