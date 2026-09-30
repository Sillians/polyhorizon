"""Offline migration: retain topic names/partition counts and move replicas to 1/2.

Start all three existing brokers before running. Defaults to plan-only. Never
deletes a volume, topic, checkpoint, or log. The previous assignment is retained.
"""
import argparse
import json
from pathlib import Path
import subprocess
import time

from kafka import KafkaAdminClient, KafkaConsumer, TopicPartition
from scripts.ops.market_supervisor import save, ROOT

BOOTSTRAP = ["localhost:9092", "localhost:9093", "localhost:9094"]
DIRECTORY = ROOT / "artifacts/low-memory"


def assignments(topics):
    previous, target = [], []
    for topic in topics:
        if topic.get("error_code", 0):
            raise RuntimeError("Topic metadata error")
        for p in topic["partitions"]:
            if p["leader"] < 0 or set(p["replicas"]) != set(p["isr"]):
                raise RuntimeError("All replicas must be in sync before migration")
            base = {"topic": topic["topic"], "partition": p["partition"]}
            previous.append(dict(base, replicas=p["replicas"]))
            target.append(dict(base, replicas=[1, 2] if p["partition"] % 2 == 0 else [2, 1]))
    if not target:
        raise RuntimeError("No existing partitions found; bootstrap is a separate operation")
    return {"version": 1, "partitions": previous}, {"version": 1, "partitions": target}


def verified(topics):
    return bool(topics) and all(p["leader"] in (1, 2) and set(p["replicas"]) == {1, 2}
        and set(p["isr"]) == {1, 2} for t in topics for p in t["partitions"])


def command(args, input=None):
    result = subprocess.run(args, input=input, text=True, capture_output=True, timeout=90)
    if result.returncode:
        raise RuntimeError("Kafka migration command failed; inspect broker logs")
    return result.stdout


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP, request_timeout_ms=20000)
    consumer = KafkaConsumer(bootstrap_servers=BOOTSTRAP, enable_auto_commit=False, request_timeout_ms=20000)
    try:
        cluster = client.describe_cluster()
        if {b["node_id"] for b in cluster["brokers"]} != {1, 2, 3}:
            raise RuntimeError("Start all three existing brokers for the migration")
        active = command(["docker", "ps", "--format", "{{.Names}} {{.Label \"io.prefect.flow-run-id\"}}"])
        if any("local-worker" in line or (len(line.split()) > 1) for line in active.splitlines()):
            raise RuntimeError("Stop Prefect workers and flow containers before offline migration")
        topics = client.describe_topics()
        before, target = assignments(topics)
        partitions = [TopicPartition(p["topic"], p["partition"]) for p in target["partitions"]]
        ends = consumer.end_offsets(partitions)
        print(json.dumps({"topics": len(topics), "partitions": len(partitions), "target_brokers": [1, 2], "execute": args.execute}))
        if not args.execute:
            return
        DIRECTORY.mkdir(parents=True, exist_ok=True)
        # Preserve the first pre-migration assignment on reruns.
        if not (DIRECTORY / "kafka-before.json").exists():
            save(DIRECTORY / "kafka-before.json", before)
        offset_path = DIRECTORY / "kafka-offsets-before.json"
        if not offset_path.exists():
            save(offset_path, {f"{p.topic}:{p.partition}": o for p, o in ends.items()})
        save(DIRECTORY / "kafka-target.json", target)
        command(["docker", "exec", "-i", "kafka-broker-1", "tee", "/tmp/polyhorizon-two-brokers.json"], json.dumps(target))
        base = ["docker", "exec", "kafka-broker-1", "kafka-reassign-partitions", "--bootstrap-server", "kafka-broker-1:29092",
                "--reassignment-json-file", "/tmp/polyhorizon-two-brokers.json"]
        command(base + ["--execute"])
        deadline = time.monotonic() + 600
        while not verified(client.describe_topics()):
            if time.monotonic() > deadline:
                raise RuntimeError("Migration incomplete; keep broker 3 and all volumes available")
            time.sleep(3)
        command(base + ["--verify"])
        for topic in topics:
            command(["docker", "exec", "kafka-broker-1", "kafka-configs", "--bootstrap-server", "kafka-broker-1:29092",
                "--alter", "--entity-type", "topics", "--entity-name", topic["topic"], "--add-config", "min.insync.replicas=2"])
        # Fresh metadata after leadership changes avoids stale fetcher state.
        consumer.close()
        consumer = KafkaConsumer(bootstrap_servers=BOOTSTRAP[:2], enable_auto_commit=False, request_timeout_ms=20000)
        after = consumer.end_offsets(partitions)
        if {f"{p.topic}:{p.partition}": o for p, o in after.items()} != json.loads(offset_path.read_text()):
            raise RuntimeError("Offsets changed during offline migration; inspect before retiring broker 3")
        save(DIRECTORY / "kafka-verified.json", {"cluster_id": cluster["cluster_id"], "brokers": [1, 2],
            "topic_partitions": len(partitions), "offsets": {f"{p.topic}:{p.partition}": o for p, o in after.items()}})
        print("Replica migration verified; offsets unchanged. Broker 3 can now be stopped; retain its volume.")
    finally:
        client.close()
        consumer.close()


if __name__ == "__main__":
    main()
