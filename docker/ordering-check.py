#!/usr/bin/env python3
#
# Copyright 2026 Kamran Zafar
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Per-node ordering check across Kafka partitions, against the docker/ test stack.

Creates DOCS documents and changes each one UPDATES times, several documents at a time in parallel, so events from
different nodes interleave. Each document's changes are made in order by one worker. The documents are then
deleted, and every partition of the topic is read. The check passes if, for every test document:

  * all of its events are on a single partition (the message key is the node id), and
  * they arrive in exactly the order the changes were made: Created, Updated (title v1 .. vN), Deleted.

It also reports how the test documents were spread across partitions, and fails if they all landed on one.

Usage (stack running, Kafka target enabled, Python 3 standard library only):
    docker compose -f docker/docker-compose.yml up -d --build
    python3 docker/ordering-check.py [--docs 24] [--updates 5] [--workers 8]

Exits with status 1 if any check fails.

Author: Kamran Zafar
"""
import argparse
import base64
import collections
import concurrent.futures
import json
import pathlib
import subprocess
import sys
import time
import urllib.error
import urllib.request

BASE = "http://localhost:8080/alfresco/api/-default-/public/alfresco/versions/1"
AUTH = "Basic " + base64.b64encode(b"admin:admin").decode()
TOPIC = "alfresco.repo.event2"
COMPOSE = ["docker", "compose", "-f", str(pathlib.Path(__file__).resolve().parent / "docker-compose.yml")]


def call(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(BASE + path, data=data, method=method, headers={"Authorization": AUTH, "Content-Type": "application/json"})
    with urllib.request.urlopen(req) as response:
        text = response.read().decode()
        return json.loads(text) if text else None


def kafka(*args):
    return subprocess.run(COMPOSE + ["exec", "-T", "kafka", *args], capture_output=True, text=True, check=True).stdout


def end_offsets():
    """{partition: end offset} for the topic."""
    out = kafka("/opt/kafka/bin/kafka-get-offsets.sh", "--bootstrap-server", "kafka:9092", "--topic", TOPIC)
    offsets = {}
    for line in out.split():
        _, partition, offset = line.rsplit(":", 2)
        offsets[int(partition)] = int(offset)
    return offsets


def wait_settled():
    # Event2 publishes asynchronously after commit; wait until no partition has moved for a few seconds
    last, stable = None, 0
    for _ in range(90):
        now = end_offsets()
        stable = stable + 1 if now == last else 0
        if stable >= 4:
            return now
        last = now
        time.sleep(1)
    raise SystemExit("Kafka offsets did not settle")


def read_partitions(start, end):
    """Every event between start and end offsets, as (partition, offset, key, event), read partition by partition."""
    events = []
    for partition, end_offset in sorted(end.items()):
        count = end_offset - start.get(partition, 0)
        if count <= 0:
            continue
        out = kafka("/opt/kafka/bin/kafka-console-consumer.sh", "--bootstrap-server", "kafka:9092", "--topic", TOPIC,
                    "--partition", str(partition), "--offset", str(start.get(partition, 0)), "--max-messages", str(count),
                    "--timeout-ms", "15000", "--property", "print.key=true", "--property", "print.offset=true")
        for line in out.splitlines():
            fields = line.split("\t")
            offset = int(next(f for f in fields if f.startswith("Offset:")).split(":", 1)[1])
            events.append((partition, offset, fields[-2], json.loads(fields[-1])))
        if sum(1 for e in events if e[0] == partition) != count:
            raise SystemExit(f"partition {partition}: expected {count} events, read {sum(1 for e in events if e[0] == partition)}")
    return events


def change_document(index, folder_id, updates, suffix):
    """One worker per document: create, update the title `updates` times, delete, all in order."""
    node_id = call("POST", f"/nodes/{folder_id}/children", {"name": f"order-{suffix}-{index:03}.txt", "nodeType": "cm:content"})["entry"]["id"]
    for version in range(1, updates + 1):
        call("PUT", f"/nodes/{node_id}", {"properties": {"cm:title": f"v{version}"}})
    call("DELETE", f"/nodes/{node_id}")
    return node_id


def summarise(event):
    kind = event["type"].replace("org.alfresco.event.node.", "")
    title = (event["data"]["resource"].get("properties") or {}).get("cm:title")
    return kind, title


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--docs", type=int, default=24)
    parser.add_argument("--updates", type=int, default=5)
    parser.add_argument("--workers", type=int, default=8)
    args = parser.parse_args()

    start = wait_settled()
    print(f"topic {TOPIC}: {len(start)} partitions")
    if len(start) < 2:
        raise SystemExit("the topic has a single partition; recreate the stack so kafka-init creates it with several")

    suffix = str(int(time.time()))
    folder_id = call("POST", "/nodes/-my-/children", {"name": f"ordering-{suffix}", "nodeType": "cm:folder"})["entry"]["id"]
    began = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        node_ids = list(pool.map(lambda i: change_document(i, folder_id, args.updates, suffix), range(args.docs)))
    print(f"{args.docs} documents x ({args.updates} updates + create + delete), {args.workers} in parallel, in {time.time() - began:.1f}s")
    call("DELETE", f"/nodes/{folder_id}")

    end = wait_settled()
    events = read_partitions(start, end)
    print(f"read {len(events)} events from partitions {sorted(p for p in end if end[p] > start.get(p, 0))}")

    expected = [("Created", None)] + [("Updated", f"v{v}") for v in range(1, args.updates + 1)] + [("Deleted", f"v{args.updates}")]
    by_node = collections.defaultdict(list)
    for partition, offset, key, event in events:
        by_node[key].append((partition, offset, event))

    failures = 0
    docs_per_partition = collections.Counter()
    for node_id in node_ids:
        node_events = by_node.get(node_id, [])
        partitions = {p for p, _, _ in node_events}
        observed = [summarise(e) for _, _, e in sorted(node_events, key=lambda x: (x[0], x[1]))]
        if len(partitions) != 1:
            failures += 1
            print(f"FAIL {node_id}: events on partitions {sorted(partitions)}")
        elif observed != expected:
            failures += 1
            print(f"FAIL {node_id} (partition {next(iter(partitions))}): expected {expected}, got {observed}")
        else:
            docs_per_partition[next(iter(partitions))] += 1

    # keys other than the test documents (e.g. folders) must also stay on one partition each
    stray = {key: sorted({p for p, _, _ in evs}) for key, evs in by_node.items() if len({p for p, _, _ in evs}) > 1}
    for key, partitions in stray.items():
        failures += 1
        print(f"FAIL key {key}: events on partitions {partitions}")

    print("test documents per partition: " + ", ".join(f"p{p}={n}" for p, n in sorted(docs_per_partition.items())))
    if len(docs_per_partition) < 2:
        failures += 1
        print("FAIL all test documents landed on one partition, so ordering across partitions was not exercised")

    print(f"\n{args.docs} documents, {len(events)} events, {failures} failures")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    try:
        main()
    except urllib.error.HTTPError as e:
        raise SystemExit(f"REST call failed: HTTP {e.code} {e.read().decode()[:200]}")
