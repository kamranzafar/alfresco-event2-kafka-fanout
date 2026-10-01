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
"""Two-node experiment (Alfresco Community does not support clustering): event2 publishing across repository nodes.

  exactly-once   create on one node, update on the other, delete on the first; each change -> exactly one event,
                 same totals on Kafka and ActiveMQ
  alternating    many documents, each updated alternately through node 1 and node 2; per-document order on Kafka and
                 resourceBefore (previous title) must be correct
  lag            node 1 dual mode, node 2 Kafka-only, ActiveMQ stopped: node 1's sender blocks, node 2's does not;
                 shows what happens to per-document order when one node's sender lags

Needs the main stack plus node 2 from docker-compose.two-nodes.yml (see README.md in this folder).
Usage: python3 docker/cluster-experiment/cluster-check.py {exactly-once,alternating,lag} [--docs N] [--updates N] [--workers N]
Exits with status 1 if a check fails (for "lag", out-of-order events are the expected outcome).

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

NODES = {1: "http://localhost:8080", 2: "http://localhost:8081"}
API = "/alfresco/api/-default-/public/alfresco/versions/1"
AUTH = "Basic " + base64.b64encode(b"admin:admin").decode()
HERE = pathlib.Path(__file__).resolve().parent
COMPOSE = ["docker", "compose", "-f", str(HERE.parent / "docker-compose.yml"), "-f", str(HERE / "docker-compose.two-nodes.yml")]
TOPIC = "alfresco.repo.event2"
errors = collections.Counter()


def call(node, method, path, body=None, retries=5):
    data = json.dumps(body).encode() if body is not None else None
    for attempt in range(retries):
        req = urllib.request.Request(NODES[node] + API + path, data=data, method=method, headers={"Authorization": AUTH, "Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req) as r:
                text = r.read().decode()
                return json.loads(text) if text else None
        except urllib.error.HTTPError as e:
            errors[f"node{node} {method} HTTP {e.code}"] += 1
            if attempt == retries - 1:
                raise
            time.sleep(0.2)


def kafka(*args):
    return subprocess.run(COMPOSE + ["exec", "-T", "kafka", *args], capture_output=True, text=True, check=True).stdout


def end_offsets():
    offsets = {}
    for line in kafka("/opt/kafka/bin/kafka-get-offsets.sh", "--bootstrap-server", "kafka:9092", "--topic", TOPIC).split():
        _, p, o = line.rsplit(":", 2)
        offsets[int(p)] = int(o)
    return offsets


def wait_settled(stable_for=4):
    last, stable = None, 0
    for _ in range(180):
        now = end_offsets()
        stable = stable + 1 if now == last else 0
        if stable >= stable_for:
            return now
        last = now
        time.sleep(1)
    raise SystemExit("Kafka offsets did not settle")


def read(start, end):
    events = []
    for p, e in sorted(end.items()):
        n = e - start.get(p, 0)
        if n <= 0:
            continue
        out = kafka("/opt/kafka/bin/kafka-console-consumer.sh", "--bootstrap-server", "kafka:9092", "--topic", TOPIC, "--partition", str(p),
                    "--offset", str(start.get(p, 0)), "--max-messages", str(n), "--timeout-ms", "15000", "--property", "print.key=true", "--property", "print.offset=true")
        for line in out.splitlines():
            f = line.split("\t")
            events.append((p, int(next(x for x in f if x.startswith("Offset:")).split(":", 1)[1]), f[-2], json.loads(f[-1])))
    return events


def activemq_count():
    req = urllib.request.Request("http://localhost:8161/api/jolokia/", method="POST", headers={"Authorization": AUTH, "Origin": "http://localhost", "Content-Type": "application/json"},
                                 data=json.dumps({"type": "read", "mbean": f"org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Topic,destinationName={TOPIC}", "attribute": "EnqueueCount"}).encode())
    return json.loads(urllib.request.urlopen(req).read())["value"]


def by_node(events):
    grouped = collections.defaultdict(list)
    for p, o, k, e in events:
        grouped[k].append((p, o, e))
    return {k: [e for _, _, e in sorted(v, key=lambda x: (x[0], x[1]))] for k, v in grouped.items()}


def title(e, part="resource"):
    return ((e["data"].get(part) or {}).get("properties") or {}).get("cm:title")


def kind(e):
    return e["type"].replace("org.alfresco.event.node.", "")


def folder(node, suffix, name):
    return call(node, "POST", "/nodes/-my-/children", {"name": f"{name}-{suffix}", "nodeType": "cm:folder"})["entry"]["id"]


def exactly_once(docs):
    suffix = str(int(time.time()))
    start, amq0 = wait_settled(), activemq_count()
    parent = folder(1, suffix, "cluster-once")

    def run(i):
        a, b = (1, 2) if i % 2 == 0 else (2, 1)
        nid = call(a, "POST", f"/nodes/{parent}/children", {"name": f"once-{i:03}.txt", "nodeType": "cm:content"})["entry"]["id"]
        call(b, "PUT", f"/nodes/{nid}", {"properties": {"cm:title": f"updated-on-node{b}"}})
        call(a, "DELETE", f"/nodes/{nid}")
        return nid, a, b

    with concurrent.futures.ThreadPoolExecutor(8) as pool:
        results = list(pool.map(run, range(docs)))
    end = wait_settled()
    time.sleep(3)
    amq1 = activemq_count()
    events = by_node(read(start, end))
    kafka_n = sum(end.values()) - sum(start.values())
    failures = 0
    for nid, a, b in results:
        observed = [(kind(e), title(e)) for e in events.get(nid, [])]
        expected = [("Created", None), ("Updated", f"updated-on-node{b}"), ("Deleted", f"updated-on-node{b}")]
        if observed != expected:
            failures += 1
            times = [(kind(e), e["time"][11:23]) for e in events.get(nid, [])]
            print(f"  FAIL {nid[:8]} (create node{a}, update node{b}): Kafka order with commit times {times}")
    print(f"exactly-once: {docs} documents, Kafka +{kafka_n}, ActiveMQ +{amq1 - amq0}, {failures} failures")
    return failures + (kafka_n != amq1 - amq0)


def alternating(docs, updates, workers):
    suffix = str(int(time.time()))
    start = wait_settled()
    parent = folder(1, suffix, "cluster-alt")

    def run(i):
        nid = call(1, "POST", f"/nodes/{parent}/children", {"name": f"alt-{i:03}.txt", "nodeType": "cm:content"})["entry"]["id"]
        for v in range(1, updates + 1):
            call(2 if v % 2 else 1, "PUT", f"/nodes/{nid}", {"properties": {"cm:title": f"v{v}"}})
        call(1, "DELETE", f"/nodes/{nid}")
        return nid

    with concurrent.futures.ThreadPoolExecutor(workers) as pool:
        nids = list(pool.map(run, range(docs)))
    events = by_node(read(start, wait_settled()))
    order_fail = before_fail = 0
    for nid in nids:
        evs = events.get(nid, [])
        observed = [(kind(e), title(e)) for e in evs]
        expected = [("Created", None)] + [("Updated", f"v{v}") for v in range(1, updates + 1)] + [("Deleted", f"v{updates}")]
        if observed != expected:
            order_fail += 1
            print(f"  ORDER {nid[:8]}: Kafka order with commit times {[(k, t, e['time'][11:23]) for (k, t), e in zip(observed, evs)]}")
            continue
        for v, e in zip(range(1, updates + 1), evs[1:-1]):
            if title(e, "resourceBefore") != (f"v{v - 1}" if v > 1 else None):
                before_fail += 1
                print(f"  BEFORE {nid[:8]} v{v}: resourceBefore title {title(e, 'resourceBefore')!r}, expected {f'v{v - 1}' if v > 1 else None!r}")
    print(f"alternating: {docs} documents x {updates} updates alternating node2/node1, {order_fail} out of order, {before_fail} wrong resourceBefore")
    return order_fail + before_fail


def lag(docs):
    dc = lambda *a: subprocess.run(COMPOSE + list(a), capture_output=True, text=True)
    modes = {service: dc("exec", "-T", service, "grep", "^repo.event2.route.activemq.enabled", "/usr/local/tomcat/shared/classes/alfresco-global.properties").stdout.strip()
             for service in ("alfresco", "alfresco2")}
    if modes != {"alfresco": "repo.event2.route.activemq.enabled=true", "alfresco2": "repo.event2.route.activemq.enabled=false"}:
        raise SystemExit(f"lag needs node 1 in dual mode and node 2 Kafka-only (NODE2_PROPERTIES), found {modes}")
    suffix = str(int(time.time()))
    parent = folder(2, suffix, "cluster-lag")
    start = wait_settled()
    dc("stop", "activemq")
    print("ActiveMQ stopped: node 1 (dual mode) blocks on ActiveMQ, node 2 (Kafka-only) does not")
    nids = []
    for i in range(docs):
        nid = call(2, "POST", f"/nodes/{parent}/children", {"name": f"lag-{i:03}.txt", "nodeType": "cm:content"})["entry"]["id"]
        for v, node in ((1, 1), (2, 2), (3, 1), (4, 2)):
            call(node, "PUT", f"/nodes/{nid}", {"properties": {"cm:title": f"v{v}"}})
        nids.append(nid)
    print(f"made {docs} documents x updates v1(node1) v2(node2) v3(node1) v4(node2); final title in the repository: v4")
    time.sleep(10)
    mid = end_offsets()
    early = by_node(read(start, mid))
    print("on Kafka while ActiveMQ is down: " + ", ".join(f"{nid[:8]}=" + "/".join(str(title(e)) for e in early.get(nid, [])) for nid in nids))
    dc("start", "activemq")
    events = by_node(read(start, wait_settled(stable_for=8)))
    out_of_order = wrong_final = 0
    for nid in nids:
        titles = [title(e) for e in events.get(nid, []) if kind(e) == "Updated"]
        times = [e["time"] for e in events.get(nid, []) if kind(e) == "Updated"]
        in_order = titles == ["v1", "v2", "v3", "v4"]
        out_of_order += not in_order
        wrong_final += (titles[-1:] != ["v4"])
        print(f"  {nid[:8]}: Kafka order {titles}{'' if in_order else '  <-- out of order'}; last event says {titles[-1] if titles else None!r}; commit times in Kafka order {[t[11:23] for t in times]}")
    print(f"lag: {docs} documents, {out_of_order} out of order, {wrong_final} where the last event on Kafka is not the final state")
    return out_of_order


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("test", choices=["exactly-once", "alternating", "lag"])
    ap.add_argument("--docs", type=int, default=20)
    ap.add_argument("--updates", type=int, default=6)
    ap.add_argument("--workers", type=int, default=8)
    a = ap.parse_args()
    fn = {"exactly-once": lambda: exactly_once(a.docs), "alternating": lambda: alternating(a.docs, a.updates, a.workers), "lag": lambda: lag(a.docs)}[a.test]
    failures = fn()
    if errors:
        print("HTTP errors seen (retried): " + ", ".join(f"{k} x{v}" for k, v in errors.items()))
    sys.exit(1 if failures else 0)
