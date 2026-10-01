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
"""Event2 coverage check against the docker/ test stack.

Makes every kind of repository change through the REST API (create, content upload, property update, rename,
aspects, type change, move, secondary parents, peer associations, permissions, delete, restore, purge, folder
delete) and, for each one, prints the events that reached Kafka and checks that ActiveMQ received the same number.

Usage (stack running, Kafka target enabled, Python 3 standard library only):
    docker compose -f docker/docker-compose.yml up -d --build
    python3 docker/event-matrix.py

Exits with status 1 if any step published a different number of messages to Kafka and ActiveMQ.

Author: Kamran Zafar
"""
import base64
import json
import pathlib
import subprocess
import sys
import time
import urllib.error
import urllib.request

BASE = "http://localhost:8080/alfresco/api/-default-/public/alfresco/versions/1"
AUTH = "Basic " + base64.b64encode(b"admin:admin").decode()
COMPOSE = ["docker", "compose", "-f", str(pathlib.Path(__file__).resolve().parent / "docker-compose.yml")]


def call(method, path, body=None, raw=None, ctype="application/json"):
    data = raw if raw is not None else (json.dumps(body).encode() if body is not None else None)
    req = urllib.request.Request(BASE + path, data=data, method=method, headers={"Authorization": AUTH, "Content-Type": ctype})
    try:
        with urllib.request.urlopen(req) as r:
            text = r.read().decode()
            return r.status, (json.loads(text) if text else None)
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()[:200]


def kafka_end_offsets():
    """{partition: end offset}; the topic may have several partitions."""
    out = subprocess.run(COMPOSE + ["exec", "-T", "kafka", "/opt/kafka/bin/kafka-get-offsets.sh", "--bootstrap-server", "kafka:9092", "--topic", "alfresco.repo.event2"], capture_output=True, text=True).stdout
    offsets = {}
    for line in out.split():
        _, partition, offset = line.rsplit(":", 2)
        offsets[int(partition)] = int(offset)
    return offsets


def activemq_count():
    req = urllib.request.Request("http://localhost:8161/api/jolokia/", method="POST", headers={"Authorization": "Basic " + base64.b64encode(b"admin:admin").decode(), "Origin": "http://localhost", "Content-Type": "application/json"},
                                 data=json.dumps({"type": "read", "mbean": "org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Topic,destinationName=alfresco.repo.event2", "attribute": "EnqueueCount"}).encode())
    return json.loads(urllib.request.urlopen(req).read())["value"]


def kafka_events(start, end):
    """Events published between two offset snapshots, read from every partition, in event-time order."""
    events = []
    for partition, end_offset in end.items():
        count = end_offset - start.get(partition, 0)
        if count <= 0:
            continue
        out = subprocess.run(COMPOSE + ["exec", "-T", "kafka", "/opt/kafka/bin/kafka-console-consumer.sh", "--bootstrap-server", "kafka:9092", "--topic", "alfresco.repo.event2", "--partition", str(partition),
                                        "--offset", str(start.get(partition, 0)), "--max-messages", str(count), "--timeout-ms", "10000", "--property", "print.key=true", "--property", "print.offset=true"], capture_output=True, text=True).stdout
        for line in out.splitlines():
            fields = line.split("\t")
            offset = int(next(f for f in fields if f.startswith("Offset:")).split(":", 1)[1])
            events.append((partition, offset, fields[-2], json.loads(fields[-1])))
    # order is only defined per partition; for display, sort by event time, then partition position
    events.sort(key=lambda e: (e[3]["time"], e[0], e[1]))
    return [(key, event) for _, _, key, event in events]


def wait_settled():
    # Event2 is sent asynchronously after commit; wait until no partition's offset has moved for a few seconds
    last, stable = None, 0
    for _ in range(40):
        now = kafka_end_offsets()
        stable = stable + 1 if now == last else 0
        if stable >= 3:
            return now
        last = now
        time.sleep(1)
    return last


steps = []


def step(label, fn):
    k0, a0 = kafka_end_offsets(), activemq_count()
    result = fn()
    k1 = wait_settled()
    a1 = activemq_count()
    kafka_n = sum(k1.values()) - sum(k0.values())
    events = kafka_events(k0, k1) if kafka_n > 0 else []
    steps.append((label, result, kafka_n, a1 - a0, events))
    return result


suffix = str(int(time.time()))
ids = {}

def create(name, parent="-my-", node_type="cm:content", **extra):
    status, body = call("POST", f"/nodes/{parent}/children", {"name": name, "nodeType": node_type, **extra})
    ids[name] = body["entry"]["id"]
    return status

step("create folder A", lambda: create(f"A-{suffix}", node_type="cm:folder"))
step("create folder B", lambda: create(f"B-{suffix}", node_type="cm:folder"))
step("create document in A", lambda: create(f"doc-{suffix}.txt", parent=ids[f"A-{suffix}"]))
doc = ids[f"doc-{suffix}.txt"]
step("create second document in A", lambda: create(f"doc2-{suffix}.txt", parent=ids[f"A-{suffix}"]))
doc2 = ids[f"doc2-{suffix}.txt"]
step("upload content (new content)", lambda: call("PUT", f"/nodes/{doc}/content", raw=b"hello event2", ctype="text/plain")[0])
step("update properties (title)", lambda: call("PUT", f"/nodes/{doc}", {"properties": {"cm:title": "v2"}})[0])
step("rename", lambda: call("PUT", f"/nodes/{doc}", {"name": f"renamed-{suffix}.txt"})[0])

def aspects(node):
    return call("GET", f"/nodes/{node}")[1]["entry"]["aspectNames"]

step("add aspect cm:dublincore", lambda: call("PUT", f"/nodes/{doc}", {"aspectNames": aspects(doc) + ["cm:dublincore"]})[0])
step("remove aspect cm:dublincore", lambda: call("PUT", f"/nodes/{doc}", {"aspectNames": [a for a in aspects(doc) if a != "cm:dublincore"]})[0])
step("change type (cm:content -> cm:dictionaryModel)", lambda: call("PUT", f"/nodes/{doc2}", {"nodeType": "cm:dictionaryModel"})[0])
step("move document A -> B", lambda: call("POST", f"/nodes/{doc}/move", {"targetParentId": ids[f"B-{suffix}"]})[0])
step("add secondary parent (A)", lambda: call("POST", f"/nodes/{ids[f'A-{suffix}']}/secondary-children", {"childId": doc, "assocType": "cm:contains"})[0])
step("remove secondary parent (A)", lambda: call("DELETE", f"/nodes/{ids[f'A-{suffix}']}/secondary-children/{doc}?assocType=cm:contains")[0])
step("add peer association (cm:references)", lambda: call("POST", f"/nodes/{doc}/targets", {"targetId": doc2, "assocType": "cm:references"})[0])
step("remove peer association", lambda: call("DELETE", f"/nodes/{doc}/targets/{doc2}?assocType=cm:references")[0])
step("set permissions", lambda: call("PUT", f"/nodes/{doc}", {"permissions": {"isInheritanceEnabled": False, "locallySet": [{"authorityId": "GROUP_EVERYONE", "name": "Consumer", "accessStatus": "ALLOWED"}]}})[0])
step("delete document (to trash)", lambda: call("DELETE", f"/nodes/{doc}")[0])
step("restore document from trash", lambda: call("POST", f"/deleted-nodes/{doc}/restore")[0])
step("delete document again", lambda: call("DELETE", f"/nodes/{doc}")[0])
step("purge document from trash", lambda: call("DELETE", f"/deleted-nodes/{doc}")[0])
step("delete folder A (contains doc2)", lambda: call("DELETE", f"/nodes/{ids[f'A-{suffix}']}")[0])

mismatches = 0
for label, result, kafka_n, amq_n, events in steps:
    match = "" if kafka_n == amq_n else "  <-- MISMATCH"
    mismatches += kafka_n != amq_n
    print(f"\n## {label}: HTTP {result} | kafka +{kafka_n} | activemq +{amq_n}{match}")
    for key, e in events:
        data, res = e["data"], e["data"]["resource"]
        what = res.get("name") or (f"{res.get('assocType')} parent={res.get('parent',{}).get('id','')[:8]} child={res.get('child',{}).get('id','')[:8]}" if "parent" in res else f"{res.get('assocType')} source={res.get('source',{}).get('id','')[:8]} target={res.get('target',{}).get('id','')[:8]}")
        before = sorted((data.get("resourceBefore") or {}).keys())
        print(f"   key={key[:8]}  {e['type'].replace('org.alfresco.event.', ''):22} {what}" + (f"  before={before}" if before else ""))
print("\nids:", {k: v[:8] for k, v in ids.items()})
print(f"\n{len(steps)} steps, {mismatches} mismatches between Kafka and ActiveMQ")
sys.exit(1 if mismatches else 0)
