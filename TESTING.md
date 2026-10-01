# Testing

This page describes how `alfresco-event2-kafka-fanout` 1.0.0 was tested and what the results were.

Testing was done on 1 October 2026 at seven levels:

1. Unit and wiring tests.
2. A check of what the AMP bundles.
3. End-to-end tests on a stock ACS Community 26.2.0 container with real ActiveMQ and Kafka brokers.
4. A Kafka outage and recovery test.
5. A per-node ordering test on a Kafka topic with 6 partitions.
6. Kafka-only mode (`repo.event2.route.activemq.enabled=false`), including an ActiveMQ outage.
7. An experiment with two repository nodes on one database (Alfresco Community does not support clustering).

## Summary

| Test | Result |
|---|---|
| Unit and wiring tests (`mvn test`) | 18 / 18 pass |
| AMP contents | Bundles exactly the 5 libraries missing from `alfresco.war`, plus the module jar |
| Module installs on ACS 26.2.0 | Yes: `Installing module 'alfresco-event2-kafka-fanout' version 1.0.0` |
| Dual publish: same messages on both brokers | 137 on ActiveMQ, 137 on Kafka |
| Event coverage (21 change types, run 3 times) | 21 / 21 steps match on both brokers in every run: twice on a 1-partition topic, once on 6 partitions |
| Kafka message key | Node id; child id for child associations; source id for peer associations |
| Kafka outage | Uploads unaffected, ActiveMQ got every event, Kafka failures logged; each event delayed about 10 s |
| Kafka recovery | Automatic. Events produced during the outage are not re-sent to Kafka |
| Kafka-only mode | Kafka receives every event, with the same per-step counts as dual mode; ActiveMQ receives none; ordering 0 failures; an ActiveMQ outage does not affect Kafka (events on Kafka within 2–3 s) |
| Two repository nodes (experiment) | Every change published exactly once (Kafka = ActiveMQ in every run). Per-document order is **not** guaranteed across repository nodes: it broke after a node restart (4–7 of 20 documents) and whenever one node's sender lagged (5 of 5) |
| Per-node ordering across 6 partitions | 0 failures in 2 runs (24 documents / 175 events, and 60 documents / 728 events): each node's events on one partition, in the order made; documents spread over all 6 partitions |

## Versions tested

### Runtime (Docker stack in `docker/`)

| Component | Version |
|---|---|
| ACS repository image | `alfresco/alfresco-content-repository-community:26.2.0` (`sha256:230ded9ff8d3…`) |
| `alfresco-repository` | 26.2.0.96 |
| Java (in the ACS image) | OpenJDK 21.0.9 (Red Hat build 21.0.9+10-LTS) |
| Tomcat | 11.0.13 |
| Spring Framework | 7.0.8 |
| Apache Camel (in `alfresco.war`) | 4.18.2 |
| Event model (`acs-event-model`) | 1.1.0-A.1 |
| ActiveMQ client (in `alfresco.war`) | 6.2.4 |
| ActiveMQ broker | `alfresco/alfresco-activemq:6.2-jre17-rockylinux8` (ActiveMQ Classic 6.2.10) |
| Kafka broker | `apache/kafka:3.9.1` (KRaft, single node, replication factor 1). The topic was auto-created with 1 partition for the tests in sections 3–5 and event coverage runs 1–2. For section 6 and event coverage run 3, `kafka-init` created it with 6 partitions |
| PostgreSQL | `postgres:16.6` |
| Search / transforms | Disabled (`index.subsystem.name=noindex`, local and legacy transforms off) |

### Bundled by the module

| Library | Version | License |
|---|---|---|
| `camel-kafka` | 4.18.2 | Apache 2.0 |
| `kafka-clients` | 3.9.2 | Apache 2.0 |
| `lz4-java` (`at.yawk.lz4`) | 1.10.3 | Apache 2.0 |
| `snappy-java` | 1.1.10.5 | Apache 2.0 |
| `zstd-jni` | 1.5.6-4 | BSD 2-Clause |

### Build and test tools

| Tool | Version |
|---|---|
| JDK used to build | Oracle JDK 25.0.2 (compiled with `--release 21`) |
| Maven | 3.9.14 |
| JUnit | 5.11.4 |
| Mockito | 5.20.0 |
| `camel-mock`, `spring-test` | 4.18.2, 7.0.8 |
| Docker / Compose | 29.4.3 / 5.1.3 |
| Python (for `docker/event-matrix.py`) | 3.13.5 |
| Host | macOS 15.7.9, x86_64 |

## 1. Unit and wiring tests

Run with `mvn test`. Result: **18 tests, 0 failures**. No broker or Alfresco instance is needed.

| Test class | Tests | What it checks |
|---|---|---|
| `Event2FanOutRouteBuilderTest` | 10 | Every combination of enabled targets, run in a real Camel context with mock endpoints. Also checks that: <ul><li>the Kafka message is keyed by the node id;</li><li>the internal header and the JMS-only header are removed before Kafka;</li><li>ActiveMQ messages are unchanged;</li><li>a Kafka failure is swallowed by default;</li><li>with `failOnError=true`, a Kafka failure is reported to the sender but ActiveMQ still gets the event;</li><li>an ActiveMQ failure is reported to the sender but Kafka still gets the event;</li><li>when both fail, the sender gets the ActiveMQ failure with the Kafka failure attached as a suppressed exception;</li><li>bypass mode adds no routes;</li><li>the shipped default Kafka endpoint options are accepted by `camel-kafka` 4.18.2.</li></ul> |
| `KeyedEvent2MessageProducerTest` | 6 | The key for node, child-association and peer-association events, and that no header is added when sending straight to a broker |
| `ModuleWiringTest` | 2 | Loads `module-context.xml` and the Messaging subsystem extension context with the module's default properties. Checks that `event2MessageProducer` is replaced and the route builder gets the right settings |

## 2. AMP contents

What `alfresco.war` already contains was worked out by resolving the runtime dependencies of
`alfresco-repository` 26.2.0.96 (316 artifacts) and comparing them with the module's. Five libraries are missing
from the war, and the AMP bundles exactly those plus the module jar. Everything else, including Camel 4.18.2 itself,
comes from the war. The image check below confirms `alfresco.war` ships Camel 4.18.2 and no Kafka client before the
module is installed.

```
module.properties, LICENSE, NOTICE
lib/alfresco-event2-kafka-fanout-1.0.0-SNAPSHOT.jar
lib/camel-kafka-4.18.2.jar
lib/kafka-clients-3.9.2.jar
lib/lz4-java-1.10.3.jar
lib/snappy-java-1.1.10.5.jar
lib/zstd-jni-1.5.6-4.jar
```

## 3. End-to-end: install and dual publish

The AMP was installed into the stock image with `alfresco-mmt` (`docker/Dockerfile`), and the stack in
`docker/docker-compose.yml` was started with Kafka enabled (`docker/alfresco-global.properties`).

- The log showed `Installing module 'alfresco-event2-kafka-fanout' version 1.0.0`, and Tomcat started normally.
- A document was created, its title updated, then it was deleted. Kafka received `node.Created`, `node.Updated` (with
  `resourceBefore.properties.cm:title = v1`) and `node.Deleted` in that order, all keyed by the document's node id.
- Including the events from Alfresco's first-start bootstrap, both brokers had **137** messages: ActiveMQ topic
  `EnqueueCount` 137, Kafka end offset 137.

## 4. End-to-end: event coverage

`docker/event-matrix.py` makes 21 kinds of change through the REST API. After each one it waits for the Kafka offset
to stop moving, then records the events on Kafka and the change in the ActiveMQ topic's `EnqueueCount`. It was run
three times: twice on a 1-partition topic (the second time from another directory, using the copy in the
repository), then once on a 6-partition topic after the script was changed to read every partition. **All three runs:
21 steps, 0 mismatches between Kafka and ActiveMQ**, with the same per-step counts.

| # | Change | HTTP | Kafka | ActiveMQ | Events on Kafka (key) |
|---|---|---|---|---|---|
| 1 | Create folder A | 201 | 2 | 2 | `node.Updated` Company Home; `node.Created` A |
| 2 | Create folder B | 201 | 2 | 2 | `node.Updated` Company Home; `node.Created` B |
| 3 | Create document in A | 201 | 2 | 2 | `node.Updated` A; `node.Created` doc |
| 4 | Create second document in A | 201 | 2 | 2 | `node.Updated` A; `node.Created` doc2 |
| 5 | Upload new content | 200 | 1 | 1 | `node.Updated` doc; before: `content`, `modifiedAt` |
| 6 | Update `cm:title` | 200 | 1 | 1 | `node.Updated` doc; before: `properties`, `aspectNames` (`cm:titled` added), `localizedProperties` |
| 7 | Rename | 200 | 2 | 2 | `node.Updated` A; `node.Updated` doc; before: `name`, `primaryHierarchy`, `properties` |
| 8 | Add aspect `cm:dublincore` | 200 | 1 | 1 | `node.Updated` doc; before: `aspectNames` |
| 9 | Remove aspect `cm:dublincore` | 200 | 1 | 1 | `node.Updated` doc; before: `aspectNames`, `properties` |
| 10 | Change type `cm:content` → `cm:dictionaryModel` | 200 | 1 | 1 | `node.Updated` doc2; before: `nodeType`, `properties` |
| 11 | Move doc from A to B | 200 | 3 | 3 | `node.Updated` A; `node.Updated` B; `node.Updated` doc; before: `primaryHierarchy` |
| 12 | Add secondary parent (A) | 201 | 2 | 2 | `node.Updated` doc (`secondaryParents`); `assoc.child.Created` `cm:contains` (key = child) |
| 13 | Remove secondary parent | 204 | 2 | 2 | `node.Updated` doc (`secondaryParents`); `assoc.child.Deleted` (key = child) |
| 14 | Add peer association `cm:references` | 201 | 2 | 2 | `node.Updated` doc (`cm:referencing` aspect added); `assoc.peer.Created` (key = source) |
| 15 | Remove peer association | 204 | 1 | 1 | `assoc.peer.Deleted` (key = source) |
| 16 | Set permissions | 200 | 0 | 0 | none: `PERMISSION_UPDATED` is not generated by Community |
| 17 | Delete doc (to trash) | 204 | 2 | 2 | `node.Updated` B; `node.Deleted` doc |
| 18 | Restore doc from trash | 200 | 1 | 1 | `node.Created` doc |
| 19 | Delete doc again | 204 | 2 | 2 | `node.Updated` B; `node.Deleted` doc |
| 20 | Purge doc from trash | 204 | 0 | 0 | none |
| 21 | Delete folder A (contains doc2) | 204 | 3 | 3 | `node.Updated` Company Home; `node.Deleted` doc2; `node.Deleted` A |

Observations:

- **Keys.** Every event was keyed as designed: node events by the node id, `assoc.child.*` by the child id, and
  `assoc.peer.*` by the source id. In both runs, all events for the same node arrived in the order the changes were
  made.
- **Order across different nodes can vary.** In step 11 the two folders' `node.Updated` events came in a different
  order in each run. Kafka only orders events per key, and these are different nodes, so this is expected.
- **Parent folders produce events too.** Most changes also emit a `node.Updated` for the folders involved, because
  their `modifiedAt` changes. Consumers interested only in documents should filter on `nodeType` or `isFile`.
- **Restore and purge.** Restoring from trash emits `node.Created`. Purging the trash emits nothing.
- **Uploads.** A plain upload emits only `node.Created`. `assoc.child.*` events appear only for secondary
  associations.

<details>
<summary>Full output of the second run (<code>python3 docker/event-matrix.py</code>)</summary>

```
## create folder A: HTTP 201 | kafka +2 | activemq +2
   key=6b7a30d1  node.Updated           Company Home  before=['@type', 'modifiedAt']
   key=11a30be1  node.Created           A-1790825133

## create folder B: HTTP 201 | kafka +2 | activemq +2
   key=6b7a30d1  node.Updated           Company Home  before=['@type', 'modifiedAt']
   key=329c8561  node.Created           B-1790825133

## create document in A: HTTP 201 | kafka +2 | activemq +2
   key=11a30be1  node.Updated           A-1790825133  before=['@type', 'modifiedAt']
   key=8ba85b8a  node.Created           doc-1790825133.txt

## create second document in A: HTTP 201 | kafka +2 | activemq +2
   key=11a30be1  node.Updated           A-1790825133  before=['@type', 'modifiedAt']
   key=7ef8c344  node.Created           doc2-1790825133.txt

## upload content (new content): HTTP 200 | kafka +1 | activemq +1
   key=8ba85b8a  node.Updated           doc-1790825133.txt  before=['@type', 'content', 'modifiedAt']

## update properties (title): HTTP 200 | kafka +1 | activemq +1
   key=8ba85b8a  node.Updated           doc-1790825133.txt  before=['@type', 'aspectNames', 'localizedProperties', 'modifiedAt', 'properties']

## rename: HTTP 200 | kafka +2 | activemq +2
   key=11a30be1  node.Updated           A-1790825133  before=['@type', 'modifiedAt']
   key=8ba85b8a  node.Updated           renamed-1790825133.txt  before=['@type', 'name', 'primaryHierarchy', 'properties']

## add aspect cm:dublincore: HTTP 200 | kafka +1 | activemq +1
   key=8ba85b8a  node.Updated           renamed-1790825133.txt  before=['@type', 'aspectNames']

## remove aspect cm:dublincore: HTTP 200 | kafka +1 | activemq +1
   key=8ba85b8a  node.Updated           renamed-1790825133.txt  before=['@type', 'aspectNames', 'localizedProperties', 'properties']

## change type (cm:content -> cm:dictionaryModel): HTTP 200 | kafka +1 | activemq +1
   key=7ef8c344  node.Updated           doc2-1790825133.txt  before=['@type', 'nodeType', 'properties']

## move document A -> B: HTTP 200 | kafka +3 | activemq +3
   key=329c8561  node.Updated           B-1790825133  before=['@type', 'modifiedAt']
   key=11a30be1  node.Updated           A-1790825133  before=['@type', 'modifiedAt']
   key=8ba85b8a  node.Updated           renamed-1790825133.txt  before=['@type', 'primaryHierarchy', 'properties']

## add secondary parent (A): HTTP 201 | kafka +2 | activemq +2
   key=8ba85b8a  node.Updated           renamed-1790825133.txt  before=['@type', 'properties', 'secondaryParents']
   key=8ba85b8a  assoc.child.Created    cm:contains parent=11a30be1 child=8ba85b8a

## remove secondary parent (A): HTTP 204 | kafka +2 | activemq +2
   key=8ba85b8a  node.Updated           renamed-1790825133.txt  before=['@type', 'properties', 'secondaryParents']
   key=8ba85b8a  assoc.child.Deleted    cm:contains parent=11a30be1 child=8ba85b8a

## add peer association (cm:references): HTTP 201 | kafka +2 | activemq +2
   key=8ba85b8a  node.Updated           renamed-1790825133.txt  before=['@type', 'aspectNames']
   key=8ba85b8a  assoc.peer.Created     cm:references source=8ba85b8a target=7ef8c344

## remove peer association: HTTP 204 | kafka +1 | activemq +1
   key=8ba85b8a  assoc.peer.Deleted     cm:references source=8ba85b8a target=7ef8c344

## set permissions: HTTP 200 | kafka +0 | activemq +0

## delete document (to trash): HTTP 204 | kafka +2 | activemq +2
   key=329c8561  node.Updated           B-1790825133  before=['@type', 'modifiedAt']
   key=8ba85b8a  node.Deleted           renamed-1790825133.txt

## restore document from trash: HTTP 200 | kafka +1 | activemq +1
   key=8ba85b8a  node.Created           renamed-1790825133.txt

## delete document again: HTTP 204 | kafka +2 | activemq +2
   key=329c8561  node.Updated           B-1790825133  before=['@type', 'modifiedAt']
   key=8ba85b8a  node.Deleted           renamed-1790825133.txt

## purge document from trash: HTTP 204 | kafka +0 | activemq +0

## delete folder A (contains doc2): HTTP 204 | kafka +3 | activemq +3
   key=6b7a30d1  node.Updated           Company Home  before=['@type', 'modifiedAt']
   key=7ef8c344  node.Deleted           doc2-1790825133.txt
   key=11a30be1  node.Deleted           A-1790825133

ids: {'A-1790825133': '11a30be1', 'B-1790825133': '329c8561', 'doc-1790825133.txt': '8ba85b8a', 'doc2-1790825133.txt': '7ef8c344'}

21 steps, 0 mismatches between Kafka and ActiveMQ
```

Keys and ids are shown truncated to 8 characters.

</details>

## 5. Kafka outage and recovery

Settings: `failOnError=false` (the default) and the default endpoint timeouts (`maxBlockMs=5000`,
`requestTimeoutMs=5000`, `deliveryTimeoutMs=10000`).

**Procedure.** Stop the Kafka container while Alfresco keeps running. Create three documents through the REST API.
Sample the ActiveMQ topic's `EnqueueCount` every 5 s for 90 s and read the Alfresco log. Then restart Kafka and
create one more document.

**Results during the outage:**

- Document creation was unaffected: HTTP 201 in 0.197 s, 0.095 s and 0.089 s. Event2 publishes after commit, on its
  own thread.
- ActiveMQ received all 4 resulting events (`EnqueueCount` 141 → 145).
- Each Kafka publish failed after the 10 s delivery timeout and was logged, one every 10 s:

  ```
  02:32:56,547 ERROR [event2.kafka.Event2FanOutRouteBuilder] [Camel (alfrescoCamelContext) thread #17 - KafkaProducer[alfresco.repo.event2]] Failed to publish event2 message to Kafka: Expiring 1 record(s)
  02:33:06,693 ERROR [event2.kafka.Event2FanOutRouteBuilder] ... Failed to publish event2 message to Kafka: Expiring 1 record(s)
  02:33:16,714 ERROR [event2.kafka.Event2FanOutRouteBuilder] ... Failed to publish event2 message to Kafka: Expiring 1 record(s)
  02:33:26,722 ERROR [event2.kafka.Event2FanOutRouteBuilder] ... Failed to publish event2 message to Kafka: Expiring 1 record(s)
  ```

- Event2 sends one event at a time, so while Kafka is down **each event waits about 10 s**. ActiveMQ delivery of
  later events is delayed by that wait (see [Delivery behaviour](README.md#delivery-behaviour)).

**Results after recovery:**

- Once Kafka was back, the next document produced `node.Updated` Company Home and `node.Created` `after-recovery.txt`
  on Kafka (offset 141 → 143) without restarting anything.
- Final totals: ActiveMQ 149, Kafka 143. The 6 events missing from Kafka were all produced while Kafka was down:
  the 4 above, plus 2 from a document created before Kafka had finished restarting. With `failOnError=false`, events
  that fail to reach Kafka are not re-sent.

An earlier outage attempt is not counted. A test-script error meant Kafka was never actually stopped, which the
container uptime and the Kafka offsets confirmed.

## 6. Ordering across partitions

Kafka keeps order only within a partition. The module keys every message by node id (the child for child
associations, the source for peer associations), so all events for a node go to the same partition and stay in
order. Events for different nodes on different partitions have no defined order relative to each other.

**Setup.** The stack was recreated so that `kafka-init` creates `alfresco.repo.event2` with 6 partitions before
Alfresco starts (`EVENT2_TOPIC_PARTITIONS`, default 6).

**Procedure (`docker/ordering-check.py`).**

1. Create a folder, then run many documents through the same sequence in parallel worker threads, so events from
   different nodes interleave. Each document is handled by one worker, in order:
   1. create the document,
   2. set `cm:title` to v1, v2, … vN, one request per value,
   3. delete it.
2. Wait for the topic offsets to settle, then read every partition.
3. For each test document, check that:
   1. all its events are on one partition,
   2. they arrive in exactly the order made: `Created`, `Updated` with titles v1…vN, `Deleted` (the `Deleted` event
      carries the final title),
   3. no other key, such as the folder, has events on more than one partition.

   The run fails if all documents land on one partition, because then nothing was tested across partitions.

**Results:**

| Run | Documents × changes | Parallel workers | Events read | Test documents per partition (p0–p5) | Failures |
|---|---|---|---|---|---|
| 1 | 24 × (create + 5 updates + delete) | 8 | 175 | 4, 4, 1, 3, 7, 5 | 0 |
| 2 | 60 × (create + 10 updates + delete) | 16 | 728 | 9, 10, 17, 7, 7, 10 | 0 |

The event counts include the folder's own events, such as `node.Updated` when children are added. All 6 partitions
received events in both runs.

```
$ python3 docker/ordering-check.py --docs 60 --updates 10 --workers 16
topic alfresco.repo.event2: 6 partitions
60 documents x (10 updates + create + delete), 16 in parallel, in 9.6s
read 728 events from partitions [0, 1, 2, 3, 4, 5]
test documents per partition: p0=9, p1=10, p2=17, p3=7, p4=7, p5=10

60 documents, 728 events, 0 failures
```

**What this shows, and what it doesn't:**

- **Shown:** per-node order holds across a multi-partition topic, with real Alfresco events and many nodes changing
  at once.
- **Not shown:** that the check would catch a wrong order. There was no deliberately broken control run, so the check
  has only been seen passing.
- **Not covered: changing the partition count.** Adding partitions to a live topic changes which partition a key maps
  to, so for a node that is changing at that moment, events before and after the change can be read out of order.
  Create the topic with the partition count you need before enabling the Kafka target.

## 7. Kafka-only mode

**Setup.** Only the Alfresco container was recreated, with the test stack's properties plus
`repo.event2.route.activemq.enabled=false`. The data volumes and the 6-partition topic were kept. Afterwards the
stack was switched back to dual mode.

**Results:**

- **Basic lifecycle.** Creating, updating and deleting a document produced `node.Created` → `node.Updated` →
  `node.Deleted`, all on partition 2 and keyed by the document's node id, plus a `node.Updated` for the parent folder.
  The ActiveMQ topic's `EnqueueCount` stayed at 1069.
- **Event coverage (`event-matrix.py`).** Kafka's per-step counts were identical to the dual-mode runs
  (2, 2, 2, 2, 1, 1, 2, 1, 1, 1, 3, 2, 2, 2, 1, 0, 2, 1, 2, 0, 3), and ActiveMQ received 0 at every step. The script
  reports "19 mismatches" and exits 1, because it compares the two brokers. In this mode that is the expected result:
  the 19 are exactly the steps that produce events.
- **Ordering (`ordering-check.py --docs 60 --updates 10 --workers 16`).** 730 events across all 6 partitions,
  0 failures.
- **ActiveMQ outage.** With the ActiveMQ container stopped, documents were created (HTTP 201 in about 0.09 s each)
  and their events reached Kafka within 2.0 s, 2.2 s and 3.0 s. These times include 1–2 s of polling overhead from the
  test script itself. In dual mode, the same outage held Kafka back for the whole outage (about 50 s); in Kafka-only
  mode, Event2 publishing no longer touches ActiveMQ at all.

Alfresco updates a folder's `modified` date at most once a second for changes to its children
(`AbstractNodeDAOImpl`, `modifiedDateToleranceMs = 1000`). Three documents created within 200 ms therefore produced
three `node.Created` events but only one `node.Updated` for the folder. A first attempt at the outage test wrongly
expected one folder event per document, and it is not counted.

## 8. Two repository nodes (experiment)

Alfresco Community does not support clustering. Its caches are local to each JVM (`DefaultCacheFactory`), and it has
no Hazelcast cache synchronisation. This was therefore an experiment on an unsupported setup, aimed only at the
module's behaviour when several repository nodes publish Event2: does each change go out exactly once, and does
per-document order survive? A real Enterprise cluster was not tested.

**Setup.** A second repository node (`alfresco2`, port 8081) was added from the same image, so it had the same module.
It used the same Postgres database, the same content store volume, the same ActiveMQ and the same 6-partition Kafka
topic, with a 2.5 GB memory limit. It was started after node 1 was up and removed after the tests. The compose override and
the test script are in [`docker/cluster-experiment/`](docker/cluster-experiment/README.md).

**Tests and results:**

| Test | What was done | Result |
|---|---|---|
| Exactly once | 20 documents, 8 at a time: create on one node, update on the other, delete on the first | Kafka and ActiveMQ received the same number of events in every run, and each document had exactly 3 events: no duplicates, no gaps |
| Order, warm nodes | Same test repeated 3 times; plus 40 documents × 6 updates alternating node 2 / node 1, 12 at a time, run twice | 0 out of order. `resourceBefore` always held the true previous title (no stale-cache values) |
| Order, node just restarted | Node 2 restarted, then the exactly-once test run at once (twice) | **7 and 4 of 20 documents out of order** on Kafka |
| Order, one node's sender lagging | Node 1 in dual mode, node 2 Kafka-only, ActiveMQ stopped (node 1's sender blocks, node 2's does not). 5 documents, each updated v1 (node 1), v2 (node 2), v3 (node 1), v4 (node 2) | **5 of 5 out of order**: Kafka received v2, v4, v1, v3. The last event says v3, but the document's final title is v4 |

**Examples from the restart runs.** The `time` field is the transaction commit time.

- Created on node 2, updated on node 1: Kafka received `Updated` (committed 06:52:00.989) **before** `Created`
  (committed 06:52:00.721).
- Created on node 1, updated on the just-restarted node 2: Kafka received `Created`, `Deleted` (06:52:01.142), **then**
  `Updated` (06:52:00.732). A consumer that keeps the latest state per document would bring the deleted document back.

The out-of-order events in the restart runs all came from the node that had just restarted. That fits its first
sends being slow while the Kafka producer and ActiveMQ connection are set up. This explanation is inferred from the
pattern and was not measured directly.

**Why order breaks.** Each repository node has its own Event2 queue and sender thread, and publishes only the
transactions it commits. Kafka keeps the order in which messages *arrive*. When two nodes change the same document in
quick succession and the first node is slower to publish, its event arrives second. The module cannot fix this:
nothing on the Kafka side knows the commit order. Stock Alfresco on ActiveMQ has the same property, because the
reordering happens before the events are split between the two brokers.

**What consumers should do with several repository nodes:** use the event's `time` (commit time) to order events for
a node, rather than Kafka offsets. Ignore any event older than the last one applied for that node, and treat
`node.Deleted` as final for that node id. Commit times come from each node's clock, so keep the repository nodes'
clocks synchronised (NTP).

## Issues found during testing

| Issue | Resolution |
|---|---|
| ACS fails to start (`catalina.sh: eval: syntax error near unexpected token '('`) when the ActiveMQ `failover:(...)` URL, or a Kafka URI with `&`, is passed in `JAVA_OPTS` | Put endpoint URIs in `alfresco-global.properties`. Documented in the README |
| Earlier assumption that an upload emits `assoc.child.Created` | Wrong: only secondary associations emit child-association events (`EventGenerator.onCreateChildAssociation`). Confirmed by step 3 |

## Not yet tested

- **Load and performance:** no throughput or latency benchmarks yet.
- **`failOnError=true`:** covered by unit tests only, not end to end.
- **Kafka security (SASL/SSL), long outages** (memory use of the Event2 queue), **ACS versions other than 26.2.0,
  and Enterprise Edition**, including a real (Hazelcast) Enterprise cluster. Section 8 is a Community experiment only.
