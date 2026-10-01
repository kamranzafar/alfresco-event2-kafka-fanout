# Two repository nodes (experiment)

> **Unsupported setup.** Alfresco Community does not support clustering: its caches are local to each JVM, with no
> Hazelcast synchronisation between nodes. These files run two Community repository nodes on one database only to
> check how this module behaves when several nodes publish Event2. They are not a reference for a production cluster.

Results and explanation: [TESTING.md, section 8](../../TESTING.md#8-two-repository-nodes-experiment).

## Files

| File | Purpose |
|---|---|
| `docker-compose.two-nodes.yml` | Adds `alfresco2` (port 8081) to the main stack. It uses the same image, database, content store volume, ActiveMQ and Kafka, with a 2.5 GB memory limit |
| `alfresco-global-kafka-only.properties` | Kafka-only settings for node 2, used by the `lag` test |
| `cluster-check.py` | The tests (Python 3, standard library only) |

## Run

Start the main stack first, so node 1 creates the database before node 2 joins:

```bash
mvn package
docker compose -f docker/docker-compose.yml up -d --build
# wait until http://localhost:8080/alfresco responds, then add node 2:
docker compose -f docker/docker-compose.yml -f docker/cluster-experiment/docker-compose.two-nodes.yml up -d alfresco2
```

```bash
python3 docker/cluster-experiment/cluster-check.py exactly-once --docs 20
python3 docker/cluster-experiment/cluster-check.py alternating --docs 40 --updates 6 --workers 12
```

| Test | What it does | Passes when |
|---|---|---|
| `exactly-once` | Create on one node, update on the other, delete on the first | Each document has exactly its 3 events, in order, and Kafka and ActiveMQ received the same count |
| `alternating` | Each document is updated alternately through node 2 and node 1 | Every document's events are in order, and `resourceBefore` holds the true previous title |
| `lag` | Stops ActiveMQ while node 1 (dual mode) blocks and node 2 (Kafka-only) does not | Out-of-order events are the **expected** outcome. It shows why per-document order is not guaranteed across nodes. It restarts ActiveMQ when done |

The `lag` test needs node 2 in Kafka-only mode, and refuses to run otherwise:

```bash
NODE2_PROPERTIES=./cluster-experiment/alfresco-global-kafka-only.properties \
  docker compose -f docker/docker-compose.yml -f docker/cluster-experiment/docker-compose.two-nodes.yml up -d alfresco2
python3 docker/cluster-experiment/cluster-check.py lag --docs 5
```

`NODE2_PROPERTIES` is resolved relative to `docker/`, the main compose file's directory.

Run the tests a few minutes after node 2 starts. Right after a restart its first events can be slow and arrive out of
order, which is one of the findings. Remove node 2 when done:

```bash
docker compose -f docker/docker-compose.yml -f docker/cluster-experiment/docker-compose.two-nodes.yml rm -sf alfresco2
```
