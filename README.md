# alfresco-event2-kafka-fanout

An Alfresco Content Services (Community) repository module that publishes **Event2** repository events
(`NODE_CREATED`, `NODE_UPDATED`, `NODE_DELETED`, association events) to **Kafka**, alongside or instead of the
stock ActiveMQ topic `alfresco.repo.event2`. Each target is switched on or off with `alfresco-global.properties`.

It needs no changes to Alfresco itself. It ships as an AMP (or a plain JAR plus its libraries).

## Why Kafka

Stock Alfresco publishes Event2 to a topic on ActiveMQ Classic. ActiveMQ is a good message broker. Kafka is a
durable, replayable event log, and for a stream of repository change events that difference matters.

### Advantages over ActiveMQ Classic for this stream

| | ActiveMQ Classic (stock `alfresco.repo.event2` topic) | Kafka (this module) |
|---|---|---|
| **Consumer offline** | A plain topic delivers only to subscribers connected at that moment. Events published while a consumer is down are lost unless it uses durable subscriptions or virtual topics | Events are stored for the topic's retention period. A consumer that was down carries on from its last offset |
| **Replay** | Not possible: a delivered message is gone | Any consumer can rewind to an earlier offset or timestamp to reprocess, rebuild an index, or recover from a bug |
| **New consumers** | Start with events from now on | Can start from the beginning of the retained log, or from now |
| **Scaling consumers** | Each subscriber gets every message on its own; sharing the load needs virtual topics or queues | A consumer group shares partitions across instances. Each team or application has its own group, independent of the others |
| **Ordering** | One ordered stream per subscriber | Ordered per node: the message key is the node id, so all events for a node land on one partition |
| **Consumer throughput and backlog** | Tuned for in-flight messages; a growing backlog puts pressure on the broker | Consumers read in parallel across partitions, large backlogs are kept on disk, and a slow consumer does not affect the others |
| **Duplicate protection** | The failover transport suppresses duplicate sends on reconnect; consumers deduplicate themselves | The idempotent producer (on by default here) means client retries never write duplicates or reorder a partition. Consumers are still at-least-once and should deduplicate on the event `id` |
| **Ecosystem** | JMS / AMQP clients | Kafka Connect sinks (Elasticsearch/OpenSearch, S3 and data lakes, databases), stream processing (Kafka Streams, Flink), Schema Registry, managed services (Amazon MSK, Confluent Cloud, Azure Event Hubs Kafka API, Aiven) |
| **Monitoring** | Queue and topic depth | Consumer lag per group and partition shows exactly how far behind each consumer is |
| **Multi-site** | Network of brokers | MirrorMaker 2 / cluster linking replicate topics across regions |

### How this modernises an Alfresco platform

- **Content changes become a shared enterprise data stream.** Document created, updated and deleted events sit on the same platform as the rest of the organisation's events. Downstream teams can join them with business events, feed analytics, or build their own read models without touching Alfresco.
- **Integrations push instead of poll.** Search indexes, records and compliance tools, data lakes, notifications and workflow engines can react to events, instead of polling the REST API or audit log.
- **Recovery by replay.** A downstream index or projection can be rebuilt by replaying the topic rather than re-crawling the repository. This works within Kafka's retention period, or with log compaction on the node-id key, which keeps the latest event per node.
- **Standard, cloud-native infrastructure.** Many organisations already run Kafka, often as a managed service with existing security, monitoring and governance. Alfresco events join that estate rather than needing a separate broker for every new integration.
- **Gradual migration with low risk.** Turn on dual publish, move or build consumers on Kafka at their own pace, then switch ActiveMQ off where nothing depends on it. Each step is a property change, and it can be reversed.
- **Same event contract.** Messages on Kafka have the same Event2 JSON (`RepoEvent`, CloudEvents-style) as on ActiveMQ, so existing parsing code and the published schemas still apply.

### What it does not change

Be clear about these before moving to Kafka-only:

- **Alfresco's own services still use ActiveMQ.** The Transform Service and the repository's transform and rendition queues, Search Enterprise live indexing, Desktop Sync and Out-of-Process SDK applications all use ActiveMQ. This module only adds Kafka for the Event2 stream. Keep `repo.event2.route.activemq.enabled=true`, and keep ActiveMQ itself, while any of them are in use.
- **Kafka has the same delivery guarantee as ActiveMQ.** Both receive the same Event2 stream, published from Alfresco after commit, and both brokers lose events the same way: if Alfresco stops before sending, or if a send fails. The one difference is during a broker outage. ActiveMQ's failover connection waits for the broker to return and keeps the events. Kafka gives up after `deliveryTimeoutMs` and, with `failOnError=false`, drops them (see [Delivery behaviour](#delivery-behaviour)). Like the stock ActiveMQ topic, Kafka is an integration and analytics stream, not a transactional system of record for repository changes.
- **The publishing mechanism is unchanged.** The module keeps Alfresco's own Event2 publishing as it is: events are sent after commit, one at a time, from Alfresco's single ordered sender thread. The route waits for each acknowledgement, so the publish rate is set by Alfresco, not the broker. Kafka's benefits are on the consumer side. With dual publish, each event goes to both brokers in turn. No benchmarks have been run yet.
- **Not exactly-once.** The idempotent producer only removes duplicates from the Kafka client's own retries. Events are published after commit, may be missed during a Kafka outage, and are delivered to consumers at least once. Make consumers idempotent by deduplicating on the event `id`.
- **Kafka is more to run.** Partitions, retention, replication and consumer groups all need planning. Where nothing needs replay or the Kafka ecosystem, the stock ActiveMQ topic remains the simpler option.
- **Some features are JMS-specific.** Message selectors, request/reply and per-message priority have no direct Kafka equivalent. Event2 consumers do not normally rely on them.

## How it works

```
Event2 (EventGenerator, after commit)
   │
KeyedEvent2MessageProducer ──► direct:alfresco.repo.event2.fanout      (Event2FanOutRouteBuilder)
                                        │ multicast, in order; every target attempted
                         ┌──────────────┴───────────────┐
                 ActiveMQ target                   Kafka target
          (repo.event2.route.activemq.*)     (repo.event2.route.kafka.*)
          → amqp:topic:alfresco.repo.event2  → kafka:alfresco.repo.event2
            message unchanged                  key = node id, JMS headers removed
```

The module hooks in at two supported extension points:

| File | Loaded by | Purpose |
|---|---|---|
| `alfresco/module/alfresco-event2-kafka-fanout/module-context.xml` | the module service, after the core contexts | Replaces the core `event2MessageProducer` bean with `KeyedEvent2MessageProducer`. It sends to `repo.event2.producer.endpoint` and adds the node id used as the Kafka key |
| `alfresco/extension/subsystems/Messaging/default/default/event2-kafka-fanout-context.xml` | the Messaging subsystem | Declares `Event2FanOutRouteBuilder` next to `alfrescoCamelContext`, whose `<contextScan/>` starts the routes |
| `alfresco/module/alfresco-event2-kafka-fanout/alfresco-global.properties` | global properties, before your own `alfresco-global.properties` | Defaults for every setting below |

The Kafka message key is the id of the node the event is about: the node itself, the child of a child association,
or the source of a peer association. All events for a node land on the same partition, so their order is kept.

## Compatibility

| Module | ACS Community | alfresco-repository | Camel | Java |
|---|---|---|---|---|
| 1.0.x | 26.2.x | 26.2.0.96 | 4.18.2 | 21 |

The module's Camel version must match the one in `alfresco.war`. To target another ACS release, change
`alfresco.repository.version` and `camel.version` in `pom.xml`, then rebuild and run the tests.

## Build

```bash
mvn package
```

This produces:

- `target/alfresco-event2-kafka-fanout-<version>.amp`: the module jar plus `camel-kafka`, `kafka-clients`, `lz4-java`, `zstd-jni` and `snappy-java`. These are the only libraries `alfresco.war` does not already have.
- `target/alfresco-event2-kafka-fanout-<version>.jar`: the module on its own.

## Install

**AMP (recommended)**, for example in a Docker image:

```dockerfile
FROM alfresco/alfresco-content-repository-community:26.2.0
USER root
COPY target/alfresco-event2-kafka-fanout-*.amp /usr/local/tomcat/amps/
RUN java -jar /usr/local/tomcat/alfresco-mmt/alfresco-mmt*.jar install \
        /usr/local/tomcat/amps/alfresco-event2-kafka-fanout-*.amp /usr/local/tomcat/webapps/alfresco -nobackup -force
USER alfresco
```

**JAR:** copy the module jar and the five libraries listed above into `webapps/alfresco/WEB-INF/lib`.
The AMP's `lib/` folder contains exactly that set.

On startup the log shows `Installing module 'alfresco-event2-kafka-fanout' version 1.0.0`.

### Using the published artifacts

Released versions are on Maven Central as `org.kamranzafar.alfresco:alfresco-event2-kafka-fanout`, with the AMP
published alongside the jar:

```xml
<dependency>
  <groupId>org.kamranzafar.alfresco</groupId>
  <artifactId>alfresco-event2-kafka-fanout</artifactId>
  <version>1.0.0</version>
  <type>amp</type>
</dependency>
```

Or download the AMP directly:

```bash
curl -O https://repo1.maven.org/maven2/org/kamranzafar/alfresco/alfresco-event2-kafka-fanout/1.0.0/alfresco-event2-kafka-fanout-1.0.0.amp
```

## Configuration

Put these in `alfresco-global.properties`. With the defaults the module is installed but changes nothing: events go
only to ActiveMQ, unchanged.

| Property | Default | Meaning |
|---|---|---|
| `repo.event2.route.activemq.enabled` | `true` | Publish to the stock ActiveMQ topic |
| `repo.event2.route.activemq.endpoint` | `${repo.event2.topic.endpoint}` | ActiveMQ target (the stock setting) |
| `repo.event2.route.kafka.enabled` | `false` | Publish to Kafka |
| `repo.event2.route.kafka.endpoint` | `kafka:alfresco.repo.event2?brokers=localhost:9092&requestRequiredAcks=all&enableIdempotence=true&maxBlockMs=5000&requestTimeoutMs=5000&deliveryTimeoutMs=10000` | Camel Kafka endpoint: topic, brokers, security and producer options |
| `repo.event2.route.kafka.failOnError` | `false` | `false`: Kafka failures are logged and not reported to the sender. `true`: they are reported to the sender like ActiveMQ failures. Either way, a Kafka failure never stops delivery to ActiveMQ |
| `repo.event2.producer.endpoint` | `direct:alfresco.repo.event2.fanout` | Set to `${repo.event2.topic.endpoint}` to bypass the module completely |

| Goal | Settings |
|---|---|
| Dual publish | `repo.event2.route.kafka.enabled=true` |
| Kafka only | also `repo.event2.route.activemq.enabled=false` |
| Back to stock behaviour without uninstalling | `repo.event2.producer.endpoint=${repo.event2.topic.endpoint}` |

Security goes on the endpoint, for example `&securityProtocol=SASL_SSL&saslMechanism=PLAIN&saslJaasConfig=RAW(...)`.

Create the Kafka topic with the number of partitions you need before enabling the Kafka target. Each node's events
stay in order because they all go to one partition, chosen from the node id. Adding partitions later changes that
mapping, so events for a node that is changing at that moment can be read out of order.

Settings take effect at startup, or when the Messaging subsystem restarts.

> **Put endpoint URIs in `alfresco-global.properties`, not `JAVA_OPTS`.** The image's `catalina.sh` runs `JAVA_OPTS`
> through `eval`, so the `&` and `(` in Kafka and ActiveMQ URIs break startup
> (`syntax error near unexpected token '('`). Simple values such as `-Drepo.event2.route.kafka.enabled=true` are fine.

## Delivery behaviour

- **Order:** for each event, ActiveMQ first, then Kafka. With the default `async` send strategy, Event2 sends from a single ordered thread after commit, so uploads and edits don't wait on either broker. With `repo.event2.send.strategy=direct`, the request waits for both sends after commit. On Kafka, order is guaranteed per node: all of a node's events go to one partition. Events for different nodes can be read in a different order than they happened.
- **Targets are independent of each other's errors.** Each event is always attempted on every enabled target, and an error on one does not stop delivery to the other. A target that *blocks* is different: both targets share the single sender thread, so a stalled send holds up the other target until it returns (see "ActiveMQ outage" below).
- **ActiveMQ error:** if an ActiveMQ send fails with an error, Kafka still gets the event. Once Kafka has been attempted, the error is reported to Alfresco's Event2 sender, which logs it and drops the event for ActiveMQ, as in stock Alfresco.
- **ActiveMQ outage:** when the ActiveMQ broker is down, Alfresco's failover connection doesn't raise an error. It blocks until the broker returns, about 50 s in testing. No events were lost on either broker, but Kafka received nothing until ActiveMQ was back. In Kafka-only mode (`repo.event2.route.activemq.enabled=false`), an ActiveMQ outage doesn't affect Kafka (see [TESTING.md](TESTING.md)).
- **Kafka failure, with `failOnError=false`:** logged as `ERROR [event2.kafka.Event2FanOutRouteBuilder] Failed to publish event2 message to Kafka: ...`. The event is **not** retried later; Kafka gets at-most-once delivery during an outage.
- **Kafka recovery:** automatic. The next event after Kafka returns is published normally.
- **Several repository nodes:** each node publishes only the transactions it commits, so every change is published
  exactly once. But order per document is **not** guaranteed across nodes. If two nodes change the same document in
  quick succession and the first is slower to publish (just restarted, stalled on a broker, GC pause), its event
  arrives second. Testing showed an `Updated` arriving after the `Deleted` it preceded. Stock Alfresco on ActiveMQ
  behaves the same. Consumers should order a node's events by the event `time` (commit time) and ignore older events
  (see [TESTING.md](TESTING.md), section 8).

### Measured during a Kafka outage (Docker stack below)

- Uploads stayed fast (about 0.1 s) and ActiveMQ received every event.
- Every event waited the full `deliveryTimeoutMs` (10 s) on Kafka before the sender moved on. Because the sender is a single ordered thread, this **limits Event2 throughput to about one event every 10 s for as long as Kafka is down**. ActiveMQ delivery of each later event is delayed by that wait, and events queue up in memory in Alfresco's event2 sender.

On a busy repository a long Kafka outage therefore delays ActiveMQ consumers too. Ways to reduce this:

- Lower `deliveryTimeoutMs`. It must be at least `requestTimeoutMs` plus `lingerMs`.
- Use `failOnError=true` only if Kafka is the system of record.
- Planned: make the Kafka leg asynchronous, with a bounded queue or a circuit breaker, so a Kafka outage cannot slow ActiveMQ.

## Tests

How the module was tested, with results and versions: [TESTING.md](TESTING.md).

```bash
mvn test
```

There are 18 tests:

- `Event2FanOutRouteBuilderTest`: every combination of enabled targets, the Kafka key and header handling, failure isolation between the two targets (each still gets the event when the other fails) and error reporting, the bypass mode, and a check that the default Kafka endpoint options are valid.
- `KeyedEvent2MessageProducerTest`: the key for each resource type, and no header added when sending straight to a broker.
- `ModuleWiringTest`: loads the module's Spring XML with its default properties.

### End to end with Docker

`docker/docker-compose.yml` runs ACS 26.2 (with the AMP installed), Postgres, ActiveMQ and Kafka, with no search or
transforms. `docker/alfresco-global.properties` turns Kafka on.

```bash
mvn package
docker compose -f docker/docker-compose.yml up -d --build

# create a document
curl -u admin:admin -H 'Content-Type: application/json' \
  -X POST http://localhost:8080/alfresco/api/-default-/public/alfresco/versions/1/nodes/-my-/children \
  -d '{"name":"hello.txt","nodeType":"cm:content"}'

# read the Kafka topic (key = node id)
docker compose -f docker/docker-compose.yml exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic alfresco.repo.event2 --from-beginning --property print.key=true

docker compose -f docker/docker-compose.yml down -v
```

### Event coverage check

`docker/event-matrix.py` (Python 3, standard library only) makes every kind of repository change through the REST
API against the running stack. For each change it prints the events that reached Kafka, with their keys, and checks
that ActiveMQ received the same number. It exits with status 1 on any mismatch, so it can gate an upgrade to a new
ACS version.

```bash
python3 docker/event-matrix.py
```

See [TESTING.md](TESTING.md) for the results against ACS Community 26.2.0, the versions tested, the Kafka outage
and recovery test, and what has not been tested yet.

### Ordering check

The test stack's `kafka-init` service creates `alfresco.repo.event2` with 6 partitions (`EVENT2_TOPIC_PARTITIONS`)
before Alfresco starts. `docker/ordering-check.py` runs many documents through create, a series of updates and delete
in parallel. It then checks that each document's events are on one partition and in the order they were made. It exits
with status 1 on any violation.

```bash
python3 docker/ordering-check.py --docs 60 --updates 10 --workers 16
```

### Two repository nodes (experiment)

[`docker/cluster-experiment/`](docker/cluster-experiment/README.md) adds a second repository node and tests
exactly-once publishing and per-document ordering across nodes. Alfresco Community does not support clustering, so
this is an experiment on an unsupported setup, not a cluster reference.

## Uninstalling

Removing an AMP from an installed `alfresco.war` is not supported by the MMT. Rebuild the image without it, or set
`repo.event2.producer.endpoint=${repo.event2.topic.endpoint}` and `repo.event2.route.kafka.enabled=false`.

## License

Copyright 2026 Kamran Zafar. Licensed under the [Apache License, Version 2.0](LICENSE); see [NOTICE](NOTICE) for the
licenses of the bundled libraries.

The module compiles against Alfresco's `alfresco-repository` but does not include it: the jar is supplied by the
Alfresco installation it is deployed into, which remains under Alfresco's own license (LGPL v3 for Community Edition).
