# kafka_telemetry_logger

An Elixir application that consumes messages from the device telemetry Kafka
topic using [`kafka_ex`](https://kafka-ex.hexdocs.pm/), processes them through a
[Broadway](https://hexdocs.pm/broadway) pipeline that logs each message's
**headers** and **payload**, and publishes them to a target topic. Source
offsets are committed only after a batch has been fully processed and published.

It reads the same topic as the marimo notebook
`ops/display-kafka-device-telemetry-topic.py` and applies the same
header-decoding strategy.

## Architecture

`kafka_ex` is push-based (it invokes `handle_message_set/2`), while Broadway is a
demand-driven GenStage pipeline. This app bridges the two and lets Broadway drive
the offset commits:

```
KafkaEx GenConsumer ──deliver batch (ref)──▶ Producer (GenStage)
   (blocks on ref)                              │ emits Broadway.Message
        ▲                                       ▼
        │                              processors  ──▶ handle_message  (log headers + payload)
        │                                       │
        │                              batcher    ──▶ handle_batch ──▶ TargetProducer.publish (stub)
        │                                       │ ack(success / fail)
        └──{:batch_complete, ref, :ok|:error}◀──┘  (custom Acknowledger)
   sync_commit on :ok  /  raise → reprocess on :error
```

- **`KafkaTelemetryLogger.TelemetryConsumer`** — the `KafkaEx.Consumer.GenConsumer`.
  On each fetched batch it hands the messages to the Broadway producer (tagged
  with a unique `ref`) and **blocks** until Broadway acknowledges the whole
  batch. On success it returns `{:sync_commit, state}`; on failure it raises, so
  offsets are *not* committed and the batch is re-fetched (let-it-crash). This
  blocking also gives natural backpressure.
- **`KafkaTelemetryLogger.Producer`** — a custom GenStage producer (not
  `broadway_kafka`). It buffers each delivered batch, emits `Broadway.Message`s
  on demand, tracks per-`ref` completion, and notifies the consumer with
  `{:batch_complete, ref, :ok | :error}` once every message in the batch is acked.
- **`KafkaTelemetryLogger.Acknowledger`** — a `Broadway.Acknowledger` that routes
  per-message ack results back to the producer.
- **`KafkaTelemetryLogger.Pipeline`** — the `Broadway` pipeline. `handle_message/3`
  writes each message's headers + payload to a file; `handle_batch/4` publishes
  the batch via the target producer.
- **`KafkaTelemetryLogger.PayloadWriter`** — a single process that writes headers
  + payloads to a file (buffered) and logs throughput (messages/second) to the
  console. Payloads are kept off the console so it stays readable and fast, and
  the console can be used to watch progress.
- **`KafkaTelemetryLogger.TargetProducer`** — a **stub** that pretends to publish
  to the target Kafka topic (it just logs and returns `:ok`; it does not talk to
  Kafka). Replace `publish/1` with a real producer to make it live.

The KafkaEx consumer group uses an **ephemeral consumer group** (random suffix)
and reads from the beginning of the topic (`auto_offset_reset: :earliest`).

### Parallelism & ordering

`KafkaEx.Consumer.ConsumerGroup` starts **one `GenConsumer` per assigned
partition**, so partitions are fetched in parallel (and spread across nodes if
you run the same group on several BEAM instances). Each consumer delivers to the
**single** Broadway producer with its own `ref`/caller, so per-`ref` completion
tracking and offset commits stay independent and correct across consumers.

Downstream throughput is governed by the Broadway stage concurrency, not the
partition count — tune it via config (defaults shown):

```elixir
config :kafka_telemetry_logger, KafkaTelemetryLogger.Pipeline,
  processor_concurrency: 4,
  batcher_concurrency: 2,
  batch_size: 100,
  batch_timeout: 1_000
```

The pipeline sets `partition_by: &(&1.metadata.partition)` at the root, so each
source Kafka partition is pinned to a consistent processor and batcher —
**ordering within a partition is preserved** while different partitions run in
parallel. Keep the **producer at `concurrency: 1`** (scale processors/batchers
instead); `Producer.deliver/3` matches a single producer name strictly and will
crash loudly if that invariant is broken.

Header/value decoding (`KafkaTelemetryLogger.Decoder`) mirrors the notebook:

1. **UTF-8** — the common case.
2. **Azure IoT Hub binary string envelope** — IoT Hub encodes string header
   values as `marker byte (0xA0–0xBF) + uint8 length + UTF-8 data`.
3. **Base64 fallback** — for any remaining binary values (e.g. the
   `iothub-enqueuedtime` timestamp, `0x83` marker).

## Prerequisites

Kafka must be reachable. Point `kubectl` at the cluster, then forward the Kafka
broker port (9092) to your machine with kubefwd, which writes an `/etc/hosts`
entry so `kafka-kafka-brokers` resolves to the forwarded PLAINTEXT broker:

```sh
sudo kubefwd svc -n kafka
```

> The app requires the broker to be reachable at boot — start kubefwd first.

Also install dependencies once:

```sh
mix deps.get
```

## Running

```sh
mix run --no-halt
```

You should see log lines like:

Payloads go to a file (see below); the **console shows only progress**: startup,
per-batch publishes, and a throughput meter:

```
13:11:33.906 [info] Starting device telemetry consumer: topic=event-hub-device-telemetry-... start_from=earliest
13:11:33.911 [info] Writing message payloads to /.../telemetry_payloads.log
13:11:38.505 [info] [target-producer stub] published 100 message(s) to device-telemetry-processed
13:11:39.914 [info] consumed 536 messages (268/s)
13:11:41.914 [info] consumed 1082 messages (273/s)
```

The payload file (`telemetry_payloads.log` by default) gets one line per message:

```
[partition=0 offset=65350]	headers=message_type=charger_report:v1.0.0, device_id=..., iothub-enqueuedtime=gwAAAZ8AYbIW, ...	payload={"report:charger:active:energy:+":[...]}
```

(`iothub-enqueuedtime` is a binary timestamp header, so it shows up base64-encoded
— matching the notebook.)

## Throughput & tuning

Reading a topic from the start goes only as fast as the fetch → process →
commit cycle allows. The defaults are tuned for catching up on a backlog:

- **`fetch_max_bytes`** (default 10 MB; KafkaEx's default is only 1 MB) — the
  biggest lever for a **single-partition** topic, where there's one consumer and
  one fetch in flight at a time. Bigger fetches amortise the per-fetch
  round-trip. Override per run with `KAFKA_FETCH_MAX_BYTES`.
- **`batch_timeout`** is small (100 ms) — since the consumer blocks per fetched
  batch, a large batcher timeout would stall every cycle for its full duration.
- **`:async_commit`** batches offset commits (every `commit_interval`) instead of
  a broker round-trip per batch, while still only committing processed offsets.
- Payloads are written to a **file**, not the console — logging large payloads to
  the console is itself a major bottleneck.

For a multi-partition topic, throughput also scales with the number of consumers
(one per partition) and the processor/batcher concurrency.

## Configuration

All settings have defaults matching the notebook and can be overridden with
environment variables:

| Variable                | Default                                                        | Description                                  |
| ----------------------- | ------------------------------------------------------------- | -------------------------------------------- |
| `KAFKA_BROKERS`         | `kafka-kafka-brokers:9092`                                     | Comma-separated `host:port` broker list      |
| `KAFKA_TOPIC`           | `event-hub-device-telemetry-evh-cev-dev-device-telemetry`     | Topic to consume                             |
| `KAFKA_START_FROM`      | `earliest`                                                     | `earliest` or `latest`                       |
| `KAFKA_CONSUMER_GROUP`  | `kafka-telemetry-logger-<random>`                             | Consumer group id (ephemeral by default)     |
| `KAFKA_TARGET_TOPIC`    | `device-telemetry-processed`                                  | Topic the (stub) publisher targets           |
| `KAFKA_PAYLOAD_FILE`    | `telemetry_payloads.log`                                      | File that headers/payloads are written to    |
| `KAFKA_FETCH_MAX_BYTES` | `10000000`                                                    | Bytes per KafkaEx fetch (throughput lever)   |

Example:

```sh
KAFKA_BROKERS=localhost:9092 KAFKA_START_FROM=latest mix run --no-halt
```

## Troubleshooting

**`JoinGroup` times out every ~1s / "Receiving data from broker … timed out".**
KafkaEx's default `sync_timeout` (1000ms) is shorter than the broker's initial
consumer-group rebalance delay (`group.initial.rebalance.delay.ms`, ~3s), so the
`JoinGroup` response never arrives in time. Fixed by setting
`config :kafka_ex, sync_timeout: 30_000` in `config/config.exs`.

**`Could not connect to broker "kafka-kafka-N…" :nxdomain` / `:no_broker`.**
The bootstrap connection succeeds, but the group coordinator (or a partition
leader) is a per-broker pod hostname that kubefwd hasn't mapped in `/etc/hosts`.
kubefwd only forwards broker pods that are Running/Ready when it starts, so a
missing/not-ready broker pod leaves a gap. Because the coordinator is chosen by
hashing the (random) consumer group id, some runs land on the missing broker and
fail while others succeed.

Check which brokers are forwarded:

```sh
grep -oE '127[0-9.]+ +kafka-kafka-[0-9]\.kafka-kafka-brokers' /etc/hosts
```

If a broker is missing, ensure its pod is up (`kubectl get pods -n kafka`) and
restart kubefwd so it forwards all broker pods.

## Tests

The decoder logic and the Broadway pipeline (including the commit-on-success and
no-commit-on-failure paths) are covered by tests that need no broker:

```sh
mix test
```

The pipeline tests drive the real Broadway topology by delivering batches to the
producer and asserting on the `{:batch_complete, ref, :ok | :error}` signal that
gates the source-offset commit.

## Layout

- `lib/kafka_telemetry_logger/application.ex` — supervision tree; starts the
  payload writer, the Broadway pipeline, and then the KafkaEx consumer group.
- `lib/kafka_telemetry_logger/telemetry_consumer.ex` — `GenConsumer` that feeds
  batches into Broadway and commits offsets once Broadway acknowledges them.
- `lib/kafka_telemetry_logger/producer.ex` — GenStage producer bridging KafkaEx
  into Broadway.
- `lib/kafka_telemetry_logger/acknowledger.ex` — `Broadway.Acknowledger` routing
  acks back to the producer.
- `lib/kafka_telemetry_logger/pipeline.ex` — Broadway pipeline: writes headers +
  payload to a file and publishes to the target topic.
- `lib/kafka_telemetry_logger/payload_writer.ex` — writes payloads to a file and
  logs throughput to the console.
- `lib/kafka_telemetry_logger/target_producer.ex` — stub publisher for the target
  topic.
- `lib/kafka_telemetry_logger/decoder.ex` — value/header decoding.
- `config/runtime.exs` — connection settings and env-var overrides.
