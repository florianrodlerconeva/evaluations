# kafka_telemetry_logger

A small Elixir application that consumes messages from the device telemetry
Kafka topic using [`kafka_ex`](https://kafka-ex.hexdocs.pm/) and logs the
**headers** and **payload** of every message.

It is the streaming/logging counterpart to the marimo notebook
`ops/display-kafka-device-telemetry-topic.py`: it connects to the same dev
cluster, reads the same topic, and applies the same header-decoding strategy —
but instead of rendering a table, it just logs each message continuously.

## What it does

- Starts a `KafkaEx.Consumer.ConsumerGroup` with an **ephemeral consumer group**
  (random suffix) so it never interferes with real consumers and always gets a
  clean, uncommitted read.
- Consumes from the beginning of the topic (`auto_offset_reset: :earliest`).
- For each message, logs `partition`, `offset`, decoded `headers`, and the
  decoded `payload`.

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

```
12:00:20.550 [info] Starting device telemetry consumer: topic=event-hub-device-telemetry-evh-cev-dev-device-telemetry group=kafka-telemetry-logger-4ece202b start_from=earliest
12:00:21.100 [info] Kafka message [partition=3 offset=142857]
  headers: message_type=telemetry, tenant=acme, iothub-enqueuedtime=g...==
  payload: {"deviceId":"...","measurements":[...]}
```

## Configuration

All settings have defaults matching the notebook and can be overridden with
environment variables:

| Variable                | Default                                                        | Description                                  |
| ----------------------- | ------------------------------------------------------------- | -------------------------------------------- |
| `KAFKA_BROKERS`         | `kafka-kafka-brokers:9092`                                     | Comma-separated `host:port` broker list      |
| `KAFKA_TOPIC`           | `event-hub-device-telemetry-evh-cev-dev-device-telemetry`     | Topic to consume                             |
| `KAFKA_START_FROM`      | `earliest`                                                     | `earliest` or `latest`                       |
| `KAFKA_CONSUMER_GROUP`  | `kafka-telemetry-logger-<random>`                             | Consumer group id (ephemeral by default)     |

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

The decoder logic is covered by unit tests (no broker required):

```sh
mix test
```

## Layout

- `lib/kafka_telemetry_logger/application.ex` — supervision tree; starts the
  consumer group.
- `lib/kafka_telemetry_logger/telemetry_consumer.ex` — `GenConsumer` that logs
  each message's headers and payload.
- `lib/kafka_telemetry_logger/decoder.ex` — value/header decoding.
- `config/runtime.exs` — connection settings and env-var overrides.
