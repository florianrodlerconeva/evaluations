import Config

# --- Connection settings -----------------------------------------------------
#
# Defaults mirror the marimo notebook
# (ops/display-kafka-device-telemetry-topic.py). The dev Kafka cluster is
# reached via kubefwd port-forwarding, which writes an /etc/hosts entry so that
# `kafka-kafka-brokers` resolves to the forwarded PLAINTEXT broker on port 9092:
#
#     sudo kubefwd svc -n kafka
#
# Override any of these with environment variables, e.g.:
#
#     KAFKA_BROKERS=localhost:9092 KAFKA_TOPIC=my-topic mix run --no-halt

bootstrap_servers = System.get_env("KAFKA_BROKERS", "kafka-kafka-brokers:9092")

topic =
  System.get_env(
    "KAFKA_TOPIC",
    "event-hub-device-telemetry-evh-cev-dev-device-telemetry"
  )

# Read from the start of the topic by default (matches the notebook's
# "earliest" mode). Set to "latest" to only receive newly produced messages.
start_from =
  case System.get_env("KAFKA_START_FROM", "earliest") do
    "latest" -> :latest
    _ -> :earliest
  end

# Ephemeral consumer group so we never interfere with real consumers and always
# get a clean, uncommitted read (mirrors the notebook's random group id).
consumer_group =
  System.get_env(
    "KAFKA_CONSUMER_GROUP",
    "kafka-telemetry-logger-" <> Base.encode16(:crypto.strong_rand_bytes(4), case: :lower)
  )

# Parse "host:port,host2:port2" into KafkaEx's [{"host", port}] broker format.
brokers =
  bootstrap_servers
  |> String.split(",", trim: true)
  |> Enum.map(fn entry ->
    case String.split(String.trim(entry), ":", parts: 2) do
      [host, port] -> {host, String.to_integer(port)}
      [host] -> {host, 9092}
    end
  end)

config :kafka_ex, brokers: brokers

# Target topic that processed messages are (stub-)published to.
target_topic = System.get_env("KAFKA_TARGET_TOPIC", "device-telemetry-processed")

config :kafka_telemetry_logger,
  topic: topic,
  consumer_group: consumer_group,
  auto_offset_reset: start_from,
  target_topic: target_topic
