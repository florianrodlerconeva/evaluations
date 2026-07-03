import Config

# Keep the log output focused on the message contents.
config :logger, :console,
  format: "$time [$level] $message\n",
  level: :info

# KafkaEx client-wide configuration. The broker list and consumer group are
# finalised at runtime (see config/runtime.exs) so they can be overridden with
# environment variables without recompiling.
config :kafka_ex,
  # Use the modern kayrock-based client so that record headers and the v2
  # message format are available.
  kafka_version: "kayrock",
  client_id: "kafka_telemetry_logger",
  disable_default_worker: true,
  # Default is 1000ms, which is shorter than the broker's initial consumer
  # group rebalance delay (group.initial.rebalance.delay.ms, ~3s). A too-short
  # receive timeout makes JoinGroup time out and retry forever, so give broker
  # requests room to complete.
  sync_timeout: 30_000

# Whether to start the Kafka consumer group when the application boots. Turned
# off in test so the suite does not require a live broker.
config :kafka_telemetry_logger, start_consumer: true

# Broadway pipeline tuning. Multiple KafkaEx consumers (one per assigned
# partition) feed a single producer; downstream parallelism is governed by
# these stage concurrencies, and `partition_by` keeps per-partition ordering.
# Raise the concurrencies to scale throughput with your partition count.
#
# batch_timeout is small: because the consumer blocks per fetched batch, the
# batcher never fills to batch_size across fetches, so a large timeout would
# stall every cycle for its full duration.
config :kafka_telemetry_logger, KafkaTelemetryLogger.Pipeline,
  processor_concurrency: 4,
  batcher_concurrency: 2,
  batch_size: 100,
  batch_timeout: 100

# Max bytes per KafkaEx fetch. Bigger = fewer fetch/process/commit cycles when
# catching up on a backlog (default in KafkaEx is only 1 MB).
config :kafka_telemetry_logger, fetch_max_bytes: 10_000_000

# How often the payload writer logs throughput (messages/second) to the console.
config :kafka_telemetry_logger, progress_interval_ms: 2_000

import_config "#{config_env()}.exs"
