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

import_config "#{config_env()}.exs"
