import Config

# Don't connect to Kafka during tests.
config :kafka_telemetry_logger, start_consumer: false

config :logger, level: :warning
