defmodule KafkaTelemetryLogger.Application do
  @moduledoc """
  Starts a KafkaEx consumer group that consumes the device telemetry topic and
  logs every message's headers and payload.
  """

  use Application

  require Logger

  alias KafkaTelemetryLogger.TelemetryConsumer

  @impl true
  def start(_type, _args) do
    if Application.get_env(:kafka_telemetry_logger, :start_consumer, true) do
      start_consumer()
    else
      # Nothing to supervise (e.g. during tests).
      Supervisor.start_link([], strategy: :one_for_one, name: KafkaTelemetryLogger.Supervisor)
    end
  end

  defp start_consumer do
    topic = fetch_env!(:topic)
    consumer_group = fetch_env!(:consumer_group)

    auto_offset_reset =
      Application.get_env(:kafka_telemetry_logger, :auto_offset_reset, :earliest)

    Logger.info(
      "Starting device telemetry consumer: topic=#{topic} " <>
        "group=#{consumer_group} start_from=#{auto_offset_reset}"
    )

    children = [
      %{
        id: KafkaEx.Consumer.ConsumerGroup,
        start:
          {KafkaEx.Consumer.ConsumerGroup, :start_link,
           [
             TelemetryConsumer,
             consumer_group,
             [topic],
             # GenConsumer options (e.g. auto_offset_reset) are forwarded to the
             # spawned consumers. Read from the earliest offset by default since
             # this is an ephemeral group with no committed offsets.
             [
               auto_offset_reset: auto_offset_reset,
               commit_interval: 5_000
             ]
           ]},
        type: :supervisor
      }
    ]

    opts = [strategy: :one_for_one, name: KafkaTelemetryLogger.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp fetch_env!(key) do
    Application.fetch_env!(:kafka_telemetry_logger, key)
  end
end
