defmodule KafkaTelemetryLogger.Application do
  @moduledoc """
  Starts the Broadway pipeline and a KafkaEx consumer group.

  The consumer group fetches messages from the device telemetry topic and feeds
  them into the Broadway pipeline, which logs each message's headers and payload
  and publishes them to a target topic (stub). Offsets are committed only once
  Broadway has fully processed and published each batch.

  The Broadway pipeline is started *before* the consumer group so its producer
  process is registered and ready to receive batches.
  """

  use Application

  require Logger

  alias KafkaTelemetryLogger.{Pipeline, TelemetryConsumer}

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
      # Start the Broadway pipeline first so its producer is ready to receive
      # batches from the consumer.
      Pipeline,
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
             #
             # commit_interval is set high because commits are driven explicitly
             # (sync_commit) once Broadway acknowledges each batch.
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
