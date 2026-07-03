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

  alias KafkaTelemetryLogger.{PayloadWriter, Pipeline, TelemetryConsumer}

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

    # Larger fetches mean far fewer fetch/process/commit cycles when reading a
    # backlog (the default is only 1 MB, ~a handful of large telemetry messages).
    fetch_max_bytes = Application.get_env(:kafka_telemetry_logger, :fetch_max_bytes, 10_000_000)

    Logger.info(
      "Starting device telemetry consumer: topic=#{topic} " <>
        "group=#{consumer_group} start_from=#{auto_offset_reset}"
    )

    children = [
      # The payload writer must be up before the pipeline processes messages.
      PayloadWriter,
      # Start the Broadway pipeline before the consumer so its producer is ready
      # to receive batches.
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
             # Commits are async (batched every commit_interval) but only ever
             # cover offsets Broadway has already acknowledged. Larger fetches
             # amortise the per-cycle overhead when catching up on a backlog.
             [
               auto_offset_reset: auto_offset_reset,
               commit_interval: 5_000,
               fetch_options: [max_bytes: fetch_max_bytes]
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
