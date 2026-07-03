defmodule KafkaTelemetryLogger.Pipeline do
  @moduledoc """
  The Broadway pipeline.

  * `handle_message/3` writes each message's headers and payload to a file (via
    `KafkaTelemetryLogger.PayloadWriter`) to keep large payloads off the console.
  * `handle_batch/4` forwards the batch to the target Kafka topic via
    `KafkaTelemetryLogger.TargetProducer` (a stub).

  Messages enter through `KafkaTelemetryLogger.Producer`, which is fed by the
  KafkaEx consumer. Acknowledgement of a batch is what ultimately lets the
  consumer commit its source offsets, so a batch is only "done" once it has
  been logged *and* successfully published.
  """

  use Broadway

  require Logger

  alias Broadway.Message
  alias KafkaTelemetryLogger.{Decoder, PayloadWriter, Producer, TargetProducer}

  def start_link(_opts) do
    config = Application.get_env(:kafka_telemetry_logger, __MODULE__, [])
    processor_concurrency = Keyword.get(config, :processor_concurrency, 4)
    batcher_concurrency = Keyword.get(config, :batcher_concurrency, 2)
    batch_size = Keyword.get(config, :batch_size, 100)
    batch_timeout = Keyword.get(config, :batch_timeout, 1_000)

    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      # Keep the producer at concurrency 1. All KafkaEx partition consumers
      # deliver to a single named producer (see `Producer.deliver/3`); to add
      # parallelism scale the processors/batchers below, NOT the producer.
      producer: [
        module: {Producer, []},
        concurrency: 1
      ],
      # Route each source Kafka partition to a consistent processor and batcher
      # so message ordering *within* a partition is preserved even though
      # multiple partitions are processed in parallel.
      partition_by: &partition_of/1,
      processors: [
        default: [concurrency: processor_concurrency]
      ],
      batchers: [
        default: [
          concurrency: batcher_concurrency,
          batch_size: batch_size,
          batch_timeout: batch_timeout
        ]
      ]
    )
  end

  # Partitioning key: the source Kafka partition. `phash2(partition) rem
  # concurrency` then maps each partition to a fixed processor/batcher.
  defp partition_of(%Message{metadata: %{partition: partition}}), do: partition

  @impl true
  def handle_message(_processor, %Message{data: record} = message, _context) do
    PayloadWriter.write(%{
      partition: record.partition,
      offset: record.offset,
      headers: Decoder.decode_headers(record.headers),
      payload: Decoder.decode_value(record.value)
    })

    message
  end

  @impl true
  def handle_batch(_batcher, messages, _batch_info, _context) do
    records = Enum.map(messages, & &1.data)

    case TargetProducer.publish(records) do
      :ok ->
        messages

      {:error, reason} ->
        # Mark every message failed so the source offsets are NOT committed and
        # the batch will be re-fetched and reprocessed.
        Logger.error("Failed to publish batch to target topic: #{inspect(reason)}")
        Enum.map(messages, &Message.failed(&1, reason))
    end
  end
end
