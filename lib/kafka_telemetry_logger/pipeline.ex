defmodule KafkaTelemetryLogger.Pipeline do
  @moduledoc """
  The Broadway pipeline.

  * `handle_message/3` logs each message's headers and payload (the original
    behaviour of the app).
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
  alias KafkaTelemetryLogger.{Decoder, Producer, TargetProducer}

  def start_link(_opts) do
    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      producer: [
        module: {Producer, []},
        concurrency: 1
      ],
      processors: [
        default: [concurrency: 2]
      ],
      batchers: [
        default: [concurrency: 1, batch_size: 100, batch_timeout: 1_000]
      ]
    )
  end

  @impl true
  def handle_message(_processor, %Message{data: record} = message, _context) do
    headers = Decoder.decode_headers(record.headers)
    payload = Decoder.decode_value(record.value)

    Logger.info("""
    Kafka message [partition=#{record.partition} offset=#{record.offset}]
      headers: #{format_headers(headers)}
      payload: #{payload}\
    """)

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

  defp format_headers(headers) when map_size(headers) == 0, do: "(none)"

  defp format_headers(headers) do
    headers
    |> Enum.map(fn {key, value} -> "#{key}=#{value}" end)
    |> Enum.join(", ")
  end
end
