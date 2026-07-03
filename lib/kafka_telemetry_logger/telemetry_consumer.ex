defmodule KafkaTelemetryLogger.TelemetryConsumer do
  @moduledoc """
  A `KafkaEx.Consumer.GenConsumer` that logs the headers and payload of every
  message it consumes from the device telemetry topic.
  """

  use KafkaEx.Consumer.GenConsumer

  require Logger

  alias KafkaEx.Messages.Fetch.Record
  alias KafkaTelemetryLogger.Decoder

  @impl true
  def handle_message_set(message_set, state) do
    Enum.each(message_set, &log_message/1)
    # Commit asynchronously; we only care about reading and logging.
    {:async_commit, state}
  end

  defp log_message(%Record{} = record) do
    headers = Decoder.decode_headers(record.headers)
    payload = Decoder.decode_value(record.value)

    Logger.info("""
    Kafka message [partition=#{record.partition} offset=#{record.offset}]
      headers: #{format_headers(headers)}
      payload: #{payload}\
    """)
  end

  defp format_headers(headers) when map_size(headers) == 0, do: "(none)"

  defp format_headers(headers) do
    headers
    |> Enum.map(fn {key, value} -> "#{key}=#{value}" end)
    |> Enum.join(", ")
  end
end
