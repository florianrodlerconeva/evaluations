defmodule KafkaTelemetryLogger.Decoder do
  @moduledoc """
  Decodes Kafka record values and headers into human-readable text.

  This mirrors the decoding strategy used by the marimo notebook
  `ops/display-kafka-device-telemetry-topic.py` (which in turn mirrors the
  the-pond `kafka_ingestion` logic).
  """

  alias KafkaEx.Messages.Header

  @doc """
  Decodes a record value (payload) to text.

  Returns the UTF-8 string when the bytes are valid UTF-8, otherwise a base64
  representation so the value is always safe to log.
  """
  @spec decode_value(binary() | nil) :: String.t() | nil
  def decode_value(nil), do: nil

  def decode_value(raw) when is_binary(raw) do
    if String.valid?(raw), do: raw, else: Base.encode64(raw)
  end

  @doc """
  Decodes a list of `KafkaEx.Messages.Header` structs into a plain map of
  `String.t()` keys to decoded string values.
  """
  @spec decode_headers([Header.t()] | nil) :: %{optional(String.t()) => String.t()}
  def decode_headers(nil), do: %{}

  def decode_headers(headers) when is_list(headers) do
    Map.new(headers, fn %Header{key: key, value: value} ->
      {key, decode_header_value(value)}
    end)
  end

  @doc """
  Decodes a single header value.

  Strategy, in order:

    1. UTF-8 — covers the common case.
    2. Azure IoT Hub binary string envelope — IoT Hub encodes string header
       values as: marker byte (0xA0-0xBF) + uint8 length + UTF-8 data.
    3. Base64 fallback — a JSON/log-safe representation for any remaining
       binary values (e.g. the `iothub-enqueuedtime` timestamp, 0x83 marker).
  """
  @spec decode_header_value(binary() | nil) :: String.t() | nil
  def decode_header_value(nil), do: nil

  def decode_header_value(value) when is_binary(value) do
    cond do
      String.valid?(value) -> value
      true -> decode_iothub_string(value)
    end
  end

  # IoT Hub string envelope: <<marker, length, utf8_data::binary-size(length), _rest>>
  defp decode_iothub_string(<<marker, length, rest::binary>> = value)
       when marker in 0xA0..0xBF do
    case rest do
      <<str::binary-size(length), _::binary>> ->
        if String.valid?(str), do: str, else: Base.encode64(value)

      _ ->
        Base.encode64(value)
    end
  end

  defp decode_iothub_string(value), do: Base.encode64(value)
end
