defmodule KafkaTelemetryLogger.DecoderTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Messages.Header
  alias KafkaTelemetryLogger.Decoder

  describe "decode_value/1" do
    test "returns nil for nil" do
      assert Decoder.decode_value(nil) == nil
    end

    test "returns UTF-8 text as-is" do
      assert Decoder.decode_value(~s({"deviceId":"abc"})) == ~s({"deviceId":"abc"})
    end

    test "base64-encodes non-UTF-8 bytes" do
      raw = <<0xFF, 0xFE, 0x00>>
      assert Decoder.decode_value(raw) == Base.encode64(raw)
    end
  end

  describe "decode_header_value/1" do
    test "plain UTF-8 header" do
      assert Decoder.decode_header_value("device-telemetry") == "device-telemetry"
    end

    test "Azure IoT Hub binary string envelope (marker + length + utf8)" do
      # 0xA5 marker, length 5, "tenant"[0..4] -> "tenan"
      payload = "hello"
      envelope = <<0xA5, byte_size(payload)>> <> payload
      assert Decoder.decode_header_value(envelope) == "hello"
    end

    test "IoT Hub envelope with truncated data falls back to base64" do
      # marker says 10 bytes but only 3 present
      envelope = <<0xA5, 10, "abc">>
      assert Decoder.decode_header_value(envelope) == Base.encode64(envelope)
    end

    test "non-envelope binary (e.g. 0x83 timestamp marker) falls back to base64" do
      raw = <<0x83, 0x00, 0x01, 0x02>>
      assert Decoder.decode_header_value(raw) == Base.encode64(raw)
    end

    test "nil header value" do
      assert Decoder.decode_header_value(nil) == nil
    end
  end

  describe "decode_headers/1" do
    test "nil -> empty map" do
      assert Decoder.decode_headers(nil) == %{}
    end

    test "decodes a list of Header structs into a map" do
      headers = [
        %Header{key: "message_type", value: "telemetry"},
        %Header{key: "tenant", value: <<0xA3, 3, "abc">>},
        %Header{key: "iothub-enqueuedtime", value: <<0x83, 0xAA, 0xBB>>}
      ]

      assert Decoder.decode_headers(headers) == %{
               "message_type" => "telemetry",
               "tenant" => "abc",
               "iothub-enqueuedtime" => Base.encode64(<<0x83, 0xAA, 0xBB>>)
             }
    end
  end
end
