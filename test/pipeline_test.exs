defmodule KafkaTelemetryLogger.PipelineTest do
  @moduledoc """
  Exercises the Broadway pipeline end-to-end without a live Kafka broker:
  a batch delivered to the producer must flow through the processors and the
  batcher (which publishes to the target-topic stub) and only then report
  completion back to the caller — this is what gates the source-offset commit.
  """
  use ExUnit.Case

  import ExUnit.CaptureLog

  alias KafkaEx.Messages.Fetch.Record
  alias KafkaEx.Messages.Header
  alias KafkaTelemetryLogger.{Pipeline, Producer}

  setup do
    start_supervised!(Pipeline)
    :ok
  end

  defp record(offset, opts \\ []) do
    %Record{
      topic: "telemetry",
      partition: 0,
      offset: offset,
      key: Keyword.get(opts, :key, "device-#{offset}"),
      value: Keyword.get(opts, :value, ~s({"offset":#{offset}})),
      headers: Keyword.get(opts, :headers, [%Header{key: "message_type", value: "data_point"}])
    }
  end

  test "completes with :ok once the batch is processed and published" do
    ref = make_ref()
    Producer.deliver([record(1), record(2)], ref, self())

    assert_receive {:batch_complete, ^ref, :ok}, 5_000
  end

  test "empty batches complete immediately" do
    ref = make_ref()
    Producer.deliver([], ref, self())

    assert_receive {:batch_complete, ^ref, :ok}, 1_000
  end

  test "completes with :error when the target publish fails (offsets not committed)" do
    Application.put_env(:kafka_telemetry_logger, :target_publish_result, {:error, :unavailable})
    on_exit(fn -> Application.delete_env(:kafka_telemetry_logger, :target_publish_result) end)

    ref = make_ref()

    log =
      capture_log(fn ->
        Producer.deliver([record(3), record(4)], ref, self())
        assert_receive {:batch_complete, ^ref, :error}, 5_000
      end)

    assert log =~ "Failed to publish batch to target topic"
  end

  test "concurrent batches are tracked independently by ref" do
    ref_a = make_ref()
    ref_b = make_ref()

    Producer.deliver([record(10), record(11)], ref_a, self())
    Producer.deliver([record(20)], ref_b, self())

    assert_receive {:batch_complete, ^ref_a, :ok}, 5_000
    assert_receive {:batch_complete, ^ref_b, :ok}, 5_000
  end
end
