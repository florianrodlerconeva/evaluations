defmodule KafkaTelemetryLogger.TargetProducer do
  @moduledoc """
  Stub for publishing processed messages to a *target* Kafka topic.

  This does **not** talk to Kafka — it just logs what it would send and returns
  `:ok`. Swap the body of `publish/1` for a real producer (e.g. a KafkaEx
  produce call) to make it live.

  The result can be overridden at runtime via the
  `:target_publish_result` application env (used by tests to exercise the
  failure path), e.g. `Application.put_env(:kafka_telemetry_logger,
  :target_publish_result, {:error, :unavailable})`.
  """

  require Logger

  @type record :: KafkaEx.Messages.Fetch.Record.t()

  @spec publish([record()]) :: :ok | {:error, term()}
  def publish([]), do: :ok

  def publish(records) do
    case Application.get_env(:kafka_telemetry_logger, :target_publish_result, :ok) do
      :ok ->
        topic = target_topic()

        Enum.each(records, fn record ->
          Logger.debug(fn ->
            "[target-producer stub] -> #{topic} " <>
              "key=#{inspect(record.key)} (source offset #{record.offset})"
          end)
        end)

        Logger.info("[target-producer stub] published #{length(records)} message(s) to #{topic}")
        :ok

      {:error, _reason} = error ->
        error
    end
  end

  defp target_topic do
    Application.get_env(:kafka_telemetry_logger, :target_topic, "device-telemetry-processed")
  end
end
