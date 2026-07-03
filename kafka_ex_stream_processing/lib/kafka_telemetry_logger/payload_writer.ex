defmodule KafkaTelemetryLogger.PayloadWriter do
  @moduledoc """
  Writes each message's headers and payload to a file and periodically logs
  throughput (messages/second) to the console.

  This keeps the (potentially large) payloads out of the console — the console
  is used only to report what the app is doing and how fast — and makes it easy
  to see whether the pipeline is keeping up when reading a topic from the start.

  A single writer process owns the file handle and serialises writes. The file
  is opened with `:delayed_write` so appends are buffered for throughput; data
  is flushed on a normal shutdown.
  """

  use GenServer

  require Logger

  @doc "Writes one message entry: `%{partition:, offset:, headers:, payload:}`."
  @spec write(map()) :: :ok
  def write(entry), do: GenServer.call(__MODULE__, {:write, entry})

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    path =
      Keyword.get(opts, :path) ||
        Application.get_env(:kafka_telemetry_logger, :payload_file, "telemetry_payloads.log")

    interval = Application.get_env(:kafka_telemetry_logger, :progress_interval_ms, 2_000)

    {:ok, io} = File.open(path, [:write, :delayed_write, :utf8])
    Logger.info("Writing message payloads to #{Path.expand(path)}")

    :timer.send_interval(interval, :report)

    now = mono()
    {:ok, %{io: io, path: path, count: 0, last_count: 0, started: now, last: now}}
  end

  @impl true
  def handle_call({:write, entry}, _from, state) do
    IO.write(state.io, format(entry))
    {:reply, :ok, %{state | count: state.count + 1}}
  end

  @impl true
  def handle_info(:report, state) do
    now = mono()
    delta = state.count - state.last_count
    seconds = (now - state.last) / 1_000

    if delta > 0 do
      rate = delta / max(seconds, 0.001)

      Logger.info(
        "consumed #{state.count} messages (#{:erlang.float_to_binary(rate, decimals: 0)}/s)"
      )
    end

    {:noreply, %{state | last_count: state.count, last: now}}
  end

  @impl true
  def terminate(_reason, state) do
    File.close(state.io)
  end

  defp format(%{partition: partition, offset: offset, headers: headers, payload: payload}) do
    "[partition=#{partition} offset=#{offset}]\theaders=#{format_headers(headers)}\tpayload=#{payload}\n"
  end

  defp format_headers(headers) when map_size(headers) == 0, do: "(none)"

  defp format_headers(headers) do
    headers
    |> Enum.map(fn {key, value} -> "#{key}=#{value}" end)
    |> Enum.join(", ")
  end

  defp mono, do: System.monotonic_time(:millisecond)
end
