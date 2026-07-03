defmodule KafkaTelemetryLogger.Acknowledger do
  @moduledoc """
  Broadway acknowledger that routes per-message ack results back to the
  `KafkaTelemetryLogger.Producer`.

  The producer sets each message's acknowledger to
  `{__MODULE__, producer_pid, %{ref: batch_ref}}`. Broadway calls `ack/3` with
  the `producer_pid` as the ack ref and the successful/failed message lists;
  we forward one `{:ack, batch_ref, :ok | :error}` per message so the producer
  can track when a whole batch is done.
  """

  @behaviour Broadway.Acknowledger

  @impl true
  def ack(producer, successful, failed) do
    Enum.each(successful, &notify(producer, &1, :ok))
    Enum.each(failed, &notify(producer, &1, :error))
    :ok
  end

  defp notify(producer, %Broadway.Message{acknowledger: {_mod, _ack_ref, %{ref: ref}}}, status) do
    send(producer, {:ack, ref, status})
  end
end
