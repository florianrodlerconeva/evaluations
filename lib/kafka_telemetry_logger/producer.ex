defmodule KafkaTelemetryLogger.Producer do
  @moduledoc """
  A GenStage producer that bridges the push-based KafkaEx consumer into the
  demand-driven Broadway pipeline.

  The KafkaEx `GenConsumer` hands each fetched batch to this producer via
  `deliver/3`, tagging it with a unique `ref`. The producer buffers the
  messages and emits them to Broadway as demand arrives. Each emitted
  `Broadway.Message` carries an acknowledger
  (`KafkaTelemetryLogger.Acknowledger`) that reports success/failure back here.

  Once every message belonging to a `ref` has been acknowledged, the producer
  notifies the original caller with `{:batch_complete, ref, :ok | :error}` so
  the consumer can decide whether to commit the offsets.

  Concurrency is fixed at 1 so there is a single, named producer process that
  the consumer can reach by module name.
  """

  use GenStage

  alias KafkaTelemetryLogger.{Acknowledger, Pipeline}

  @doc """
  Hands a batch of KafkaEx records off to the pipeline.

  `ref` identifies the batch and `caller` is the process to notify with
  `{:batch_complete, ref, status}` once all messages have been acknowledged.

  Broadway wraps and names the producer process itself, so we locate it via
  `Broadway.producer_names/1` (concurrency is 1, so there is exactly one).
  """
  @spec deliver([KafkaEx.Messages.Fetch.Record.t()], reference(), pid()) :: :ok
  def deliver(records, ref, caller) do
    [producer | _] = Broadway.producer_names(Pipeline)
    GenStage.cast(producer, {:deliver, records, ref, caller})
  end

  @impl true
  def init(_opts) do
    {:producer, %{queue: :queue.new(), size: 0, demand: 0, pending: %{}}}
  end

  @impl true
  def handle_cast({:deliver, [], ref, caller}, state) do
    # Nothing to process — complete immediately.
    send(caller, {:batch_complete, ref, :ok})
    {:noreply, [], state}
  end

  def handle_cast({:deliver, records, ref, caller}, state) do
    producer = self()

    messages =
      Enum.map(records, fn record ->
        %Broadway.Message{
          data: record,
          acknowledger: {Acknowledger, producer, %{ref: ref}},
          metadata: %{partition: record.partition, offset: record.offset}
        }
      end)

    pending =
      Map.put(state.pending, ref, %{
        caller: caller,
        remaining: length(records),
        failed: false
      })

    queue = Enum.reduce(messages, state.queue, &:queue.in/2)

    dispatch(%{
      state
      | queue: queue,
        size: state.size + length(messages),
        pending: pending
    })
  end

  @impl true
  def handle_demand(incoming, state) do
    dispatch(%{state | demand: state.demand + incoming})
  end

  @impl true
  def handle_info({:ack, ref, status}, state) do
    {:noreply, [], record_ack(state, ref, status)}
  end

  # Decrement the outstanding count for `ref`; when it reaches zero, tell the
  # caller whether the whole batch succeeded.
  defp record_ack(state, ref, status) do
    case Map.fetch(state.pending, ref) do
      {:ok, %{remaining: remaining, failed: failed, caller: caller} = entry} ->
        remaining = remaining - 1
        failed = failed or status == :error

        if remaining == 0 do
          send(caller, {:batch_complete, ref, if(failed, do: :error, else: :ok)})
          %{state | pending: Map.delete(state.pending, ref)}
        else
          new_entry = %{entry | remaining: remaining, failed: failed}
          %{state | pending: Map.put(state.pending, ref, new_entry)}
        end

      :error ->
        state
    end
  end

  defp dispatch(%{demand: 0} = state), do: {:noreply, [], state}
  defp dispatch(%{size: 0} = state), do: {:noreply, [], state}

  defp dispatch(state) do
    take = min(state.demand, state.size)
    {events, queue} = take_events(state.queue, take, [])

    {:noreply, Enum.reverse(events),
     %{state | queue: queue, size: state.size - take, demand: state.demand - take}}
  end

  defp take_events(queue, 0, acc), do: {acc, queue}

  defp take_events(queue, n, acc) do
    {{:value, event}, queue} = :queue.out(queue)
    take_events(queue, n - 1, [event | acc])
  end
end
