defmodule KafkaTelemetryLogger.TelemetryConsumer do
  @moduledoc """
  A `KafkaEx.Consumer.GenConsumer` that feeds fetched messages into the Broadway
  pipeline and lets Broadway drive offset commits.

  KafkaEx is push-based and only supports `:async_commit`/`:sync_commit` return
  values (there is no "don't commit"). To commit *only* after messages have been
  fully processed and published to the target topic, `handle_message_set/2`
  hands the batch to `KafkaTelemetryLogger.Producer` and then **blocks** until
  Broadway acknowledges every message in that batch:

    * all messages acked successfully -> `{:sync_commit, state}` (commit)
    * any message failed              -> raise, so nothing is committed and the
      batch is re-fetched and reprocessed (let-it-crash)

  Blocking here also provides natural backpressure: KafkaEx will not fetch the
  next batch until the current one has drained through Broadway.
  """

  use KafkaEx.Consumer.GenConsumer

  require Logger

  alias KafkaTelemetryLogger.Producer

  # How long to wait for Broadway to finish a batch before giving up (and
  # crashing, which triggers a reprocess from the last committed offset).
  @ack_timeout 60_000

  @impl true
  def handle_message_set([], state), do: {:async_commit, state}

  def handle_message_set(message_set, state) do
    ref = make_ref()
    Producer.deliver(message_set, ref, self())

    receive do
      {:batch_complete, ^ref, :ok} ->
        {:sync_commit, state}

      {:batch_complete, ^ref, :error} ->
        raise "Broadway failed to process batch #{inspect(ref)}; " <>
                "not committing offsets so the batch will be reprocessed"
    after
      @ack_timeout ->
        raise "Timed out after #{@ack_timeout}ms waiting for Broadway to " <>
                "process batch #{inspect(ref)}"
    end
  end
end
