defmodule Sequin.TestSupport.ReplicationSlots do
  @moduledoc """
  The Ecto sandbox interferes with replication slots. Even running replication slot tests in
  async: false mode doesn't solve it. Neither does setting up a different logical database for
  replication slot tests -- it appears creating a replication slot requires a lock that affects
  the whole db instance!

  Replication slot tests can carefully create the replication slot before touching the
  database/Ecto, but it's a footgun.

  To navigate around this, we reset and create the slot once before all tests here.
  """
  import Ecto.Query

  alias Sequin.Consumers
  alias Sequin.Replication.PostgresReplicationSlot
  alias Sequin.Repo
  alias Sequin.Runtime.SlotMessageStore

  @doc """
  To add a replication slot used in a test, you must register it here.
  """
  def replication_slots do
    %{
      Sequin.PostgresReplicationTest => "__postgres_replication_test_slot__",
      SequinWeb.PostgresReplicationControllerTest => "__postgres_rep_controller_test_slot__",
      Sequin.YamlLoaderTest => "__yaml_loader_test_slot__",
      SequinWeb.YamlControllerTest => "__yaml_controller_test_slot__",
      Sequin.Runtime.SlotProducerTest => "__slot_producer_test_slot__",
      Sequin.Runtime.SlotProducer.IntegrationTest => "__slot_producer_integration_test_slot__",
      # Only reads the replication slot
      Sequin.Factory.ReplicationFactory => "__postgres_replication_test_slot__"
    }
  end

  def slot_name(mod), do: Map.fetch!(replication_slots(), mod)

  def reset_slot(repo, slot, attempt \\ 0) do
    case repo.query("SELECT pg_replication_slot_advance($1, pg_current_wal_lsn())::text", [slot]) do
      {:ok, _} ->
        :ok

      {:error, _} ->
        # There is sometimes a race where the slot is still in use by the previous test.
        if attempt < 3 do
          Process.sleep(100)
          reset_slot(repo, slot, attempt + 1)
        else
          # IO.puts("Reset attempted")
          # raise "Failed to reset replication slot #{slot} after #{attempt} attempts: #{inspect(error)}"
          IO.puts("Recreating slot")
          recreate_slot(repo, slot)
          # After recreating, we need to drain messages to move the slot forward.
          # Drain messages synchronously - this will wait for replication to start and then
          # drain all messages, acknowledging them which moves the slot forward
          # Process.sleep(1000)
          # IO.puts("Draining messages")
          # drain_all_messages(slot)
          # :ok
        end
    end
  end

  def recreate_slot(repo, slot) do
    # Step 1: Best-effort drop
    case repo.query("select pg_drop_replication_slot($1)", [slot]) do
      {:ok, _} ->
        :ok

      {:error, %Postgrex.Error{postgres: %{code: :undefined_object}}} ->
        # Slot does not exist — fine
        :ok

      {:error, %Postgrex.Error{postgres: %{code: :object_in_use}}} ->
        # Slot is active — can't drop it safely
        # In tests, this usually means a walsender is still attached
        # We intentionally ignore and try to recreate anyway
        :ok

      {:error, _} ->
        # Any other error — ignore and continue
        :ok
    end

    # Step 2: Create fresh slot
    case repo.query(
           "select pg_create_logical_replication_slot($1, 'pgoutput')::text",
           [slot]
         ) do
      {:ok, _} ->
        :ok

      {:error, %Postgrex.Error{postgres: %{code: :duplicate_object}}} ->
        # Slot already exists (race / concurrent test)
        :ok

      {:error, error} ->
        raise """
        Failed to recreate replication slot #{slot}.

        Error:
        #{inspect(error)}
        """
    end
  end

  @doc """
  Drains all messages from consumers associated with a replication slot by slot_name.
  This is needed when recreating a slot because recreate doesn't advance the LSN,
  so old messages remain in the slot and need to be consumed and acknowledged.

  Acknowledging messages moves the slot forward, effectively clearing old messages.

  This function will wait for SlotMessageStore processes to be available and will
  keep draining until no more messages are available.

  If called before replication starts, it will retry until replication is ready.
  Uses a loop-based approach to avoid stack overflow from deep recursion.

  This is a synchronous (blocking) call that will wait until draining is complete.
  """
  def drain_all_messages(slot_name) do
    # Retry until replication slot and consumers are ready, then drain once
    drain_all_messages_loop(slot_name, 150)
  end

  defp drain_all_messages_loop(slot_name, retries_left) when retries_left > 0 do
    # Find the replication slot by slot_name
    case Repo.one(from(pgr in PostgresReplicationSlot, where: pgr.slot_name == ^slot_name)) do
      nil ->
        # No replication slot found yet, retry later
        Process.sleep(300)
        drain_all_messages_loop(slot_name, retries_left - 1)

      replication_slot ->
        # Get all consumers for this replication slot
        consumers = Consumers.list_consumers_for_replication_slot(replication_slot.id)

        case consumers do
          [] ->
            # No consumers found yet, retry later
            Process.sleep(300)
            drain_all_messages_loop(slot_name, retries_left - 1)

          _ ->
            # Wait for replication to start and process messages, then drain once
            drain_with_retry(consumers, 50)
            :ok
        end
    end
  catch
    # If SlotMessageStore processes aren't running yet, retry
    :exit, {:noproc, _} ->
      Process.sleep(200)
      drain_all_messages_loop(slot_name, retries_left - 1)

    :exit, {:normal, _} ->
      Process.sleep(200)
      drain_all_messages_loop(slot_name, retries_left - 1)
  end

  defp drain_all_messages_loop(_slot_name, 0) do
    # Out of retries - give up
    # (replication may not have started, but that's ok - messages will be processed when it does)
    :ok
  end

  defp drain_with_retry(consumers, retries_left) when retries_left > 0 do
    # First check if SlotMessageStore processes are ready
    all_ready? =
      Enum.all?(consumers, fn consumer ->
        check_slot_message_store_ready(consumer)
      end)

    if all_ready? do
      # Processes are ready, wait a bit for replication to process initial messages
      Process.sleep(1000)
      drain_until_stable(consumers)
    else
      # SlotMessageStore not ready yet, wait and retry
      Process.sleep(100)
      drain_with_retry(consumers, retries_left - 1)
    end
  catch
    :exit, {:noproc, _} ->
      # Processes not ready yet, wait and retry
      Process.sleep(100)
      drain_with_retry(consumers, retries_left - 1)
  end

  defp drain_with_retry(_consumers, 0) do
    # Out of retries, give up silently
    # This is ok - messages will be consumed when replication fully starts
    :ok
  end

  defp check_slot_message_store_ready(consumer) do
    SlotMessageStore.peek_messages(consumer, 1)
    true
  catch
    :exit, {:noproc, _} -> false
    :exit, {:normal, _} -> false
  end

  defp drain_until_stable(consumers, stable_rounds \\ 0)

  defp drain_until_stable(consumers, stable_rounds) when stable_rounds < 5 do
    # Drain all consumers
    total_drained =
      Enum.reduce(consumers, 0, fn consumer, acc ->
        case drain_consumer_messages_once(consumer) do
          {:ok, count} -> acc + count
          {:error, _} -> acc
        end
      end)

    # Check message count to ensure we've drained everything
    total_remaining =
      Enum.reduce(consumers, 0, fn consumer, acc ->
        case SlotMessageStore.count_messages(consumer) do
          {:ok, count} -> acc + count
          {:error, _} -> acc
        end
      end)

    if total_drained > 0 or total_remaining > 0 do
      # Messages were drained or still exist, wait a bit and continue draining
      Process.sleep(500)
      drain_until_stable(consumers, 0)
    else
      # No messages and count confirms empty, wait a bit to ensure stability
      Process.sleep(500)
      drain_until_stable(consumers, stable_rounds + 1)
    end
  catch
    :exit, {:noproc, _} ->
      :ok
  end

  defp drain_until_stable(_consumers, _stable_rounds) do
    # No messages for multiple rounds, we're done
    :ok
  end

  defp drain_consumer_messages_once(consumer) do
    # Preload postgres_database if needed
    consumer = Repo.preload(consumer, :postgres_database)

    # Use peek_messages to get ALL messages (including non-deliverable ones)
    # produce() only returns deliverable messages, but we need to drain everything
    # peek_messages returns a list directly, not {:ok, list}
    case SlotMessageStore.peek_messages(consumer, 1000) do
      [] ->
        {:ok, 0}

      messages when is_list(messages) and length(messages) > 0 ->
        # Extract ack_ids and acknowledge all messages to remove them from the store
        ack_ids = Enum.map(messages, & &1.ack_id)

        case SlotMessageStore.messages_succeeded_returning_messages(consumer, ack_ids) do
          {:ok, _} -> {:ok, length(messages)}
          {:error, _} -> {:ok, 0}
        end

      {:error, _} ->
        {:ok, 0}
    end
  catch
    :exit, {:noproc, _} ->
      {:error, :noproc}

    :exit, {:normal, _} ->
      {:ok, 0}
  end

  @doc """
  Run this before ExUnit.start/0. Because replication slots and sandboxes don't play nicely, we
  want to create the slots just once before we run the test suite.
  """
  def setup_all do
    replication_slots()
    |> Map.values()
    |> Enum.each(fn slot_name ->
      case Repo.query("select pg_drop_replication_slot($1)", [slot_name]) do
        {:ok, _} -> :ok
        {:error, %Postgrex.Error{postgres: %{code: :undefined_object}}} -> :ok
      end

      Repo.query!("select pg_create_logical_replication_slot($1, 'pgoutput')::text", [slot_name])
    end)
  end
end
