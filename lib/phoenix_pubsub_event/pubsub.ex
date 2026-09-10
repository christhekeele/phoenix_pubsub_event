defmodule PhoenixPubSubEvent.PubSub do
  @moduledoc """
  Codification of conventions around `Phoenix.PubSub`.

  `Phoenix.PubSub` is a flexible, very general-purpose library. The community has
  many conventions around its use, this is a formalization and extension of some of them.

  The functions in `PhoenixPubSubEvent` exist to prevent you from needing to construct
  these topics or events by hand.
  The conventions that module enforces are documented here, but in practice
  just use `PhoenixPubSubEvent`.

  ## Events

  A `t:PhoenixPubSubEvent.t/0` is always a three-tuple with 3 components: `{subject, name, payload}`.

  - The event `subject` is always an atom, a module, or a two-tuple of `{struct_module, struct_id}`.
  - The event `name` is always an atom.
  - The event `payload` is always a map with a unique `t:reference/0` under the `:ref` key.

  This is the structure that will always be sent as messages to subscribed processes.

  If constructing events yourself (instead of using `PhoenixPubSubEvent.publish!/3`),
  you must always use `PhoenixPubSubEvent.new/3`.

  ```elixir
  iex> alias PhoenixPubSubEvent, as: Event
  iex> event = Event.new(:subject, :event_name)
  iex> match? {:subject, :event_name, %{}}, event
  true
  ```

  ```elixir
  iex> alias PhoenixPubSubEvent, as: Event
  iex> event = Event.new({:subject, :id}, :event_name)
  iex> match? {{:subject, :id}, :event_name, %{}}, event
  true
  ```

  ```elixir
  iex> alias PhoenixPubSubEvent, as: Event
  iex> event = Event.new({:subject, :id}, :event_name, foo: :bar)
  iex> match? {{:subject, :id}, :event_name, %{foo: :bar}}, event
  true
  ```

  ## Topics

  A `t:PhoenixPubSubEvent.topic/0` is a binary delimited by colons (`":"`) that takes one of the following forms:

  - `"subject"`
      A subscription that notices all of the events about a `subject`.

  - `"subject:event_name"`
      A subscription that notices particular events happening about a `subject`.

  - `"subject:id"`
      A subscription that notices events for a particular instance of a subject.

  - `"subject:id:event_name"`
      A subscription that just notices events with a particular name for
      a particular instance of a subject.

  These can each be subscribed to individually.

  - Unlike in `Phoenix.PubSub`, subscribing that same process to the exact same topic
  multiple times will not cause events to be received multiple times.

  - However, subscribing to multiple topics overlapping the same subject may cause
    an event to be received multiple times, once for each overlapping subscription.
    Callers should use the event's `payload.ref` to perform event deduplication
    if subscribing to multiple topics about the same subject at different levels.

  See `PhoenixPubSubEvent` for usage examples.
  """

  alias PhoenixPubSubEvent, as: Event

  require Event

  @default_pubsub_server Application.compile_env(:cerebrate, [:event, :pubsub_server], nil)

  @typedoc """
  A map from topics to the result of trying to broadcast an event to them.
  """
  @type topic_result :: %{Event.topic() => :ok | {:error, term()}}

  @doc """
  Returns the topics the caller is currently subscribed to.

  Accepted optional `options` include:

  - `:pubsub_server`: The pubsub server to use for subscriptions.
      If no `:pubsub_server` is given, defaults to the value configured in
      `Application.compile_env(:phoenix_pubsub_event, :pubsub_server)`.

  Fails when provided any other options.

  Returns `{:ok, topics}` or `{:error, reason}`.
  """
  @spec subscriptions(keyword()) :: {:ok, [Event.topic()]} | {:error, term()}
  def subscriptions(options \\ []) do
    {pubsub_server, options} = Keyword.pop(options, :pubsub_server, @default_pubsub_server)

    if Enum.empty?(options) do
      {:ok,
       pubsub_server
       |> process_subscriptions()
       |> MapSet.to_list()}
    else
      {:error, {:invalid_options, options}}
    end
  end

  @doc """
  Assertively returns the topics the caller is currently subscribed to.

  Accepted optional `options` include:

  - `:pubsub_server`: The pubsub server to use for subscriptions.
      If no `:pubsub_server` is given, defaults to the value configured in
      `Application.compile_env(:phoenix_pubsub_event, :pubsub_server)`.

  Returns `topics` or raises `#{inspect(Error)}` if checking subscription status fails.
  """
  @spec subscriptions!(keyword()) :: [Event.topic()] | no_return()
  def subscriptions!(options \\ []) do
    case subscriptions(options) do
      {:ok, topics} ->
        topics

      {:error, reason} ->
        raise Event.Error, message: "could not list subscriptions: #{inspect(reason)}"
    end
  end

  @doc """
  Subscribes the caller to an explicit `t:topic/0`.

  Accepted optional `options` include:

  - `:pubsub_server`: The pubsub server to use for subscriptions.
      If no `:pubsub_server` is given, defaults to the value configured in
      `Application.compile_env(:phoenix_pubsub_event, :pubsub_server)`.

  All other provided `options` are forwarded to `Phoenix.PubSub.subscribe/3`.

  Returns `:ok` or `{:error, reason}` if subscription fails.
  """
  @spec subscribe(Event.topic(), keyword()) :: :ok | {:error, term()}
  def subscribe(topic, options \\ []) when is_binary(topic) and is_list(options) do
    {pubsub_server, options} = Keyword.pop(options, :pubsub_server, @default_pubsub_server)

    already_subscribed? =
      pubsub_server
      |> process_subscriptions()
      |> MapSet.member?(topic)

    if already_subscribed? do
      :ok
    else
      with :ok <- Phoenix.PubSub.subscribe(pubsub_server, topic, options) do
        add_to_process_subscriptions(pubsub_server, topic)
        :ok
      end
    end
  end

  @doc """
  Assertively subscribes the caller to an explicit `t:topic/0`.

  Accepted optional `options` include:

  - `:pubsub_server`: The pubsub server to use for subscriptions.
      If no `:pubsub_server` is given, defaults to the value configured in
      `Application.compile_env(:phoenix_pubsub_event, :pubsub_server)`.

  All other provided `options` are forwarded to `Phoenix.PubSub.subscribe/3`.

  Returns the `topic` or raises `#{inspect(Event.Error)}` if subscription fails.
  """
  @spec subscribe!(Event.topic(), keyword()) :: Event.topic() | no_return()
  def subscribe!(topic, options \\ []) when is_binary(topic) and is_list(options) do
    case subscribe(topic, options) do
      :ok ->
        topic

      {:error, reason} ->
        raise Event.Error,
          message: "could not subscribe to #{inspect(topic)} for reason: #{inspect(reason)}"
    end
  end

  @doc """
  Unsubscribes the caller from an explicit `t:topic/0`.

  Accepted optional `options` include:

  - `:pubsub_server`: The pubsub server to use for subscriptions.
      If no `:pubsub_server` is given, defaults to the value configured in
      `Application.compile_env(:phoenix_pubsub_event, :pubsub_server)`.

  Fails when provided any other options.

  Returns `:ok` or `{:error, reason}` if subscription fails.
  """
  @spec unsubscribe(Event.topic(), keyword()) :: :ok | {:error, term()}
  def unsubscribe(topic, options \\ []) when is_binary(topic) and is_list(options) do
    {pubsub_server, options} = Keyword.pop(options, :pubsub_server, @default_pubsub_server)

    if Enum.empty?(options) do
      with :ok <- Phoenix.PubSub.unsubscribe(pubsub_server, topic) do
        remove_from_process_subscriptions(pubsub_server, topic)
        :ok
      end
    else
      {:error, {:invalid_options, options}}
    end
  end

  @doc """
  Assertively unsubscribes the caller from an explicit `t:topic/0`.

  Accepted optional `options` include:

  - `:pubsub_server`: The pubsub server to use for subscriptions.
      If no `:pubsub_server` is given, defaults to the value configured in
      `Application.compile_env(:phoenix_pubsub_event, :pubsub_server)`.

  Fails when provided any other options.

  Returns the `topic` or raises `#{inspect(Event.Error)}` if subscription fails.
  """
  @spec unsubscribe!(Event.topic(), keyword()) :: Event.topic() | no_return()
  def unsubscribe!(topic, options \\ []) when is_binary(topic) and is_list(options) do
    case unsubscribe(topic, options) do
      :ok ->
        topic

      {:error, reason} ->
        raise Event.Error,
          message: "could not unsubscribe to #{inspect(topic)} for reason: #{inspect(reason)}"
    end
  end

  @doc """
  Broadcasts the given `event` across provided `topics`.

  Accepted optional `options` include:

  - `:pubsub_server`: The pubsub server to use for broadcast.
      If no `:pubsub_server` is given, defaults to the value configured in
      `Application.compile_env(:phoenix_pubsub_event, :pubsub_server)`.

  Fails when provided any other options.

  Returns `:ok` or `{:error, reason}` if broadcast fails.
  """
  @spec broadcast(Event.topic() | [Event.topic()], Event.t(), keyword()) ::
          :ok | {:error, topic_result()}
  def broadcast(topics, event, options \\ [])

  def broadcast(topics, event, options) when (is_binary(topics) or is_list(topics)) and Event.is_event(event) do
    {pubsub_server, options} = Keyword.pop(options, :pubsub_server, @default_pubsub_server)

    if Enum.empty?(options) do
      topics = List.wrap(topics)

      {outcome, result} =
        for topic <- topics, reduce: {:ok, %{}} do
          {outcome, result} ->
            case Phoenix.PubSub.broadcast(pubsub_server, topic, event) do
              :ok -> {outcome, Map.put(result, topic, :ok)}
              {:error, reason} -> {:error, Map.put(result, topic, {:error, reason})}
            end
        end

      case {outcome, result} do
        {:ok, _result} -> :ok
        {:error, result} -> {:error, result}
      end
    else
      {:error, {:invalid_options, options}}
    end
  end

  @doc """
  Assertively broadcasts the given `event` across provided `topics`.

  Accepted optional `options` include:

  - `:pubsub_server`: The pubsub server to use for broadcast.
      If no `:pubsub_server` is given, defaults to the value configured in
      `Application.compile_env(:phoenix_pubsub_event, :pubsub_server)`.

  Fails when provided any other options.

  Returns `topics` or raises `#{inspect(Event.Error)}` if broadcast fails.
  """
  @spec broadcast!(Event.topic() | [Event.topic()], Event.t()) :: [Event.topic()] | no_return()
  def broadcast!(topics, event) when (is_binary(topics) or is_list(topics)) and Event.is_event(event) do
    case broadcast(topics, event) do
      :ok ->
        topics

      {:error, result} ->
        failed_topics =
          result
          |> Enum.filter(&match?({_topic, {:error, _reason}}, &1))
          |> Enum.map(&elem(&1, 0))

        raise Event.Error,
          message: "could not broadcast event #{inspect(event)} to topics #{inspect(failed_topics)}"
    end
  end

  ####
  # Private functions
  ##

  defp process_dict_key(pubsub_server) do
    {__MODULE__, pubsub_server, :subscriptions}
  end

  defp process_subscriptions(pubsub_server) do
    subscriptions =
      pubsub_server
      |> process_dict_key()
      |> Process.get()

    if subscriptions, do: subscriptions, else: MapSet.new()
  end

  defp add_to_process_subscriptions(pubsub_server, topic) do
    subscriptions =
      pubsub_server
      |> process_subscriptions()
      |> MapSet.put(topic)

    pubsub_server
    |> process_dict_key()
    |> Process.put(subscriptions)
  end

  defp remove_from_process_subscriptions(pubsub_server, topic) do
    subscriptions =
      pubsub_server
      |> process_subscriptions()
      |> MapSet.delete(topic)

    pubsub_server
    |> process_dict_key()
    |> Process.put(subscriptions)
  end
end
