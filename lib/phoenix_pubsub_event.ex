defmodule PhoenixPubSubEvent do
  @moduledoc """
  A data-driven event system built on conventions around `Phoenix.PubSub`.

  `Phoenix.PubSub` is a flexible, very general-purpose library. The community has
  many conventions around its use, this is a formalization and extension of some of them.

  The goal of this library is to make it easy to design and implement event-oriented contracts
  between publisher and consumer with little coordination, and make it consistent to reason about
  what subscriptions, events, and message structures look like.

  The functions in `PhoenixPubSubEvent.Source` help you never need to construct a topic or event
  by hand, and the event format/message structure is simple and consistent.

  This documentation concerns itself with the conventions it establishes around `Phoenix.PubSub`,
  for documentation on the main APIs and usage of this library, see `PhoenixPubSubEvent.Source`.

  ## Events

  An `t:event/0` is always a three-tuple with 3 components: `{subject, name, payload}`.

  - The event `subject` is always an atom, a module, or a two-tuple of `{struct_module, struct_id}`.
  - The event `name` is always an atom.
  - The event `payload` is always a map with a unique `t:reference/0` under the `:ref` key.

  This is the structure that will always be sent as messages to subscribed processes.

  If constructing events yourself (instead of using `PhoenixPubSubEvent.Source.publish/2`),
  you must always use `PhoenixPubSubEvent.Source.event/2`.

  ```elixir
  iex> alias PhoenixPubSubEvent.Source, as: EventSource
  iex> event = EventSource.event(:subject, :event_name)
  iex> match? {:subject, :event_name, %{}}, event
  true
  ```

  ```elixir
  iex> alias PhoenixPubSubEvent.Source, as: EventSource
  iex> event = EventSource.event({:subject, :id}, :event_name)
  iex> match? {{:subject, :id}, :event_name, %{}}, event
  true
  ```

  ```elixir
  iex> alias PhoenixPubSubEvent.Source, as: EventSource
  iex> event = EventSource.event({:subject, :id}, :event_name, foo: :bar)
  iex> match? {{:subject, :id}, :event_name, %{foo: :bar}}, event
  true
  ```

  ## Topics

  A `t:topic/0` is a binary delimited by colons (`":"`) that takes one of the following forms:

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

  - Unlike in `Phoenix.PubSub`, subscribing thet same process to the exact same topic
  multiple times will not cause events to be received multiple times.

  - However, subscribing to multiple topics overlapping the same subject may cause
    an event to be received multiple times, once for each overlapping subscription.
    Callers should use the event's `payload.ref` to perform event deduplication
    if subscribing to multiple topics about the same subject at different levels.

  See `PhoenixPubSubEvent.Source` for usage examples.
  """

  alias PhoenixPubSubEvent.Error, as: Error
  alias PhoenixPubSubEvent.Source, as: Source

  @typedoc """
  Things this library considers to be an `id` in a struct we can use to build topics.
  """
  @type id :: atom() | binary() | integer() | pid() | reference()

  @typedoc """
  A topic that can be subscribed to, published to, and unsubscribed from.
  """
  @type topic :: Phoenix.PubSub.topic()

  @typedoc """
  The subject of an event three-tuple, always the first element.
  """
  @type subject :: atom() | module() | {atom(), id()} | tuple() | term()

  @typedoc """
  The name of an event three-tuple, always the second element.
  """
  @type name :: atom()

  @typedoc """
  The payload of an event three-tuple, always the third element.
  """
  @type payload :: map() | Enumerable.t({any(), any()})

  @typedoc """
  A three-tuple event that can be sent or received as a message.
  """
  @type event :: {subject(), name(), payload()}

  @typedoc """
  A source for event topics and subjects.
  """
  @type source :: atom() | module() | {module(), id()} | struct() | Source.Subject.t()

  @typedoc """
  A map from topics to the result of trying to broadcast an event to them.
  """
  @type topic_result :: %{topic() => :ok | {:error, term()}}

  @default_pubsub_server Application.compile_env(:phoenix_pubsub_event, :pubsub_server, nil)

  @doc section: :guards
  @doc """
  Tests if `term` is something this library recognizes as an `t:id/0`.

  Allowed in guard tests. Inlined by the compiler.
  """
  defguard is_id(term)
           when is_atom(term) or is_binary(term) or is_integer(term) or is_pid(term) or
                  is_reference(term)

  @doc section: :guards
  @doc """
  Tests if `term` is something this library recognizes as an `t:event/0`.

  Allowed in guard tests. Inlined by the compiler.
  """
  defguard is_event(term) when is_tuple(term) and tuple_size(term) == 3 and is_atom(elem(term, 1))

  @doc """
  Subscribes the caller to an explicit `t:topic/0`.

  Accepted optional `options` include:

  - `:pubsub_server`: The pubsub server to use for subscriptions.
      If no `:pubsub_server` is given, defaults to the value configured in
      `Application.compile_env(:phoenix_pubsub_event, :pubsub_server)`.

  All other provided `options` are forwarded to `Phoenix.PubSub.subscribe/3`.

  Returns `:ok` or `{:error, reason}` if subscription fails.
  """
  @spec subscribe(topic(), keyword()) :: :ok | {:error, term()}
  def subscribe(topic, options \\ []) when is_binary(topic) and is_list(options) do
    {pubsub_server, options} = Keyword.pop(options, :pubsub_server, @default_pubsub_server)
    Phoenix.PubSub.subscribe(pubsub_server, topic, options)
  end

  @doc """
  Assertively subscribes the caller to an explicit `t:topic/0`.

  Accepted optional `options` include:

  - `:pubsub_server`: The pubsub server to use for subscriptions.
      If no `:pubsub_server` is given, defaults to the value configured in
      `Application.compile_env(:phoenix_pubsub_event, :pubsub_server)`.

  All other provided `options` are forwarded to `Phoenix.PubSub.subscribe/3`.

  Returns the `topic` or raises `#{inspect(Error)}` if subscription fails.
  """
  @spec subscribe!(topic(), keyword()) :: topic() | no_return()
  def subscribe!(topic, options \\ []) when is_binary(topic) and is_list(options) do
    case subscribe(topic, options) do
      :ok ->
        topic

      {:error, reason} ->
        raise Error,
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
  @spec unsubscribe(topic(), keyword()) :: :ok | {:error, term()}
  def unsubscribe(topic, options \\ []) when is_binary(topic) and is_list(options) do
    {pubsub_server, options} = Keyword.pop(options, :pubsub_server, @default_pubsub_server)

    if not Enum.empty?(options) do
      Phoenix.PubSub.unsubscribe(pubsub_server, topic)
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

  Returns the `topic` or raises `#{inspect(Error)}` if subscription fails.
  """
  @spec unsubscribe!(topic(), keyword()) :: topic() | no_return()
  def unsubscribe!(topic, options \\ []) when is_binary(topic) and is_list(options) do
    case unsubscribe(topic, options) do
      :ok ->
        topic

      {:error, reason} ->
        raise Error,
          message: "could not unsubscribe to #{inspect(topic)} for reason: #{inspect(reason)}"
    end
  end

  @doc """
  Broadcasts the given `event` across provided `topics`.

  Accepted optional `options` include:

  - `:pubsub_server`: The pubsub server to use for subscriptions.
      If no `:pubsub_server` is given, defaults to the value configured in
      `Application.compile_env(:phoenix_pubsub_event, :pubsub_server)`.

  Fails when provided any other options.

  Returns `:ok` or `{:error, reason}` if broadcast fails.
  """
  @spec broadcast(topic() | [topic()], event(), keyword()) :: :ok | {:error, topic_result()}
  def broadcast(topics, event, options \\ [])

  def broadcast(topic, event, options) when is_binary(topic) and is_event(event) do
    broadcast([topic], event, options)
  end

  def broadcast(topics, event, options) when is_list(topics) and is_event(event) do
    {pubsub_server, options} = Keyword.pop(options, :pubsub_server, @default_pubsub_server)
    topics = List.wrap(topics)

    if not Enum.empty?(options) do
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

  - `:pubsub_server`: The pubsub server to use for subscriptions.
      If no `:pubsub_server` is given, defaults to the value configured in
      `Application.compile_env(:phoenix_pubsub_event, :pubsub_server)`.

  Fails when provided any other options.

  Returns `topics` or raises `#{inspect(Error)}` if broadcast fails.
  """
  @spec broadcast!(topic() | [topic()], event()) :: [topic()] | no_return()
  def broadcast!(topics, event) when is_event(event) do
    case broadcast(topics, event) do
      :ok ->
        topics

      {:error, result} ->
        failed_topics =
          result
          |> Enum.filter(&match?({_topic, {:error, _reason}}, &1))
          |> Enum.map(&elem(&1, 0))

        raise Error,
          message:
            "could not broadcast event #{inspect(event)} to topics #{inspect(failed_topics)}"
    end
  end
end


defmodule Entity, do: defstruct([:id, :name, :status])
alias PhoenixPubSubEvent, as: Event

# defimpl Event.Source.Subject, for: Entity do
#   def for(entity) do
#     {Entity, entity.id, entity.status}
#   end
# end

# defimpl Event.Source.Subscription, for: Entity do
#   def for(entity) do
#     "#{Entity}:#{entity.id}:#{entity.status}"
#   end
# end

defimpl Event.Source.Topics, for: Entity do
  def for(entity) do
    [
      "#{Entity}",
      "#{Entity}:#{entity.id}",
      "#{Entity}:#{entity.id}:#{entity.status}"
    ]
  end
end
