defmodule PhoenixPubSubEvent.Source do
  @moduledoc """
  High-level helpers for subscribing to and publishing Phoenix PubSub events.

  An event source `t:PhoenixPubSubEvent.source/0` is anything that can be turned
  into topics and a subject: that is, something that can be subscribed to
  and published as the subject of an event.

  This library is designed around the common case of subscribing to and receiving events
  from structs with `id` fields. However, atoms, modules, and tuples can also be used
  as event sources.

  The core workflow is to use event sources to:

  - In one process:
    - Subscribe to an event source with `subscribe!/2`
      - Which uses the result of `subscription/2` as the topic
    - `receive/1` a message matching an event given by `new/3`
    - Unsubscribe when no longer intersted in an event source with `unsubscribe!/2`
  - In another process:
    - Publish events for an event source with `publish!/3`
      - Which uses the result of `topics/1` to broadcast to
      - And the result of `event/3` to construct the event

  The quickest way to get started with this library is to experiment with what functions
  return when given different event sources:

  - `subscription!/1`
      See what topic is used to subscribe to a source
  - `subscription!/2`
      See what topic is used to subscribe to a source and event name
  - `topics!/1`
      See what topics are broadcast to when a source publishes any event
  - `topics!/2`
      See what topics are broadcast to when a source publishes a specific event
  - `event/2`
      See what an event for a source looks like when published.

  ### Atoms

  Atoms can be used as subjects to construct topics for arbitrary events:

  ```elixir
  iex> alias PhoenixPubSubEvent.Source, as: EventSource
  iex>
  iex> EventSource.subscription!(:subject)
  "subject"
  iex> EventSource.subscription!(:subject, :event_name)
  "subject:event_name"
  iex> EventSource.topics!(:subject)
  ["subject"]
  iex> EventSource.topics!(:subject, :event_name)
  ["subject", "subject:event_name"]
  iex> match? {:subject, :event_name, %{}}, EventSource.event(:subject, :event_name)
  true
  ```

  An example workflow with atom sources might be:

  ```elixir
  alias PhoenixPubSubEvent.Source, as: EventSource

  # First process
  EventSource.subscribe(:subject, :event_acknowledged)
  EventSource.publish(:subject, :event)

  # Second process
  EventSource.subscribe(:subject, :event)
  receive do
    {:subject, :event, _} ->
      EventSource.publish(:subject, :event_acknowledged,
        pid: self(),
        time: DateTime.utc_now()
      )
  end

  # Second process
  receive do
    {:subject, :event_acknowledged, %{pid: client, time: time}} ->
      # Do something with client ack
  end
  ```

  ### Modules

  As modules are atoms, they can be used as sources:

  ```elixir
  iex> # Assuming: defmodule Entity, do: nil
  iex> alias PhoenixPubSubEvent.Source, as: EventSource
  iex>
  iex> EventSource.subscription!(Entity)
  "Elixir.Entity"
  iex> EventSource.subscription!(Entity, :event_name)
  "Elixir.Entity:event_name"
  iex> EventSource.topics!(Entity)
  ["Elixir.Entity"]
  iex> EventSource.topics!(Entity, :event_name)
  ["Elixir.Entity", "Elixir.Entity:event_name"]
  iex> match? {Entity, :event_name, %{}}, EventSource.event(Entity, :event_name)
  true
  ```

  An example workflow with module sources might be:

  ```elixir
  # Assuming: defmodule Entity, do: nil
  alias PhoenixPubSubEvent.Source, as: EventSource

  # First process
  EventSource.subscribe(Entity, :event_acknowledged)
  EventSource.publish(Entity, :event)

  # Second process
  EventSource.subscribe(Entity, :event)
  receive do
    {Entity, :event, _} ->
      EventSource.publish(Entity, :event_acknowledged,
        pid: self(),
        time: DateTime.utc_now()
      )
  end

  # Second process
  receive do
    {Entity, :event_acknowledged, %{pid: client, time: time}} ->
      # Do something with client ack
  end
  ```

  ### Structs with IDs

  Structs with `:id` fields produce the finest-grained subscriptions:

  ```elixir
  iex> # Assuming: defmodule Entity, do: defstruct [:id, :name]
  iex> alias PhoenixPubSubEvent.Source, as: EventSource
  iex> entity = %Entity{id: "entity_id", name: "entity_name"}
  iex>
  iex> EventSource.subscription!(entity)
  "Elixir.Entity:entity_id"
  iex> EventSource.subscription!(entity, :event_name)
  "Elixir.Entity:entity_id:event_name"
  iex> EventSource.topics!(entity)
  ["Elixir.Entity", "Elixir.Entity:entity_id"]
  iex> EventSource.topics!(entity, :event_name)
  ["Elixir.Entity", "Elixir.Entity:entity_id", "Elixir.Entity:event_name", "Elixir.Entity:entity_id:event_name"]
  iex> match? {{Entity, "entity_id"}, :event_name, %{}}, EventSource.event(entity, :event_name)
  true
  ```

  You'll notice that when we publish an event about a struct, the subject of our event
  is a two-tuple: `{Entity, "entity_id"}`. This means that in order to receive events
  about specific instances, we must match on a two-tuple subject
  (rather than the full struct itself).

  An example workflow with struct sources might be:

  ```elixir
  # Assuming: defmodule Entity, do: defstruct [:id, :name]
  alias PhoenixPubSubEvent.Source, as: EventSource

  # First process
  entity = %Entity{id: "entity_id", name: "entity_name"}
  entity_id = entity.id
  entity
  |> EventSource.subscribe!(:update_acknowledged)
  |> EventSource.publish!(:updated)

  # Second process
  EventSource.subscribe(Entity, :updated)
  receive do
    {{Entity, entity_id}, :updated, _} ->
      entity = %Entity{id: entity_id} # Or otherwise load entity from id
      EventSource.publish(entity, :update_acknowledged,
        pid: self(),
        time: DateTime.utc_now()
      )
  end

  # Second process
  receive do
    {{Entity, ^entity_id}, :update_acknowledged, %{pid: client, time: time}} ->
      # Do something with client ack
  end
  ```

  ### Two-Tuples

  The two-tuple subject emitted for a struct can itself be used as an event source.

  This makes it simpler for one caller to publish events about a struct, and a second caller
  to receive events about it and emit events related to it, without needing
  the entire original struct available to send responses.

  For example, the code below behaves identically when called with either
  the `entity` struct or the `subject` two-tuple:

  ```elixir
  iex> # Assuming: defmodule Entity, do: defstruct [:id, :name]
  iex> alias PhoenixPubSubEvent.Source, as: EventSource
  iex> entity = %Entity{id: "entity_id", name: "entity_name"}
  iex> subject = {Entity, entity.id}
  iex>
  iex> EventSource.subscription!(subject)
  "Elixir.Entity:entity_id"
  iex> EventSource.subscription!(subject, :event_name)
  "Elixir.Entity:entity_id:event_name"
  iex> EventSource.topics!(subject)
  ["Elixir.Entity", "Elixir.Entity:entity_id"]
  iex> EventSource.topics!(subject, :event_name)
  ["Elixir.Entity", "Elixir.Entity:entity_id", "Elixir.Entity:event_name", "Elixir.Entity:entity_id:event_name"]
  iex> match? {{Entity, "entity_id"}, :event_name, %{}}, EventSource.event(subject, :event_name)
  true
  ```

  This can be used to communicate about structs without loading them from ex. a database:

  ```elixir
  # Assuming: defmodule Entity, do: defstruct [:id, :name]
  alias PhoenixPubSubEvent.Source, as: EventSource

  # Server process
  # has a full Entity struct available
  entity = %Entity{id: "entity_id, name: "entity_name"}
  entity_id = entity.id
  entity
  |> EventSource.subscribe!(:update_acknowledged)
  |> EventSource.publish(:updated)

  # Client process
  # doesn't need a full struct to communicate
  EventSource.subscribe(Entity, :updated)
  receive do
    {{Entity, entity_id}, :updated, _} ->
      # Can just publish to the two-tuple instead
      EventSource.publish({Entity, entity_id}, :update_acknowledged,
        pid: self(),
        time: DateTime.utc_now()
      )
  end

  # Server process
  receive do
    {{Entity, ^entity_id}, :update_acknowledged, %{pid: client, time: time}} ->
      # Do something with client ack and full entity
      Entity.update_client_ack_time(entity, client, time)
  end
  ```

  """
  alias PhoenixPubSubEvent, as: Event
  alias PhoenixPubSubEvent.Error, as: Error
  alias PhoenixPubSubEvent.Source, as: Source

  @doc """
  Constructs a new `t:PhoenixPubSub.event/0` from an event `source` and `name`.

  Accepts an optional `payload` and co-erces it into a `t:map/0`.
  """
  @spec event(Event.source(), Event.name(), Event.payload()) :: Event.event()
  def event(source, name, payload \\ %{}) when is_atom(name) do
    subject = Source.Subject.for(source)

    payload =
      payload
      |> Map.new()
      |> Map.put_new_lazy(:ref, &make_ref/0)

    {subject, name, payload}
  end

  @doc """
  Returns the primary topic to subscribe to an event event `source` and optional `name`.

  Returns `{:ok, subscription}` or `{:error, reason}` if no subscription topic exists.
  """
  @spec subscription(Event.source(), Event.name()) :: {:ok, Event.topic()} | {:error, term()}
  def subscription(source, name \\ nil) when is_atom(name) do
    if subscription = Source.Subscription.for(source) do
      {:ok, if(name, do: "#{subscription}:#{name}", else: subscription)}
    else
      {:error, {:no_subscription, source}}
    end
  end

  @doc """
  Returns the primary topic to subscribe to an event `source` and optional `name`.

  Returns a `subscription` or raises `#{inspect(Error)}` if no subscription topic exists.
  """
  @spec subscription!(Event.source(), Event.name()) :: Event.topic() | no_return()
  def subscription!(source, name \\ nil) when is_atom(name) do
    case subscription(source, name) do
      {:ok, topic} ->
        topic

      {:error, {:no_subscription, ^source}} ->
        raise Error, message: "could not build subscription from #{inspect(source)}"
    end
  end

  @doc """
  Returns the topics to broadcast for an event `source` and optional `name`.

  Returns `{:ok, topics}` or `{:error, reason}` if no topics exist.
  """
  @spec topics(Event.source(), Event.name()) :: {:ok, [Event.topic()]} | {:error, term()}
  def topics(source, name \\ nil) when is_atom(name) do
    topics =
      Source.Topics.for(source)
      |> List.insert_at(-1, Source.Subscription.for(source))
      |> Enum.reject(&is_nil/1)
      |> Enum.uniq()

    if not Enum.empty?(topics) do
      topics =
        if name do
          topics ++ Enum.map(topics, &"#{&1}:#{name}")
        else
          topics
        end

      {:ok, topics}
    else
      {:error, {:no_topics, source}}
    end
  end

  @doc """
  Returns the topics to broadcast for an event `source` and optional `name`.

  Returns `topics` or raises `#{inspect(Error)}` if no topics exist.
  """
  @spec topics!(Event.source(), Event.name()) :: [Event.topic()] | no_return()
  def topics!(source, name \\ nil) when is_atom(name) do
    case topics(source, name) do
      {:ok, topics} ->
        topics

      {:error, {:no_topics, ^source}} ->
        raise Error, message: "could not build topics from #{inspect(source)}"
    end
  end

  @doc """
  Subscribes the caller to the provided event `source` and optional `name`.

  Accepted optional `options` include:

  - `:pubsub_server`: The pubsub server to use for subscriptions.
      If no `:pubsub_server` is given, defaults to the value configured in
      `Application.compile_env(:phoenix_pubsub_event, :pubsub_server)`.

  All other provided `options` are forwarded to `Phoenix.PubSub.subscribe/3`.

  Returns `{:ok, topic}` or `{:error, topic, reason}` if subscription fails.
  """
  @spec subscribe(Event.source(), Event.name(), keyword()) ::
          {:ok, Event.topic()} | {:error, Event.topic(), term()}
  def subscribe(source, name \\ nil, options \\ []) when is_atom(name) do
    case subscription(source, name) do
      {:ok, topic} ->
        topic
        |> Event.subscribe(options)
        |> case do
          :ok -> {:ok, topic}
          {:error, reason} -> {:error, topic, reason}
        end

      {:error, reason} ->
        {:error, nil, reason}
    end
  end

  @doc """
  Assertively subscribes the caller to the provided event `source` and optional `name`.

  Accepted optional `options` include:

  - `:pubsub_server`: The pubsub server to use for subscriptions.
      If no `:pubsub_server` is given, defaults to the value configured in
      `Application.compile_env(:phoenix_pubsub_event, :pubsub_server)`.

  All other provided `options` are forwarded to `Phoenix.PubSub.subscribe/3`.

  Returns the event `source` or raises `#{inspect(Error)}` if subscription fails.
  """
  @spec subscribe!(Event.source(), Event.name(), keyword()) :: Event.source() | no_return()
  def subscribe!(source, name \\ nil, options \\ []) when is_atom(name) do
    case subscribe(source, name, options) do
      {:ok, _topic} ->
        source

      {:error, topic, reason} ->
        raise Error,
          message: "could not subscribe to #{inspect(topic)} for reason: #{inspect(reason)}"
    end
  end

  @doc """
  Unsubscribes the caller from the provided event `source` and optional `name`.

  Accepted optional `options` include:

  - `:pubsub_server`: The pubsub server to use for subscriptions.
      If no `:pubsub_server` is given, defaults to the value configured in
      `Application.compile_env(:phoenix_pubsub_event, :pubsub_server)`.

  Fails when provided any other options.

  Returns `{:ok, topic}` or `{:error, topic, reason}` if subscription fails.
  """
  @spec unsubscribe(Event.source(), Event.name(), keyword()) ::
          {:ok, Event.topic()} | {:error, Event.topic(), term()}
  def unsubscribe(source, name \\ nil, options \\ []) when is_atom(name) do
    case subscription(source, name) do
      {:ok, topic} ->
        topic
        |> Event.unsubscribe(options)
        |> case do
          :ok -> {:ok, topic}
          {:error, reason} -> {:error, topic, reason}
        end

      {:error, reason} ->
        {:error, nil, reason}
    end
  end

  @doc """
  Assertively unsubscribes the caller from the provided event `source` and optional `name`.

  Accepted optional `options` include:

  - `:pubsub_server`: The pubsub server to use for subscriptions.
      If no `:pubsub_server` is given, defaults to the value configured in
      `Application.compile_env(:phoenix_pubsub_event, :pubsub_server)`.

  Fails when provided any other options.

  Returns the event `source` or raises `#{inspect(Error)}` if subscription fails.
  """
  @spec unsubscribe!(Event.source(), Event.name(), keyword()) :: Event.source() | no_return()
  def unsubscribe!(source, name \\ nil, options \\ []) when is_atom(name) do
    case unsubscribe(source, name, options) do
      {:ok, _topic} ->
        source

      {:error, topic, reason} ->
        raise Error,
          message: "could not unsubscribe to #{inspect(topic)} for reason: #{inspect(reason)}"
    end
  end

  @doc """
  Publishes an event constructed from event `source` and `name` with optional `payload`.

  Accepted optional `options` include:

  - `:pubsub_server`: The pubsub server to use for subscriptions.
      If no `:pubsub_server` is given, defaults to the value configured in
      `Application.compile_env(:phoenix_pubsub_event, :pubsub_server)`.

  Fails when provided any other options.

  Returns `{:ok, topics, event}` or `{:error, topics, event, reason}` if publishing fails.
  """
  @spec publish(Event.source(), Event.name(), Event.payload(), keyword()) ::
          {:ok, [Event.topic()], Event.event()}
          | {:error, [Event.topic()], Event.event(), term()}
  def publish(source, name, payload \\ %{}, options \\ []) when is_atom(name) do
    case topics(source) do
      {:ok, topics} ->
        case broadcast(topics, source, name, payload, options) do
          {:ok, event} -> {:ok, topics, event}
          {:error, event, result} -> {:error, topics, event, {:failed_broadcasts, result}}
        end

      {:error, {:no_topics, ^source}} ->
        {:error, [], event(source, name, payload), {:no_topics, source}}
    end
  end

  @doc """
  Assertively publishes an event constructed from event `source` and `name` with optional `payload`.

  Accepted optional `options` include:

  - `:pubsub_server`: The pubsub server to use for subscriptions.
      If no `:pubsub_server` is given, defaults to the value configured in
      `Application.compile_env(:phoenix_pubsub_event, :pubsub_server)`.

  Fails when provided any other options.

  Returns the event `source` or raises `#{inspect(Error)}` if publishing fails.
  """
  @spec publish!(Event.source(), Event.name(), Event.payload(), keyword()) ::
          Event.source() | no_return()
  def publish!(source, name, payload \\ %{}, options \\ []) when is_atom(name) do
    case publish(source, name, payload, options) do
      {:ok, _topics, _event} ->
        source

      {:error, _topics, event, result} ->
        failed_topics =
          result
          |> Enum.filter(&match?({_topic, {:error, _reason}}, &1))
          |> Enum.map(&elem(&1, 0))

        raise Error,
          message:
            "could not broadcast event #{inspect(event)} to topics #{inspect(failed_topics)}"
    end
  end

  @doc """
  Broadcasts an event derived from event event `source`, `name`, and optional `payload`
  across provided `topics`.

  Used for publishing events to explict topics. Generally, `publish/3` should be preferred
  to automatically generate the topics instead.

  Accepted optional `options` include:

  - `:pubsub_server`: The pubsub server to use for subscriptions.
      If no `:pubsub_server` is given, defaults to the value configured in
      `Application.compile_env(:phoenix_pubsub_event, :pubsub_server)`.

  Fails when provided any other options.

  Returns `{:ok, event}` or `{:error, event, reason}` if broadcast fails.
  """
  @spec broadcast(
          Event.topic() | [Event.topic()],
          Event.source(),
          Event.name(),
          Event.payload(),
          keyword()
        ) :: {:ok, Event.event()} | {:error, Event.event(), Event.topic_result()}
  def broadcast(topics, source, name, payload \\ %{}, options \\ [])

  def broadcast(topic, source, name, payload, options) when is_binary(topic) and is_atom(name) do
    broadcast([topic], source, name, payload, options)
  end

  def broadcast(topics, source, name, payload, options) do
    event = event(source, name, payload)

    case Event.broadcast(topics, event, options) do
      :ok -> {:ok, event}
      {:error, result} -> {:error, event, result}
    end
  end

  @doc """
  Assertively broadcasts an event derived from event `source`, `name`, and optional `payload`
  across provided `topics`.

  Used for publishing events to explict topics. Generally, `publish/3` should be preferred
  to automatically generate the topics instead.

  Accepted optional `options` include:

  - `:pubsub_server`: The pubsub server to use for subscriptions.
      If no `:pubsub_server` is given, defaults to the value configured in
      `Application.compile_env(:phoenix_pubsub_event, :pubsub_server)`.

  Fails when provided any other options.

  Returns `topics` or raises `#{inspect(Error)}` if broadcast fails.
  """
  @spec broadcast!(Event.topic() | [Event.topic()], Event.source(), Event.name(), Event.payload()) ::
          Event.topic() | [Event.topic()] | no_return()
  def broadcast!(topics, source, name, payload \\ %{}, options \\ []) do
    case broadcast(topics, source, name, payload, options) do
      {:ok, _event} ->
        topics

      {:error, event, result} ->
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
