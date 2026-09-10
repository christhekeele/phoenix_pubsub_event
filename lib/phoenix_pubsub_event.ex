defmodule PhoenixPubSubEvent do
  @moduledoc """
  High-level helpers for subscribing to and publishing `Phoenix.PubSub` events.

  The goals of this system are to:

  - make it easy to design and implement event-oriented contracts
    between publisher and consumer with little coordination
  - subscribe to and publish events from identical inputs
  - auto-generate topics and events from domain entities in a predictable fashion

  An event `t:source/0` is anything that can be turned into topics and a subject:
  that is, something that can be both subscribed to and published as the subject of an event.

  This library is designed around the common case of subscribing to and receiving events
  from structs with `id` fields. However, atoms, modules, and tuples can also be used
  as event sources.

  ### Usage

  The core workflow is to use event sources to:

  - In one process:
    - `publish!/3` events for an event source
      - Which uses the result of `topics/1` to broadcast to
      - And the result of `new/3` to construct the event
  - In another process:
    - `subscribe!/2` to an event source
      - Which uses the result of `subscription/2` as the topic
    - `receive/1` a message matching an event given by `new/3`
    - `unsubscribe!/2` when no longer interested in an event source

  An event source can be:

  - A struct with an id, ex:
    - `subscribe(%Entity{id: "id"})`
    - `publish(%Entity{id: "id"}, :event)`
  - A two-tuple of struct and id, ex:
    - `subscribe({Entity, "id"})`
    - `publish({Entity, "id"}, :event)`
  - A struct or module, ex:
    - `subscribe(Entity)`
    - `publish(Entity, :event)`
  - An arbitraty atom, ex:
    - `subscribe(:subject)`
    - `publish(:subject, :event)`

  ### Event Sources

  The easiest way to understand what `subscribe!/2` and `publish!/3` do for you
  is to experiment with what these functions return when given different event sources:

  - `subscription!/1`
      See what topic is used to subscribe to a source on `subscribe!/2`.
  - `subscription!/2`
      See what topic is used to subscribe to a source and event name on `subscribe!/3`.
  - `topics!/1`
      See what topics are broadcast to when a source publishes any event via `publish!/3`.
  - `topics!/2`
      See what topics are broadcast to when a source publishes a specific event via `publish!/3`.
  - `new/2`
      See what an event for a source looks like when published via `publish!/3`.

  #### Atoms

  Atoms can be used as subjects to construct topics for arbitrary events:

  ```elixir
  iex> alias PhoenixPubSubEvent, as: Event
  iex>
  iex> Event.subscription!(:subject)
  "subject"
  iex> Event.subscription!(:subject, :event_name)
  "subject:event_name"
  iex> Event.topics!(:subject)
  ["subject"]
  iex> Event.topics!(:subject, :event_name)
  ["subject", "subject:event_name"]
  iex> match? {:subject, :event_name, %{}}, Event.new(:subject, :event_name)
  true
  ```

  An example workflow with atom sources might be:

  ```elixir
  alias PhoenixPubSubEvent, as: Event

  # First process
  Event.subscribe(:subject, :event_acknowledged)
  Event.publish(:subject, :event)

  # Second process
  Event.subscribe(:subject, :event)
  receive do
    {:subject, :event, _} ->
      Event.publish(:subject, :event_acknowledged,
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

  #### Modules

  As modules are atoms, they can be used as sources:

  ```elixir
  iex> # Assuming: defmodule Entity, do: nil
  iex> alias PhoenixPubSubEvent, as: Event
  iex>
  iex> Event.subscription!(Entity)
  "Elixir.Entity"
  iex> Event.subscription!(Entity, :event_name)
  "Elixir.Entity:event_name"
  iex> Event.topics!(Entity)
  ["Elixir.Entity"]
  iex> Event.topics!(Entity, :event_name)
  ["Elixir.Entity", "Elixir.Entity:event_name"]
  iex> match? {Entity, :event_name, %{}}, Event.new(Entity, :event_name)
  true
  ```

  An example workflow with module sources might be:

  ```elixir
  # Assuming: defmodule Entity, do: nil
  alias PhoenixPubSubEvent, as: Event

  # First process
  Event.subscribe(Entity, :event_acknowledged)
  Event.publish(Entity, :event)

  # Second process
  Event.subscribe(Entity, :event)
  receive do
    {Entity, :event, _} ->
      Event.publish(Entity, :event_acknowledged,
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

  #### Structs with IDs

  Structs with `:id` fields produce the finest-grained subscriptions:

  ```elixir
  iex> # Assuming: defmodule Entity, do: defstruct [:id, :name]
  iex> alias PhoenixPubSubEvent, as: Event
  iex> entity = %Entity{id: "entity_id", name: "entity_name"}
  iex>
  iex> Event.subscription!(entity)
  "Elixir.Entity:entity_id"
  iex> Event.subscription!(entity, :event_name)
  "Elixir.Entity:entity_id:event_name"
  iex> Event.topics!(entity)
  ["Elixir.Entity", "Elixir.Entity:entity_id"]
  iex> Event.topics!(entity, :event_name)
  ["Elixir.Entity", "Elixir.Entity:entity_id", "Elixir.Entity:event_name", "Elixir.Entity:entity_id:event_name"]
  iex> match? {{Entity, "entity_id"}, :event_name, %{}}, Event.new(entity, :event_name)
  true
  ```

  You'll notice that when we publish an event about a struct, the subject of our event
  is a two-tuple: `{Entity, "entity_id"}`. This means that in order to receive events
  about specific instances, we must match on a two-tuple subject
  (rather than the full struct itself).

  An example workflow with struct sources might be:

  ```elixir
  # Assuming: defmodule Entity, do: defstruct [:id, :name]
  alias PhoenixPubSubEvent, as: Event

  # First process
  entity = %Entity{id: "entity_id", name: "entity_name"}
  entity_id = entity.id
  entity
  |> Event.subscribe!(:update_acknowledged)
  |> Event.publish!(:updated)

  # Second process
  Event.subscribe(Entity, :updated)
  receive do
    {{Entity, entity_id}, :updated, _} ->
      entity = %Entity{id: entity_id} # Or otherwise load entity from id
      Event.publish(entity, :update_acknowledged,
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

  #### Two-Tuples

  The two-tuple subject emitted for a struct can itself be used as an event source.

  This makes it simpler for one caller to publish events about a struct, and a second caller
  to receive events about it and emit events related to it, without needing
  the entire original struct available to send responses.

  For example, the code below behaves identically when called with either
  the `entity` struct or the `subject` two-tuple:

  ```elixir
  iex> # Assuming: defmodule Entity, do: defstruct [:id, :name]
  iex> alias PhoenixPubSubEvent, as: Event
  iex> entity = %Entity{id: "entity_id", name: "entity_name"}
  iex> subject = {Entity, entity.id}
  iex>
  iex> Event.subscription!(subject)
  "Elixir.Entity:entity_id"
  iex> Event.subscription!(subject, :event_name)
  "Elixir.Entity:entity_id:event_name"
  iex> Event.topics!(subject)
  ["Elixir.Entity", "Elixir.Entity:entity_id"]
  iex> Event.topics!(subject, :event_name)
  ["Elixir.Entity", "Elixir.Entity:entity_id", "Elixir.Entity:event_name", "Elixir.Entity:entity_id:event_name"]
  iex> match? {{Entity, "entity_id"}, :event_name, %{}}, Event.new(subject, :event_name)
  true
  ```

  This can be used to communicate about structs without loading them from ex. a database:

  ```elixir
  # Assuming: defmodule Entity, do: defstruct [:id, :name]
  alias PhoenixPubSubEvent, as: Event

  # Server process
  # has a full Entity struct available
  entity = %Entity{id: "entity_id", name: "entity_name"}
  entity_id = entity.id
  entity
  |> Event.subscribe!(:update_acknowledged)
  |> Event.publish(:updated)

  # Client process
  # doesn't need a full struct to communicate
  Event.subscribe(Entity, :updated)
  receive do
    {{Entity, entity_id}, :updated, _} ->
      # Can just publish to the two-tuple instead
      Event.publish({Entity, entity_id}, :update_acknowledged,
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

  alias PhoenixPubSubEvent.Error
  alias PhoenixPubSubEvent.PubSub
  alias PhoenixPubSubEvent.Subject
  alias PhoenixPubSubEvent.Subscription
  alias PhoenixPubSubEvent.Topics

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
  The payload passed to an event constructor, must be coercable to a map.

  Always becomes a map in the third element of an event three-tuple.

  The payload will have keys added to it before it is received, so do not
  supply a struct as the toplevel payload directly; instead, nest structs
  under keys to send them in payloads.
  """
  @type payload :: map() | Enumerable.t({Map.key(), Map.value()})

  @typedoc """
  A three-tuple event that can be sent or received as a message.
  """
  @type t :: {subject(), name(), %{required(:ref) => reference()}}

  @typedoc """
  An three-tuple event about a particular subject.
  """
  @type t(subject) :: {subject, name(), %{required(:ref) => reference()}}

  @typedoc """
  A source for event topics and subjects.
  """
  @type source :: atom() | module() | {module(), id()} | struct() | Subject.t()

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
  Tests if `term` is something this library recognizes as an event `t:t/0`.

  Allowed in guard tests. Inlined by the compiler.
  """
  defguard is_event(term)
           when is_tuple(term) and tuple_size(term) == 3 and is_atom(elem(term, 1)) and is_map(elem(term, 2))

  @doc section: :guards
  @doc """
  Tests if `term` is an event `t:t/0` for `subject`.

  Allowed in guard tests. Inlined by the compiler.
  """
  defguard is_event_for(term, subject)
           when is_event(term) and (elem(term, 0) == subject or elem(elem(term, 0), 0) == subject)

  @doc """
  Constructs a new `t:PhoenixPubSubEvent.t/0` from an event `source` and `name`.

  Accepts an optional `payload` and co-erces it into a `t:map/0`.
  """
  @spec new(source(), name(), payload()) :: t()
  def new(source, name, payload \\ %{}) when is_atom(name) do
    subject = Subject.for(source)

    payload =
      payload
      |> Map.new()
      |> Map.put_new_lazy(:ref, &make_ref/0)

    {subject, name, payload}
  end

  @doc """
  Returns the topics the caller is currently subscribed to, optionally filtered by `source`.

  Accepted optional `options` include:

  - `:pubsub_server`: The pubsub server to use for subscriptions.
      If no `:pubsub_server` is given, defaults to the value configured in
      `Application.compile_env(:phoenix_pubsub_event, :pubsub_server)`.

  Fails when provided any other options.

  Returns `{:ok, topics}` or `{:error, reason}`.
  """
  @spec subscriptions(source(), keyword()) :: {:ok, [topic()]} | {:error, term()}
  def subscriptions(source \\ nil, options \\ []) do
    case PubSub.subscriptions(options) do
      {:ok, topics} ->
        {:ok,
         topics
         |> Enum.filter(fn subscribed ->
           subscription = Subscription.for(source)

           if source && subscription do
             String.starts_with?(subscribed, subscription)
           else
             true
           end
         end)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Assertively returns the topics the caller is currently subscribed to, optionally filtered by `source`.

  Accepted optional `options` include:

  - `:pubsub_server`: The pubsub server to use for subscriptions.
      If no `:pubsub_server` is given, defaults to the value configured in
      `Application.compile_env(:phoenix_pubsub_event, :pubsub_server)`.

  Returns `topics` or raises `#{inspect(Error)}` if checking subscription status fails.
  """
  @spec subscriptions!(source(), keyword()) :: [topic()] | no_return()
  def subscriptions!(source \\ nil, options \\ []) do
    case subscriptions(source, options) do
      {:ok, topics} ->
        topics

      {:error, reason} ->
        raise Error, message: "could not list subscriptions: #{inspect(reason)}"
    end
  end

  @doc """
  Returns the primary topic to subscribe to an event `source` and optional event `name`.

  Returns `{:ok, subscription}` or `{:error, reason}` if no subscription topic exists.
  """
  @spec subscription(source(), name()) :: {:ok, topic()} | {:error, term()}
  def subscription(source, name \\ nil) when is_atom(name) do
    if subscription = Subscription.for(source) do
      {:ok, if(name, do: "#{subscription}:#{name}", else: subscription)}
    else
      {:error, {:no_subscription, source}}
    end
  end

  @doc """
  Returns the primary topic to subscribe to an event `source` and optional event `name`.

  Returns a `t:topic/0` or raises `#{inspect(Error)}` if no subscription topic exists.
  """
  @spec subscription!(source(), name()) :: topic() | no_return()
  def subscription!(source, name \\ nil) when is_atom(name) do
    case subscription(source, name) do
      {:ok, topic} ->
        topic

      {:error, {:no_subscription, ^source}} ->
        raise Error, message: "could not build subscription from #{inspect(source)}"
    end
  end

  @doc """
  Returns the topics to broadcast for an event `source` and optional event `name`.

  Returns `{:ok, topics}` or `{:error, {:no_topics, source}}` if no topics exist.

  Multiple `source`s and event `name`s can be provided as lists: topics for the
  cross product of source/event combinations will be returned. Combinations
  without topics will be ignored instead of returning an error.
  """
  @spec topics(source() | [source()], name() | [name()]) :: {:ok, [topic()]} | {:error, term()}
  def topics(source, name \\ nil)

  # Handle batch case
  def topics(sources, names) when is_list(sources) or is_list(names) do
    {outcome, topics, errors} =
      for source <- List.wrap(sources), name <- List.wrap(names), reduce: {:ok, [], []} do
        {outcome, topics, errors} ->
          case topics(source, name) do
            {:ok, result} -> {outcome, topics ++ result, errors}
            {:error, {:no_topics, ^source}} -> {outcome, topics, errors}
            {:error, error} -> {:error, topics, [error | errors]}
          end
      end

    case outcome do
      :ok -> {:ok, topics}
      :error -> {:error, errors}
    end
  end

  # Handle single case
  def topics(source, name) when not is_list(source) and is_atom(name) do
    topics =
      Topics.for(source)
      # Ensure the subscription topic for a source
      # is always the last entry in the topics list
      |> List.insert_at(-1, Subscription.for(source))
      |> Enum.reject(&is_nil/1)
      |> Enum.uniq()

    if Enum.any?(topics) do
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
  Returns the topics to broadcast for an event `source` and optional event `name`.

  Returns `topics` or raises `#{inspect(Error)}` if no topics exist.

  Multiple `source`s and event `name`s can be provided as lists: topics for the
  cross product of source/event combinations will be returned. Combinations
  without topics will be ignored instead of raising an error.
  """
  @spec topics!(source() | [source()], name() | [name()]) :: [topic()] | no_return()
  def topics!(source, name \\ nil) when is_atom(name) do
    case topics(source, name) do
      {:ok, topics} ->
        topics

      {:error, {:no_topics, ^source}} ->
        raise Error, message: "no topics or subscription for source: #{inspect(source)}"

      {:error, errors} when is_list(errors) ->
        raise Error,
          message: "could not generate topics with event name #{inspect(name)} for source: #{inspect(source)}"
    end
  end

  @doc """
  Subscribes the caller to the provided event `source` and optional event `name`.

  Accepted optional `options` include:

  - `:pubsub_server`: The pubsub server to use for subscriptions.
      If no `:pubsub_server` is given, defaults to the value configured in
      `Application.compile_env(:phoenix_pubsub_event, :pubsub_server)`.

  All other provided `options` are forwarded to `Phoenix.PubSub.subscribe/3`.

  Returns `{:ok, topic}` or `{:error, {topic, reason}}` if subscription fails.

  Multiple `source`s and event `name`s can be provided as lists to batch subscribe.
  If provided, returns an ok/error tuple where the second element is a map of
  `{source(), name()}` keys to ok/error tuples per source/event combination.
  """
  @spec subscribe(source() | [source()], name() | [name()], keyword()) ::
          {:ok, topic()}
          | {:error, {topic(), term()}}
          | {:ok | :error, %{{source(), name()} => {:ok, topic()} | {:error, {topic(), term()}}}}

  def subscribe(source, name \\ nil, options \\ [])

  # Handle batch case
  def subscribe(sources, names, options) when is_list(sources) or is_list(names) do
    for source <- List.wrap(sources), name <- List.wrap(names), reduce: {:ok, %{}} do
      {outcome, acc} ->
        {outcome, result} =
          case subscribe(source, name, options) do
            {:ok, _} = result -> {outcome, result}
            {:error, _} = result -> {:error, result}
          end

        {outcome, Map.put(acc, {source, name}, result)}
    end
  end

  # Handle single case
  def subscribe(source, name, options) when not is_list(source) and is_atom(name) and is_list(options) do
    case subscription(source, name) do
      {:ok, topic} ->
        topic
        |> PubSub.subscribe(options)
        |> case do
          :ok -> {:ok, topic}
          {:error, reason} -> {:error, {topic, reason}}
        end

      {:error, reason} ->
        {:error, {nil, reason}}
    end
  end

  @doc """
  Assertively subscribes the caller to the provided event `source` and optional event `name`.

  Accepted optional `options` include:

  - `:pubsub_server`: The pubsub server to use for subscriptions.
      If no `:pubsub_server` is given, defaults to the value configured in
      `Application.compile_env(:phoenix_pubsub_event, :pubsub_server)`.

  All other provided `options` are forwarded to `Phoenix.PubSub.subscribe/3`.

  Returns the event `source` or raises `#{inspect(Error)}` if subscription fails.

  Multiple `source`s and event `name`s can be provided as lists to batch subscribe.
  """
  @spec subscribe!(source() | [source()], name() | [name()], keyword()) :: source() | no_return()
  def subscribe!(source, name \\ nil, options \\ [])

  def subscribe!(source, name, options) do
    case subscribe(source, name, options) do
      {:ok, _topic} ->
        source

      {:error, {topic, reason}} ->
        raise Error,
          message: "could not subscribe to #{inspect(topic)} for reason: #{inspect(reason)}"

      {:error, %{} = results} ->
        failures = Map.filter(results, &match?({_, {:error, _}}, &1))

        raise Error,
          message:
            "could not batch subscribe to all topics with event name #{inspect(name)} for source: #{inspect(source)}\n" <>
              "failures: #{failures}"
    end
  end

  @doc """
  Unsubscribes the caller from the provided event `source` and optional event `name`.

  Accepted optional `options` include:

  - `:pubsub_server`: The pubsub server to use for subscriptions.
      If no `:pubsub_server` is given, defaults to the value configured in
      `Application.compile_env(:phoenix_pubsub_event, :pubsub_server)`.

  Fails when provided any other options.

  Returns `{:ok, topic}` or `{:error, {topic, reason}}` if unsubscription fails.

  Multiple `source`s and event `name`s can be provided as lists to batch unsubscribe.
  If provided, returns an ok/error tuple where the second element is a map of
  `{source(), name()}` keys to ok/error tuples per source/event combination.
  """
  @spec unsubscribe(source() | [source()], name() | [name()], keyword()) ::
          {:ok, topic()}
          | {:error, {topic(), term()}}
          | {:ok | :error, %{{source(), name()} => {:ok, topic()} | {:error, {topic(), term()}}}}

  def unsubscribe(source, name \\ nil, options \\ [])

  # Handle batch case
  def unsubscribe(sources, names, options) when is_list(sources) or is_list(names) do
    for source <- List.wrap(sources), name <- List.wrap(names), reduce: {:ok, %{}} do
      {outcome, acc} ->
        {outcome, result} =
          case unsubscribe(source, name, options) do
            {:ok, _} = result -> {outcome, result}
            {:error, _} = result -> {:error, result}
          end

        {outcome, Map.put(acc, {source, name}, result)}
    end
  end

  # Handle single case
  def unsubscribe(source, name, options) when is_atom(name) and is_list(options) do
    case subscription(source, name) do
      {:ok, topic} ->
        topic
        |> PubSub.unsubscribe(options)
        |> case do
          :ok -> {:ok, topic}
          {:error, reason} -> {:error, {topic, reason}}
        end

      {:error, reason} ->
        {:error, {nil, reason}}
    end
  end

  @doc """
  Assertively unsubscribes the caller from the provided event `source` and optional event `name`.

  Accepted optional `options` include:

  - `:pubsub_server`: The pubsub server to use for subscriptions.
      If no `:pubsub_server` is given, defaults to the value configured in
      `Application.compile_env(:phoenix_pubsub_event, :pubsub_server)`.

  Fails when provided any other options.

  Returns the event `source` or raises `#{inspect(Error)}` if subscription fails.

  Multiple `source`s and event `name`s can be provided as lists to batch unsubscribe.
  """
  @spec unsubscribe!(source() | [source()], name() | [name()], keyword()) :: source() | no_return()
  def unsubscribe!(source, name \\ nil, options \\ []) when is_atom(name) do
    case unsubscribe(source, name, options) do
      {:ok, _topic} ->
        source

      {:error, {topic, reason}} ->
        raise Error,
          message: "could not unsubscribe from #{inspect(topic)} for reason: #{inspect(reason)}"

      {:error, %{}} ->
        raise Error,
          message:
            "could not batch unsubscribe from all topics with event name #{inspect(name)} for source: #{inspect(source)}"
    end
  end

  @doc """
  Publishes an event constructed from event `source` and `name` with optional `payload`.

  Accepted optional `options` include:

  - `:pubsub_server`: The pubsub server to use for publication.
      If no `:pubsub_server` is given, defaults to the value configured in
      `Application.compile_env(:phoenix_pubsub_event, :pubsub_server)`.

  Fails when provided any other options.

  Returns `{:ok, {topics, event}}` or `{:error, {topics, event, reason}}` if publishing fails.

  Multiple `source`s and event `name`s can be provided as lists: events for the
  cross product of source/event combinations will be published. Combinations
  without topics will be ignored instead of returning an error.
  If provided, returns an ok/error tuple where the second element is a map of
  `{source(), name()}` keys to ok/error tuples per source/event combination.
  """
  @spec publish(source() | [source()], name() | [name()], payload(), keyword()) ::
          {:ok, {[topic()], t()}}
          | {:error, {[topic()], t(), term()}}
          | {:ok | :error,
             %{
               {source(), name()} =>
                 {:ok, {[topic()], t()}}
                 | {:error, {[topic()], t(), term()}}
             }}
  def publish(source, name, payload \\ %{}, options \\ [])

  # Handle batch case
  def publish(sources, names, payload, options) when is_list(sources) or is_list(names) do
    for source <- List.wrap(sources), name <- List.wrap(names), reduce: {:ok, %{}} do
      {outcome, acc} ->
        {outcome, result} =
          case publish(source, name, payload, options) do
            {:ok, _} = result -> {outcome, result}
            {:error, _} = result -> {:error, result}
          end

        {outcome, Map.put(acc, {source, name}, result)}
    end
  end

  # Handle single case
  def publish(source, name, payload, options)
      when not is_list(source) and is_atom(name) and is_map(payload) and is_list(options) do
    case topics(source, name) do
      {:ok, topics} ->
        case broadcast(topics, source, name, payload, options) do
          {:ok, event} -> {:ok, {topics, event}}
          {:error, {event, result}} -> {:error, {topics, event, {:failed_broadcasts, result}}}
        end

      {:error, {:no_topics, ^source}} ->
        {:error, {[], new(source, name, payload), {:no_topics, source}}}
    end
  end

  @doc """
  Assertively publishes an event constructed from event `source` and `name` with optional `payload`.

  Accepted optional `options` include:

  - `:pubsub_server`: The pubsub server to use for publication.
      If no `:pubsub_server` is given, defaults to the value configured in
      `Application.compile_env(:phoenix_pubsub_event, :pubsub_server)`.

  Fails when provided any other options.

  Returns the event `source` or raises `#{inspect(Error)}` if publishing fails.

  Multiple `source`s and event `name`s can be provided as lists: events for the
  cross product of source/event combinations will be published. Combinations
  without topics will be ignored instead of raising an error.
  """
  @spec publish!(source(), name(), payload(), keyword()) ::
          source() | no_return()
  def publish!(source, name, payload \\ %{}, options \\ []) when is_atom(name) do
    case publish(source, name, payload, options) do
      {:ok, {_topics, _event}} ->
        source

      {:error, {_topics, event, {:failed_broadcasts, result}}} ->
        failed_topics =
          result
          |> Enum.filter(&match?({_topic, {:error, _reason}}, &1))
          |> Enum.map(&elem(&1, 0))

        raise Error,
          message: "could not broadcast event #{inspect(event)} to topics #{inspect(failed_topics)}"

      {:error, {_topics, _event, {:no_topics, source}}} ->
        raise Error, message: "no topics or subscription for source: #{inspect(source)}"

      {:error, %{} = results} ->
        failures = Map.filter(results, &match?({_, {:error, _}}, &1))

        raise Error,
          message:
            "could not batch publish events named #{inspect(name)} for sources: #{inspect(source)}\n" <>
              "failures: #{failures}"
    end
  end

  @doc """
  Broadcasts an event derived from event `source`, `name`, and optional `payload`
  across provided `topics`.

  Used for publishing events to explict topics. Generally, `publish/3` should be preferred
  to automatically generate the topics instead.

  Accepted optional `options` include:

  - `:pubsub_server`: The pubsub server to use for broadcast.
      If no `:pubsub_server` is given, defaults to the value configured in
      `Application.compile_env(:phoenix_pubsub_event, :pubsub_server)`.

  Fails when provided any other options.

  Returns `{:ok, event}` or `{:error, event, reason}` if broadcast fails.
  """
  @spec broadcast(
          topic() | [topic()],
          source(),
          name(),
          payload,
          keyword()
        ) :: {:ok, t()} | {:error, {t(), PubSub.topic_result()}}
  def broadcast(topics, source, name, payload \\ %{}, options \\ [])

  def broadcast(topic, source, name, payload, options) when is_binary(topic) and is_atom(name) do
    broadcast([topic], source, name, payload, options)
  end

  def broadcast(topics, source, name, payload, options) do
    event = new(source, name, payload)

    case PubSub.broadcast(topics, event, options) do
      :ok -> {:ok, event}
      {:error, result} -> {:error, {event, result}}
    end
  end

  @doc """
  Assertively broadcasts an event derived from event `source`, `name`, and optional `payload`
  across provided `topics`.

  Used for publishing events to explict topics. Generally, `publish/3` should be preferred
  to automatically generate the topics instead.

  Accepted optional `options` include:

  - `:pubsub_server`: The pubsub server to use for broadcast.
      If no `:pubsub_server` is given, defaults to the value configured in
      `Application.compile_env(:phoenix_pubsub_event, :pubsub_server)`.

  Fails when provided any other options.

  Returns `topics` or raises `#{inspect(Error)}` if broadcast fails.
  """
  @spec broadcast!(topic() | [topic()], source(), name(), payload) ::
          topic() | [topic()] | no_return()
  def broadcast!(topics, source, name, payload \\ %{}, options \\ []) do
    case broadcast(topics, source, name, payload, options) do
      {:ok, _event} ->
        topics

      {:error, {event, result}} ->
        failed_topics =
          result
          |> Enum.filter(&match?({_topic, {:error, _reason}}, &1))
          |> Enum.map(&elem(&1, 0))

        raise Error,
          message: "could not broadcast event #{inspect(event)} to topics #{inspect(failed_topics)}"
    end
  end
end
