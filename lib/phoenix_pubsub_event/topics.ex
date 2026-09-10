alias PhoenixPubSubEvent, as: Event

require Event

defprotocol PhoenixPubSubEvent.Topics do
  @moduledoc """
  Protocol for generating event topics to publish to from arbitrary values.

  If you want to customize what topics events for your struct publish to,
  you can implement this protocol to return an arbitrary binary.

  For example, if you wanted a struct `status` field to be part
  of the topics you publish to, you might implement this protocol as such:

  ```elixir
  defmodule Entity do
    defstruct [:id, :name, :status]
  end

  alias PhoenixPubSubEvent, as: Event

  defimpl Event.Topics, for: Entity do
    def for(entity) do
      [
        "\#{Entity}",
        "\#{Entity}:\#{entity.id}",
        "\#{Entity}:\#{entity.id}:\#{entity.status}"
      ]
    end
  end

  entity = %Entity{id: "entity_id", name: "entity_name", status: "entity_status"}

  Event.topics!(entity)
  #=> ["Elixir.Entity", "Elixir.Entity:entity_id", "Elixir.Entity:entity_id:entity_status"]
  ```

  When defining custom topics for your struct, you probably want to ensure that
  the topics for the struct and the subject align.
  If you instead extend the subject from `{module, id}` to match the topics you want,
  this will happen organically.

  ```elixir
  defmodule Entity do
    defstruct [:id, :name, :status]
  end

  alias PhoenixPubSubEvent, as: Event

  defimpl Event.Subject, for: Entity do
    def for(entity) do
      {Entity, entity.id, entity.status}
    end
  end

  entity = %Entity{id: "entity_id", name: "entity_name", status: "entity_status"}

  entity |> Event.topics!()
  #=> "Elixir.Entity:entity_id:entity_status"
  entity |> Event.Subject.for() |> Event.topics!()
  #=> "Elixir.Entity:entity_id:entity_status"
  ```

  """

  alias PhoenixPubSubEvent, as: Event

  @fallback_to_any true

  @doc """
  Generates a list of topics for a given `value`.
  """
  @spec for(t) :: [Event.topic()]
  def for(value)
end

# Atoms can become topics via stringification.
defimpl Event.Topics, for: Atom do
  @spec for(atom()) :: [Event.topic()]
  def for(atom) do
    [Atom.to_string(atom)]
  end
end

# Bitstrings can become topics if they are binaries.
defimpl Event.Topics, for: BitString do
  # Binaries (bitstring size 8) are already topics
  @spec for(binary()) :: [Event.topic()]
  def for(binary) when is_binary(binary) do
    [binary]
  end

  # bitstrings with size other than 8 error
  @spec for(bitstring()) :: no_return()
  def for(other) do
    raise Event.Error, message: "cannot convert to topics: #{inspect(other)}"
  end
end

# Tuples become a hierarchy of increasingly specific topics.
defimpl Event.Topics, for: Tuple do
  @spec for(tuple()) :: [Event.topic()]
  def for(tuple) do
    tuple
    |> Tuple.to_list()
    |> Enum.reject(&is_nil/1)
    |> Enum.map(&topic_component_to_string/1)
    |> topics_hierarchy()
    |> Enum.map(&Enum.join(&1, ":"))
  end

  defp topic_component_to_string(value)

  # Atoms can be a component of a topic
  defp topic_component_to_string(atom) when is_atom(atom) do
    Atom.to_string(atom)
  end

  # Arbitrary strings can be a component of a topic
  defp topic_component_to_string(binary) when is_binary(binary) do
    binary
  end

  # IDs can be integers, which can be a component of a topic
  defp topic_component_to_string(integer) when is_integer(integer) do
    inspect(integer)
  end

  # IDs can be pids, which can be a component of a topic
  defp topic_component_to_string(pid) when is_pid(pid) do
    inspect(pid)
  end

  # IDs can be references, which can be a component of a topic
  defp topic_component_to_string(reference) when is_reference(reference) do
    inspect(reference)
  end

  # Anything else should raise
  defp topic_component_to_string(other) do
    raise Event.Error, message: "cannot convert to topic component: #{inspect(other)}"
  end

  # Converts a list of topic components, ex. `["foo", "bar", "baz"]
  #   to a nested hierarchy, ex: [
  #     ["foo"],
  #     ["foo", "bar"],
  #     ["foo", "bar", "baz"],
  #   ]
  defp topics_hierarchy(list) do
    list
    |> :lists.reverse()
    |> do_topics_hierarchy([])
  end

  defp do_topics_hierarchy([], acc) do
    acc
  end

  defp do_topics_hierarchy([subtopic | rest], acc) do
    acc = [[subtopic] | Enum.map(acc, fn subtopics -> [subtopic | subtopics] end)]
    do_topics_hierarchy(rest, acc)
  end
end

# Catch-all: the only way to implement a protocol for generic structs,
#   instead of just specific ones, is to implement Any and match on __struct__.
# This still allows individual structs to override with their own defimpl.
defimpl Event.Topics, for: Any do
  @spec for(%{__struct__: module(), id: Event.id()}) :: [Event.topic()]
  def for(%{__struct__: _module, id: id} = struct) when is_struct(struct) and Event.is_id(id) do
    struct
    |> Event.Subject.for()
    |> Event.Topics.for()
  end

  @spec for(term()) :: no_return()
  def for(other) do
    raise Event.Error, message: "cannot convert to topics: #{inspect(other)}"
  end
end
