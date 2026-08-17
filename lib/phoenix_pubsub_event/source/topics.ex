defprotocol PhoenixPubSubEvent.Source.Topics do
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

  defimpl Event.Source.Topics, for: Entity do
    def for(entity) do
      [
        "\#{Entity}",
        "\#{Entity}:\#{entity.id}",
        "\#{Entity}:\#{entity.id}:\#{entity.status}"
      ]
    end
  end

  entity = %Entity{id: "entity_id", name: "entity_name", status: "entity_status"}

  Event.Source.topics!(entity)
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

  defimpl Event.Source.Subject, for: Entity do
    def for(entity) do
      {Entity, entity.id, entity.status}
    end
  end

  entity = %Entity{id: "entity_id", name: "entity_name", status: "entity_status"}

  entity |> Event.Source.topics!()
  #=> "Elixir.Entity:entity_id:entity_status"
  entity |> Event.Source.Subject.for() |> Event.Source.topics!()
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

alias PhoenixPubSubEvent, as: Event
require Event

defimpl Event.Source.Topics, for: Atom do
  @spec for(atom()) :: [Event.topic()]
  def for(atom) do
    [Atom.to_string(atom)]
  end
end

defimpl Event.Source.Topics, for: BitString do
  @spec for(binary()) :: [Event.topic()]
  def for(binary) when is_binary(binary) do
    [binary]
  end

  @spec for(bitstring()) :: no_return()
  def for(other) do
    raise Event.Error, message: "cannot convert to topics: #{inspect(other)}"
  end
end

defimpl Event.Source.Topics, for: Integer do
  @spec for(integer()) :: [Event.topic()]
  def for(integer) when is_integer(integer) do
    [inspect(integer)]
  end
end

defimpl Event.Source.Topics, for: PID do
  @spec for(pid()) :: [Event.topic()]
  def for(pid) when is_pid(pid) do
    [inspect(pid)]
  end
end

defimpl Event.Source.Topics, for: Reference do
  @spec for(reference()) :: [Event.topic()]
  def for(reference) when is_reference(reference) do
    [inspect(reference)]
  end
end

defimpl Event.Source.Topics, for: Tuple do
  @spec for(tuple()) :: [Event.topic()]
  def for(tuple) do
    tuple
    |> Tuple.to_list()
    |> Enum.reject(&is_nil/1)
    |> topics_hierarchy()
    |> Enum.map(&Enum.join(&1, ":"))
  end

  defp topics_hierarchy(list) do
    list
    |> Enum.flat_map(&Event.Source.Topics.for/1)
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

defimpl Event.Source.Topics, for: List do
  @spec for([term()]) :: [Event.topic()]
  def for(list) when is_list(list) do
    list
    |> Enum.flat_map(&Event.Source.Topics.for/1)
    |> Enum.uniq()
  end
end

defimpl Event.Source.Topics, for: Any do
  @spec for(%{__struct__: module(), id: Event.id()}) :: [Event.topic()]
  def for(%{__struct__: _module, id: id} = struct)
      when is_struct(struct) and Event.is_id(id) do
    struct
    |> Event.Source.Subject.for()
    |> Event.Source.Topics.for()
  end

  @spec for(term()) :: no_return()
  def for(other) do
    raise Event.Error, message: "cannot convert to topics: #{inspect(other)}"
  end
end
