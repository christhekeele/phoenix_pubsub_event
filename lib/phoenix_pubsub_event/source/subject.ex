defprotocol PhoenixPubSubEvent.Source.Subject do
  @moduledoc """
  Protocol for generating event subjects from arbitrary values.

  If you want to customize how your struct appears in events,
  you can implement this protocol to return an arbitrary value.

  For example, if you wanted a struct `status` field to be part of
  events emitted from your struct, you might implement this protocol as such:

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

  Event.Source.event(entity, :event_name)
  #=> {{Entity, "entity_id", "entity_status"}, :event_name, %{}}
  ```

  When defining a custom subject for your struct, you probably want to ensure that topics for the
  struct and the subject align. If you are simply extending the subject from `{module, id}` to
  a longer tuple, this will happen automatically; otherwise you should also implement
  `PhoenixPubSubEvent.Source.Topics` and align the topics for your struct with the topics for your subject.

  ```elixir
  defmodule Entity do
    defstruct [:id, :name, :status]
  end

  alias PhoenixPubSubEvent, as: Event

  defimpl Event.Source.Subject, for: Entity do
    def for(enitity) do
      {Entity, entity.id, entity.status}
    end
  end

  entity = %Entity{id: "entity_id", name: "entity_name", status: "entity_status"}

  entity |> Event.Source.Topics.for()
  #=> ["Elixir.Entity", "Elixir.Entity:entity_id", "Elixir.Entity:entity_id:entity_status"]
  entity |> Event.Source.Subject.for() |> Event.Source.Topics.for()
  #=> ["Elixir.Entity", "Elixir.Entity:entity_id", "Elixir.Entity:entity_id:entity_status"]
  ```

  """

  alias PhoenixPubSubEvent, as: Event

  @fallback_to_any true

  @doc """
  Generates an event subject for a given `value`.
  """
  @spec for(t) :: Event.subject()
  def for(value)
end

alias PhoenixPubSubEvent, as: Event
require Event

defimpl Event.Source.Subject, for: Atom do
  @spec for(atom()) :: atom()
  def for(atom) when is_atom(atom) do
    atom
  end
end

defimpl Event.Source.Subject, for: Tuple do
  @spec for(tuple()) :: tuple()
  def for(tuple) when is_tuple(tuple) do
    tuple
  end
end

defimpl Event.Source.Subject, for: Any do
  @spec for(%{__struct__: module(), id: Event.id()}) :: {module(), Event.id()}
  def for(%{__struct__: module, id: id} = struct)
      when is_struct(struct) and Event.is_id(id) do
    Event.Source.Subject.for({module, id})
  end

  @spec for(term()) :: no_return()
  def for(other) do
    raise Event.Error, message: "cannot convert to subject: #{inspect(other)}"
  end
end
