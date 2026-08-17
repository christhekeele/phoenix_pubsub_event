defprotocol PhoenixPubSubEvent.Source.Subscription do
  @moduledoc """
  Protocol for generating an event topic for subscribing to arbitrary values.

  If you want to customize how to subscribe to your struct,
  you can implement this protocol to return an arbitrary binary.

  For example, if you wanted a struct `status` field to be part of
  the topic you subscribe to, you might implement this protocol as such:

  ```elixir
  defmodule Entity do
    defstruct [:id, :name, :status]
  end

  alias PhoenixPubSubEvent, as: Event

  defimpl Event.Source.Subscription, for: Entity do
    def for(entity) do
      "\#{Entity}:\#{entity.id}:\#{entity.status}"
    end
  end

  entity = %Entity{id: "entity_id", name: "entity_name", status: "entity_status"}

  Event.Source.subscription!(entity)
  #=> "Elixir.Entity:entity_id:entity_status"
  ```

  When defining a custom subscription for your struct, you probably want to ensure that
  the subscription for the struct and the subject align.
  If you instead extend the subject from `{module, id}` to match the subscription you want,
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

  entity |> Event.Source.subscription!()
  #=> "Elixir.Entity:entity_id:entity_status"
  entity |> Event.Source.Subject.for() |> Event.Source.subscription()
  #=> "Elixir.Entity:entity_id:entity_status"
  ```

  By default, the subscription for an event source is just the last topic returned
  by `PhoenixPubSubEvent.Source.Topics.for/1`, assuming it is the most specific topic,
  so you can also customize the subscription topic for an event source by simply
  implementing that protocol instead, ensuring your subscription topic is last.

  """

  alias PhoenixPubSubEvent, as: Event

  @fallback_to_any true

  @doc """
  Generates a subscription topic for a given `value`.
  """
  @spec for(t) :: Event.topic() | nil
  def for(value)
end

alias PhoenixPubSubEvent, as: Event

defimpl Event.Source.Subscription, for: Any do
  @spec for(term()) :: Event.topic() | nil
  def for(other) do
    other
    |> Event.Source.Topics.for()
    |> List.last()
  end
end
