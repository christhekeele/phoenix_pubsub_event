alias PhoenixPubSubEvent, as: Event

defprotocol PhoenixPubSubEvent.Subscription do
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

  defimpl Event.Subscription, for: Entity do
    def for(entity) do
      "\#{Entity}:\#{entity.id}:\#{entity.status}"
    end
  end

  entity = %Entity{id: "entity_id", name: "entity_name", status: "entity_status"}

  Event.subscription!(entity)
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

  defimpl Event.Subject, for: Entity do
    def for(entity) do
      {Entity, entity.id, entity.status}
    end
  end

  entity = %Entity{id: "entity_id", name: "entity_name", status: "entity_status"}

  entity |> Event.subscription!()
  #=> "Elixir.Entity:entity_id:entity_status"
  entity |> Event.Subject.for() |> Event.subscription()
  #=> "Elixir.Entity:entity_id:entity_status"
  ```

  By default, the subscription for an event source is just the last topic returned
  by `PhoenixPubSubEvent.Topics.for/1`, assuming it is the most specific topic,
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

# Catch-all: the only way to implement a protocol for generic structs,
#   instead of just specific ones, is to implement Any and match on __struct__.
# This still allows individual structs to override with their own defimpl.
defimpl Event.Subscription, for: Any do
  @spec for(term()) :: Event.topic() | nil
  def for(value) do
    # By default, just consult the list of topics, and pick the last one
    #   as the primary topic to subscribe to, under the convention that topics
    #   later in the topics list are more specific.
    value
    |> Event.Topics.for()
    |> List.last()
  rescue
    Event.Error ->
      # Return `nil` if `value` is not subscribable.
      nil
  end
end
