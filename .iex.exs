alias PhoenixPubSubEvent, as: Event
defmodule Entity, do: defstruct([:id, :name, :status])

# defimpl Event.Source.Subject, for: Entity do
#   def for(entity) do
#     {Entity, entity.id, entity.status}
#   end
# end
