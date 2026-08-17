defmodule PhoenixPubSubEvent.MixProject do
  use Mix.Project

  def project do
    [
      app: :phoenix_pubsub_event,
      version: "0.1.0",
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      docs: docs()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:phoenix_pubsub, "~> 2.2.0"},
      {:ex_doc, "~> 0.40", only: :dev, runtime: false}
    ]
  end

  defp docs do
    [
      groups_for_docs: [
        Guards: &(&1[:section] == :guards)
      ],
      groups_for_modules: [
        Core: [PhoenixPubSubEvent, PhoenixPubSubEvent.Source],
        Protocols: [
          PhoenixPubSubEvent.Source.Subject,
          PhoenixPubSubEvent.Source.Subscription,
          PhoenixPubSubEvent.Source.Topics
        ]
      ],
      nest_modules_by_prefix: [PhoenixPubSubEvent, PhoenixPubSubEvent.Source]
    ]
  end
end
