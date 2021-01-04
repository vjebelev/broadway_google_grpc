defmodule BroadwayGoogleGrpc.MixProject do
  use Mix.Project

  @version "0.1.0-beta.1"


  def project do
    [
      app: :broadway_google_grpc,
      version: @version,
      elixir: "~> 1.6",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      package: package(),
      description: description(),
      source_url: "http://github.com/vjebelev/broadway_google_grpc"
    ]
  end

  def package do
    [
      name: "broadway_google_grpc",
      maintainers: ["Vlad Jebelev"],
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/vjebelev/broadway_google_grpc"}
    ]
  end

  def description do
    "Broadway connector for Google Pubsub (grpc endpoint)"
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:goth, "~> 1.2.0"},
      {:google_protos, "~> 0.1.0"},
      {:gun, "~> 2.0.0", hex: :grpc_gun, override: true},
      {:broadway_cloud_pub_sub, "~> 0.6.0"},
      {:cowlib, "~> 2.9.0", override: true},
      {:google_pubsub_grpc, "~> 0.1.0-beta.1"}
    ]
  end
end
