defmodule BroadwayGoogleGrpc.GoogleApiClient do
  @moduledoc """
  gRPC-based Pub/Sub client used by `BroadwayCloudPubSub.Producer` to communicate with Google
  Cloud Pub/Sub service. This client implements the `BroadwayCloudPubSub.Client` behaviour
  which defines callbacks for receiving and acknowledging messages.
  """

  alias Google.Pubsub.GRPC
  alias Broadway.Message
  alias BroadwayCloudPubSub.{Client, ClientAcknowledger}

  alias Google.Pubsub.V1.{
    PullRequest,
    PullResponse,
    ReceivedMessage,
    PubsubMessage,
  }

  require Logger

  @behaviour Client

  @default_max_number_of_messages 10

  @default_scope "https://www.googleapis.com/auth/pubsub"

  defp conn!(config, adapter_opts \\ []) do
    %{
      connection_pool: connection_pool,
      token_generator: {mod, fun, args}
    } = config

    {:ok, token} = apply(mod, fun, args)
    #
    # TODO connection pooling
    {:ok, channel} = GRPC.channel(headers: ["authorization": "#{token.type} #{token.token}"])
    channel
  end

  @impl Client
  def init(opts) do
    with {:ok, subscription} <- validate_subscription(opts),
         {:ok, token_generator} <- validate_token_opts(opts),
         {:ok, pull_request} <- validate_pull_request(opts),
         {:ok, ack} <- ClientAcknowledger.init([client: __MODULE__] ++ opts) do
      adapter = Keyword.get(opts, :__internal_tesla_adapter__, Hackney)
      connection_pool = Keyword.get(opts, :__connection_pool__, :default)

      config = %{
        adapter: adapter,
        connection_pool: connection_pool,
        subscription: subscription,
        token_generator: token_generator
      }

      ack_ref = ClientAcknowledger.ack_ref(ack, config)

      {:ok, Map.merge(config, %{ack_ref: ack_ref, pull_request: pull_request})}
    end
  end

  @impl Client
  def receive_messages(demand, opts) do
    pull_request = put_max_number_of_messages(opts.pull_request, demand)

    opts
    |> conn!(recv_timeout: :infinity)
    |> Google.Pubsub.V1.Subscriber.Stub.pull(pull_request)
    |> handle_response(:receive_messages)
    |> wrap_received_messages(opts.ack_ref)
  end

  @impl Client
  def acknowledge(ack_ids, opts) do
    ack_request = %Google.Pubsub.V1.AcknowledgeRequest{
      subscription: Google.Pubsub.GRPC.full_subscription_name(opts.subscription.subscriptions_id),
      ack_ids: ack_ids
    }

    opts
    |> conn!()
    |> Google.Pubsub.V1.Subscriber.Stub.acknowledge(ack_request)
    |> handle_response(:acknowledge)
  end

  @impl Client
  def put_deadline(ack_ids, deadline, opts) when deadline in 0..600 do

    modify_ack_request = %Google.Pubsub.V1.ModifyAckDeadlineRequest{
      subscription: Google.Pubsub.GRPC.full_subscription_name(opts.subscription.subscriptions_id),
      ack_ids: ack_ids,
      ack_deadline_seconds: deadline
    }

    opts
    |> conn!()
    |> Google.Pubsub.V1.Subscriber.Stub.modify_ack_deadline(modify_ack_request)
    |> handle_response(:put_deadline)
  end

  defp handle_response({:ok, %PullResponse{received_messages: []}}, :receive_messages) do
    []
  end

  defp handle_response({:ok, response}, :receive_messages) do
    %PullResponse{received_messages: received_messages} = response
    received_messages
  end

  defp handle_response({:ok, _}, _), do: :ok

  defp handle_response({:error, reason}, :receive_messages) do
    Logger.error("Unable to fetch events from Cloud Pub/Sub. Reason: #{inspect(reason)}")
    []
  end

  defp handle_response({:error, reason}, :acknowledge) do
    Logger.error("Unable to acknowledge messages with Cloud Pub/Sub, reason: #{inspect(reason)}")
    :ok
  end

  defp handle_response({:error, reason}, :put_deadline) do
    Logger.error("Unable to put new ack deadline with Cloud Pub/Sub, reason: #{inspect(reason)}")
    :ok
  end

  defp wrap_received_messages(received_messages, ack_ref) do
    Enum.map(received_messages, fn received_message ->
      %ReceivedMessage{message: message, ack_id: ack_id} = received_message

      {data, metadata} =
        message
        |> Map.from_struct()
        |> Map.pop(:data)

      %Message{
        data: data,
        metadata: metadata,
        acknowledger: ClientAcknowledger.acknowledger(ack_id, ack_ref)
      }
    end)
  end

  defp put_max_number_of_messages(pull_request, demand) do
    max_number_of_messages = min(demand, pull_request.max_messages)

    %{pull_request | max_messages: max_number_of_messages}
  end

  defp validate(opts, key, default \\ nil) when is_list(opts) do
    validate_option(key, opts[key] || default)
  end

  defp validate_option(:token_generator, {m, f, args})
       when is_atom(m) and is_atom(f) and is_list(args) do
    {:ok, {m, f, args}}
  end

  defp validate_option(:token_generator, value),
    do: validation_error(:token_generator, "a tuple {Mod, Fun, Args}", value)

  defp validate_option(:scope, value)
       when not (is_binary(value) or is_tuple(value)) or (value == "" or value == {}),
       do: validation_error(:scope, "a non empty string or tuple", value)

  defp validate_option(:subscription, value) when not is_binary(value) or value == "",
    do: validation_error(:subscription, "a non empty string", value)

  defp validate_option(:max_number_of_messages, value) when not is_integer(value) or value < 1,
    do: validation_error(:max_number_of_messages, "a positive integer", value)

  defp validate_option(:return_immediately, nil), do: {:ok, nil}

  defp validate_option(:return_immediately, value) when not is_boolean(value),
    do: validation_error(:return_immediately, "a boolean value", value)

  defp validate_option(_, value), do: {:ok, value}

  defp validation_error(option, expected, value) do
    {:error, "expected #{inspect(option)} to be #{expected}, got: #{inspect(value)}"}
  end

  defp validate_pull_request(opts) do
    with {:ok, subscription} <- validate_subscription(opts),
         {:ok, return_immediately} <- validate(opts, :return_immediately),
         {:ok, max_number_of_messages} <-
           validate(opts, :max_number_of_messages, @default_max_number_of_messages) do
      {:ok,
       %PullRequest{
         subscription: Google.Pubsub.GRPC.full_subscription_name(subscription.subscriptions_id),
         max_messages: max_number_of_messages,
         return_immediately: return_immediately
       }}
    end
  end

  defp validate_token_opts(opts) do
    case Keyword.fetch(opts, :token_generator) do
      {:ok, _} -> validate(opts, :token_generator)
      :error -> validate_scope(opts)
    end
  end

  defp validate_scope(opts) do
    with {:ok, scope} <- validate(opts, :scope, @default_scope) do
      ensure_goth_loaded()
      {:ok, {__MODULE__, :generate_goth_token, [scope]}}
    end
  end

  defp ensure_goth_loaded() do
    unless Code.ensure_loaded?(Goth.Token) do
      Logger.error("""
      the default authentication token generator uses the Goth library but it's not available

      Add goth to your dependencies:

          defp deps() do
            {:goth, "~> 1.0"}
          end

      Or provide your own token generator:

          Broadway.start_link(
            producers: [
              default: [
                module: {BroadwayCloudPubSub.Producer,
                  token_generator: {MyGenerator, :generate, ["foo"]}
                }
              ]
            ]
          )
      """)
    end
  end

  defp validate_subscription(opts) do
    with {:ok, subscription} <- validate(opts, :subscription) do
      subscription |> String.split("/") |> validate_sub_parts(subscription)
    end
  end

  defp validate_sub_parts(
         ["projects", projects_id, "subscriptions", subscriptions_id],
         _subscription
       ) do
    {:ok, %{projects_id: projects_id, subscriptions_id: subscriptions_id}}
  end

  defp validate_sub_parts(_, subscription) do
    validation_error(:subscription, "an valid subscription name", subscription)
  end

end
