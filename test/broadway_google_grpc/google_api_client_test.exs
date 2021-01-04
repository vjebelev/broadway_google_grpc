defmodule BroadwayGoogleGrpc.GoogleApiClientTest do
  use ExUnit.Case

  import ExUnit.CaptureLog

  alias BroadwayCloudPubSub.ClientAcknowledger
  alias BroadwayGoogleGrpc.GoogleApiClient
  alias Broadway.Message
  alias Broadway.TermStorage

  @subscription_base "https://pubsub.googleapis.com/v1/projects/foo/subscriptions/bar"

  @pull_response """
  {
    "receivedMessages": [
      {
        "ackId": "1",
        "message": {
          "data": "TWVzc2FnZTE=",
          "messageId": "19917247034",
          "attributes": {
            "foo": "bar",
            "qux": ""
          },
          "publishTime": "2014-02-14T00:00:01Z"
        }
      },
      {
        "ackId": "2",
        "message": {
          "data": "TWVzc2FnZTI=",
          "messageId": "19917247035",
          "attributes": {},
          "publishTime": "2014-02-14T00:00:01Z"
        }
      },
      {
        "ackId": "3",
        "message": {
          "data": null,
          "messageId": "19917247036",
          "attributes": {
            "number": "three"
          },
          "publishTime": "2014-02-14T00:00:02Z"
        }
      }
    ]
  }
  """

  @empty_response """
  {}
  """

  describe "validate init options" do
    test ":subscription is required" do
      assert GoogleApiClient.init([]) ==
               {:error, "expected :subscription to be a non empty string, got: nil"}

      assert GoogleApiClient.init(subscription: nil) ==
               {:error, "expected :subscription to be a non empty string, got: nil"}
    end

    test ":subscription should be a valid subscription name" do
      assert GoogleApiClient.init(subscription: "") ==
               {:error, "expected :subscription to be a non empty string, got: \"\""}

      assert GoogleApiClient.init(subscription: :an_atom) ==
               {:error, "expected :subscription to be a non empty string, got: :an_atom"}

      assert {:ok, %{subscription: subscription}} =
               GoogleApiClient.init(subscription: "projects/foo/subscriptions/bar")

      assert subscription.projects_id == "foo"
      assert subscription.subscriptions_id == "bar"
    end

    test ":return_immediately is nil without default value" do
      {:ok, result} = GoogleApiClient.init(subscription: "projects/foo/subscriptions/bar")

      assert is_nil(result.pull_request.return_immediately)
    end

    test ":return immediately should be a boolean" do
      opts = [subscription: "projects/foo/subscriptions/bar"]

      {:ok, result} = opts |> Keyword.put(:return_immediately, true) |> GoogleApiClient.init()
      assert result.pull_request.return_immediately == true

      {:ok, result} = opts |> Keyword.put(:return_immediately, false) |> GoogleApiClient.init()
      assert is_nil(result.pull_request.return_immediately)

      {:error, message} =
        opts |> Keyword.put(:return_immediately, "true") |> GoogleApiClient.init()

      assert message == "expected :return_immediately to be a boolean value, got: \"true\""

      {:error, message} = opts |> Keyword.put(:return_immediately, 0) |> GoogleApiClient.init()

      assert message == "expected :return_immediately to be a boolean value, got: 0"

      {:error, message} =
        opts |> Keyword.put(:return_immediately, :an_atom) |> GoogleApiClient.init()

      assert message == "expected :return_immediately to be a boolean value, got: :an_atom"
    end

    test ":max_number_of_messages is optional with default value 10" do
      {:ok, result} = GoogleApiClient.init(subscription: "projects/foo/subscriptions/bar")

      assert result.pull_request.max_messages == 10
    end

    test ":max_number_of_messages should be a positive integer" do
      opts = [subscription: "projects/foo/subscriptions/bar"]

      {:ok, result} = opts |> Keyword.put(:max_number_of_messages, 1) |> GoogleApiClient.init()
      assert result.pull_request.max_messages == 1

      {:ok, result} = opts |> Keyword.put(:max_number_of_messages, 10) |> GoogleApiClient.init()
      assert result.pull_request.max_messages == 10

      {:error, message} =
        opts |> Keyword.put(:max_number_of_messages, 0) |> GoogleApiClient.init()

      assert message == "expected :max_number_of_messages to be a positive integer, got: 0"

      {:error, message} =
        opts |> Keyword.put(:max_number_of_messages, :an_atom) |> GoogleApiClient.init()

      assert message == "expected :max_number_of_messages to be a positive integer, got: :an_atom"
    end

    test ":scope should be a string or tuple" do
      opts = [subscription: "projects/foo/subscriptions/bar"]

      {:ok, result} = opts |> Keyword.put(:scope, "https://example.com") |> GoogleApiClient.init()

      assert {_, _, ["https://example.com"]} = result.token_generator

      {:error, message} = opts |> Keyword.put(:scope, :an_atom) |> GoogleApiClient.init()

      assert message == "expected :scope to be a non empty string or tuple, got: :an_atom"

      {:error, message} = opts |> Keyword.put(:scope, 1) |> GoogleApiClient.init()

      assert message == "expected :scope to be a non empty string or tuple, got: 1"

      {:error, message} = opts |> Keyword.put(:scope, {}) |> GoogleApiClient.init()

      assert message == "expected :scope to be a non empty string or tuple, got: {}"

      {:ok, result} =
        opts
        |> Keyword.put(:scope, {"mail@example.com", "https://example.com"})
        |> GoogleApiClient.init()

      assert {_, _, [{"mail@example.com", "https://example.com"}]} = result.token_generator
    end

    test ":token_generator defaults to using Goth with default scope" do
      opts = [subscription: "projects/foo/subscriptions/bar"]

      {:ok, result} = GoogleApiClient.init(opts)

      assert result.token_generator ==
               {BroadwayGoogleGrpc.GoogleApiClient, :generate_goth_token,
                ["https://www.googleapis.com/auth/pubsub"]}
    end

    test ":token_generator should be a tuple {Mod, Fun, Args}" do
      opts = [subscription: "projects/foo/subscriptions/bar"]

      token_generator = {Token, :fetch, []}

      {:ok, result} =
        opts
        |> Keyword.put(:token_generator, token_generator)
        |> GoogleApiClient.init()

      assert result.token_generator == token_generator

      {:error, message} =
        opts
        |> Keyword.put(:token_generator, {1, 1, 1})
        |> GoogleApiClient.init()

      assert message == "expected :token_generator to be a tuple {Mod, Fun, Args}, got: {1, 1, 1}"

      {:error, message} =
        opts
        |> Keyword.put(:token_generator, SomeModule)
        |> GoogleApiClient.init()

      assert message ==
               "expected :token_generator to be a tuple {Mod, Fun, Args}, got: SomeModule"
    end

    test ":token_generator supercedes :scope validation" do
      opts = [subscription: "projects/foo/subscriptions/bar"]

      assert {:ok, _result} =
               opts
               |> Keyword.put(:scope, :an_invalid_scope)
               |> Keyword.put(:token_generator, {__MODULE__, :generate_token, []})
               |> GoogleApiClient.init()
    end

    test ":on_success should be a valid acknowledgement option" do
      opts = [client: GoogleApiClient, subscription: "projects/foo/subscriptions/bar"]

      assert {:ok, result} = opts |> Keyword.put(:on_success, :ack) |> GoogleApiClient.init()
      assert %ClientAcknowledger{on_success: :ack} = TermStorage.get!(result.ack_ref)

      assert {:ok, result} = opts |> Keyword.put(:on_success, :nack) |> GoogleApiClient.init()
      assert %ClientAcknowledger{on_success: {:nack, 0}} = TermStorage.get!(result.ack_ref)

      assert {:ok, result} =
               opts |> Keyword.put(:on_success, {:nack, 10}) |> GoogleApiClient.init()

      assert %ClientAcknowledger{on_success: {:nack, 10}} = TermStorage.get!(result.ack_ref)

      {:error, message} =
        opts
        |> Keyword.put(:on_success, 1)
        |> GoogleApiClient.init()

      assert message ==
               "expected :on_success to be a valid acknowledgement option, got: 1"

      {:error, message} =
        opts
        |> Keyword.put(:on_success, {:nack, :foo})
        |> GoogleApiClient.init()

      assert message ==
               "expected :on_success to be a valid acknowledgement option, got: {:nack, :foo}"
    end

    test ":on_failure should be a valid acknowledgement option" do
      opts = [client: GoogleApiClient, subscription: "projects/foo/subscriptions/bar"]

      assert {:ok, result} = opts |> Keyword.put(:on_failure, :ack) |> GoogleApiClient.init()
      assert %ClientAcknowledger{on_failure: :ack} = TermStorage.get!(result.ack_ref)

      assert {:ok, result} = opts |> Keyword.put(:on_failure, :nack) |> GoogleApiClient.init()
      assert %ClientAcknowledger{on_failure: {:nack, 0}} = TermStorage.get!(result.ack_ref)

      assert {:ok, result} =
               opts |> Keyword.put(:on_failure, {:nack, 10}) |> GoogleApiClient.init()

      assert %ClientAcknowledger{on_failure: {:nack, 10}} = TermStorage.get!(result.ack_ref)

      {:error, message} =
        opts
        |> Keyword.put(:on_failure, 1)
        |> GoogleApiClient.init()

      assert message ==
               "expected :on_failure to be a valid acknowledgement option, got: 1"

      {:error, message} =
        opts
        |> Keyword.put(:on_failure, {:nack, :foo})
        |> GoogleApiClient.init()

      assert message ==
               "expected :on_failure to be a valid acknowledgement option, got: {:nack, :foo}"
    end
  end

  def generate_token, do: {:ok, "token.#{System.os_time(:second)}"}
end
