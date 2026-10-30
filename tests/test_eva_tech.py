# Copyright (C) DATADVANCE, 2010-2023
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
# CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
# TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

"""Check the eva-tech changes that keep 0.9.1 behavior on 1.0.0rc7."""

# NOTE: In this file we use `strict_ordering=True` to simplify testing.

import hashlib
import threading

import channels.db
import channels.layers
import graphene
import graphql
import pytest

import channels_graphql_ws
from channels_graphql_ws.graphql_ws_consumer import GraphqlWsConsumer, call_maybe_sync

# ---------------------------------------------------- GROUP NAME PREFIX


def test_group_name_prefix_is_the_0_9_1_one():
    """Group names are the ones 0.9.1 builds, so both versions share them.

    0.9.1 names a group "GRAPHQL_WS_SUBSCRIPTION-" plus the SHA256 of
    "<module>.<qualname>[-<group>]". A different prefix would split the
    subscribers of a mixed deploy into two sets of groups.
    """
    assert GraphqlWsConsumer.group_name_prefix == "GRAPHQL_WS_SUBSCRIPTION"

    suffix = f"{OnEvent.__module__}.{OnEvent.__qualname__}-user_1"
    expected = (
        "GRAPHQL_WS_SUBSCRIPTION-" + hashlib.sha256(suffix.encode("utf-8")).hexdigest()
    )
    assert OnEvent._group_name("user_1") == expected  # pylint: disable=protected-access


@pytest.mark.asyncio
async def test_consumer_joins_groups_with_the_0_9_1_prefix(gql):
    """The consumer joins the prefixed groups, even if a subclass sets another.

    The `gql` fixture sets `group_name_prefix = "TEST"` on its consumer
    subclass. `Subscription` reads the prefix from `GraphqlWsConsumer`, so the
    groups in the channel layer still start with "GRAPHQL_WS_SUBSCRIPTION-".
    """
    client = gql(
        subscription=Subscription,
        consumer_attrs={"strict_ordering": True},
    )
    await client.connect_and_init()
    await client.start(query="subscription { on_event(user_id: 1) { value } }")
    await client.assert_no_messages()

    groups = set(channels.layers.get_channel_layer().groups)
    assert {
        OnEvent._group_name(),  # pylint: disable=protected-access
        OnEvent._group_name("user_1"),  # pylint: disable=protected-access
    } <= groups
    assert all(not group.startswith("TEST") for group in groups)

    await client.finalize()


# ------------------------------------------------------ THE THREAD POOL


@pytest.mark.asyncio
async def test_call_maybe_sync_runs_a_sync_callback_in_the_pool():
    """A sync callback runs in a "GraphqlWs." thread, off the event loop."""
    loop_thread = threading.current_thread()

    def callback(value):
        return threading.current_thread(), value

    thread, value = await call_maybe_sync(callback, 42)

    assert value == 42
    assert thread is not loop_thread
    assert thread.name.startswith("GraphqlWs.")


@pytest.mark.asyncio
async def test_call_maybe_sync_awaits_an_async_callback_in_the_loop(monkeypatch):
    """An async callback is awaited in the event loop thread, not offloaded.

    It never goes through the pool, so `close_old_connections` does not run.
    """
    calls = []
    monkeypatch.setattr(
        channels.db, "close_old_connections", lambda: calls.append("close")
    )
    loop_thread = threading.current_thread()

    async def callback(value):
        return threading.current_thread(), value

    thread, value = await call_maybe_sync(callback, 42)

    assert value == 42
    assert thread is loop_thread
    assert calls == []


@pytest.mark.asyncio
async def test_run_in_worker_closes_old_connections_around_the_call(monkeypatch):
    """`close_old_connections` runs before and after the call, also on error."""
    calls = []
    monkeypatch.setattr(
        channels.db, "close_old_connections", lambda: calls.append("close")
    )

    def callback():
        calls.append("callback")
        return "result"

    def failing_callback():
        calls.append("failing_callback")
        raise ValueError("Boom!")

    assert await call_maybe_sync(callback) == "result"
    assert calls == ["close", "callback", "close"]

    calls.clear()
    with pytest.raises(ValueError, match="Boom!"):
        await call_maybe_sync(failing_callback)
    assert calls == ["close", "failing_callback", "close"]


@pytest.mark.asyncio
@pytest.mark.parametrize("subprotocol", ["graphql-transport-ws", "graphql-ws"])
async def test_sync_subscription_callbacks_run_in_the_pool(gql, subprotocol):
    """Sync `subscribe`, `publish` and `unsubscribed` run in "GraphqlWs." threads.

    In the event loop thread Django refuses ORM queries, which these
    callbacks make in evapacs-back.
    """
    OnEvent.threads.clear()
    client = gql(
        subscription=Subscription,
        consumer_attrs={"strict_ordering": True},
        subprotocol=subprotocol,
    )
    await client.connect_and_init()

    sub_id = await client.start(query="subscription { on_event(user_id: 1) { value } }")
    await client.assert_no_messages()
    await OnEvent.broadcast_async(group="user_1", payload="hello")
    resp = await client.receive_next(sub_id)
    assert resp["data"]["on_event"]["value"] == "hello"

    await client.complete(sub_id)
    await client.receive_complete(sub_id)

    assert set(OnEvent.threads) == {"subscribe", "publish", "unsubscribed"}
    for callback, thread_name in OnEvent.threads.items():
        assert thread_name.startswith("GraphqlWs."), (callback, thread_name)

    await client.finalize()


# ------------------------------------------ FAILED SUBSCRIBE (graphql-ws)


@pytest.mark.asyncio
async def test_failed_subscribe_graphql_ws_sends_data_then_complete(gql):
    """With graphql-ws a failing `subscribe` ends with data and complete.

    0.9.1 answered the same way, and the front waits for the complete.
    """
    client = gql(
        subscription=Subscription,
        consumer_attrs={"strict_ordering": True},
        subprotocol="graphql-ws",
    )
    await client.connect_and_init()

    sub_id = await client.start(query="subscription { on_failing_subscribe { value } }")
    errors, data = await client.receive_error(sub_id)
    assert data is None
    assert len(errors) == 1
    assert errors[0]["message"] == f"RuntimeError: {OnFailingSubscribe.ERROR}"
    await client.receive_complete(sub_id)
    await client.assert_no_messages()

    await client.finalize()


@pytest.mark.asyncio
async def test_failed_subscribe_graphql_transport_ws_sends_only_error(gql):
    """With graphql-transport-ws a failing `subscribe` sends one error, no complete.

    The protocol ends the operation with the error message itself.
    """
    client = gql(
        subscription=Subscription,
        consumer_attrs={"strict_ordering": True},
        subprotocol="graphql-transport-ws",
    )
    await client.connect_and_init()

    sub_id = await client.start(query="subscription { on_failing_subscribe { value } }")
    errors, data = await client.receive_error(sub_id)
    assert data is None
    assert len(errors) == 1
    assert errors[0]["message"] == f"RuntimeError: {OnFailingSubscribe.ERROR}"
    await client.assert_no_messages()

    await client.finalize()


# ------------------------------------- create_source_event_stream COPY


class _Consumer(GraphqlWsConsumer):
    """Consumer to call `_on_gql_subscribe__create_subscription` directly."""

    schema = graphene.Schema(
        query=type("NumberQuery", (graphene.ObjectType,), {"value": graphene.String()}),
        subscription=type(
            "NumberSubscription",
            (graphene.ObjectType,),
            {"on_number": graphene.Int(number=graphene.Int(required=True))},
        ),
        auto_camelcase=False,
    )


async def _create_subscription(subscribe_field_resolver, variables=None, **kwds):
    """Call the copy of `create_source_event_stream` on a fresh consumer."""
    consumer = _Consumer()
    document = graphql.parse(
        "subscription ($number: Int!) { on_number(number: $number) }"
    )
    return await consumer._on_gql_subscribe__create_subscription(  # pylint: disable=protected-access
        document,
        variable_values=variables,
        subscribe_field_resolver=subscribe_field_resolver,
        **kwds,
    )


@pytest.mark.asyncio
async def test_create_subscription_returns_variable_errors_as_result():
    """Errors from `ExecutionContext.build` come back as an ExecutionResult.

    A missing required variable makes `build` return a list of errors.
    """

    def resolver(*_, **__):
        raise AssertionError("The resolver must not run!")

    result = await _create_subscription(resolver, variables={})

    assert isinstance(result, graphql.ExecutionResult)
    assert result.data is None
    assert [error.message for error in result.errors] == [
        "Variable '$number' of required type 'Int!' was not provided."
    ]


@pytest.mark.asyncio
async def test_create_subscription_returns_resolver_errors_as_result():
    """An error in the subscribe resolver comes back as an ExecutionResult.

    `execute_subscription` wraps it in a GraphQLError located at the field.
    """

    def resolver(*_, **__):
        raise ValueError("Cannot subscribe!")

    result = await _create_subscription(resolver, variables={"number": 1})

    assert isinstance(result, graphql.ExecutionResult)
    assert result.data is None
    assert len(result.errors) == 1
    assert result.errors[0].message == "Cannot subscribe!"
    assert result.errors[0].path == ["on_number"]


@pytest.mark.asyncio
async def test_create_subscription_raises_when_resolver_returns_no_stream():
    """A subscribe resolver that returns no AsyncIterable raises TypeError.

    Same as graphql-core 3.2.13: it is a programming error, not a result.
    """
    with pytest.raises(
        TypeError,
        match="Subscription field must return AsyncIterable. Received: 42.",
    ):
        await _create_subscription(lambda *_, **__: 42, variables={"number": 1})


@pytest.mark.asyncio
@pytest.mark.parametrize("subprotocol", ["graphql-transport-ws", "graphql-ws"])
async def test_execution_context_class_runs_every_operation(gql, subprotocol):
    """`execution_context_class` builds the context of queries and subscriptions.

    For a subscription it builds the source stream context, which upstream
    `create_source_event_stream` always builds with the default class, and
    the context of every notification.
    """
    RecordingExecutionContext.operations.clear()
    client = gql(
        query=Query,
        subscription=Subscription,
        consumer_attrs={
            "strict_ordering": True,
            "execution_context_class": RecordingExecutionContext,
        },
        subprotocol=subprotocol,
    )
    await client.connect_and_init()

    msg_id = await client.start(query="query { value }")
    resp = await client.receive_next(msg_id)
    assert resp["data"] == {"value": Query.VALUE}
    await client.receive_complete(msg_id)
    assert RecordingExecutionContext.operations == ["query"]

    sub_id = await client.start(query="subscription { on_event(user_id: 1) { value } }")
    await client.assert_no_messages()
    assert RecordingExecutionContext.operations == ["query", "subscription"]

    await OnEvent.broadcast_async(group="user_1", payload="hello")
    resp = await client.receive_next(sub_id)
    assert resp["data"]["on_event"]["value"] == "hello"
    assert RecordingExecutionContext.operations == [
        "query",
        "subscription",
        "subscription",
    ]

    await client.finalize()


# --------------------------------------------------------------- SCHEMA


class RecordingExecutionContext(graphql.ExecutionContext):
    """Execution context that records the operation type it runs."""

    operations: list = []

    def __init__(self, *args, **kwds):
        """Record the operation type."""
        super().__init__(*args, **kwds)
        RecordingExecutionContext.operations.append(self.operation.operation.value)


class OnEvent(channels_graphql_ws.Subscription):
    """Subscription that records the thread of each callback."""

    # pylint: disable=arguments-differ

    threads: dict = {}

    value = graphene.String()

    class Arguments:
        """Subscription arguments."""

        user_id = graphene.Int()

    @staticmethod
    def subscribe(root, info, user_id):
        """Record the thread and join the user's group."""
        del root, info
        OnEvent.threads["subscribe"] = threading.current_thread().name
        return [f"user_{user_id}"]

    @staticmethod
    def publish(payload, info, user_id):
        """Record the thread and send the payload."""
        del info, user_id
        OnEvent.threads["publish"] = threading.current_thread().name
        return OnEvent(value=payload)

    @staticmethod
    def unsubscribed(root, info, user_id):
        """Record the thread."""
        del root, info, user_id
        OnEvent.threads["unsubscribed"] = threading.current_thread().name


class OnFailingSubscribe(channels_graphql_ws.Subscription):
    """Subscription whose `subscribe` raises."""

    # pylint: disable=arguments-differ

    ERROR = "Subscribe failed!"

    value = graphene.String()

    @staticmethod
    def subscribe(root, info):
        """Fail, as an access check that rejects the user does."""
        del root, info
        raise RuntimeError(OnFailingSubscribe.ERROR)

    @staticmethod
    def publish(payload, info):
        """Never runs."""
        del payload, info
        raise AssertionError("publish must not run!")


class Subscription(graphene.ObjectType):
    """Root subscription."""

    on_event = OnEvent.Field()
    on_failing_subscribe = OnFailingSubscribe.Field()


class Query(graphene.ObjectType):
    """Root query."""

    VALUE = "Hi!"

    value = graphene.String()

    @staticmethod
    def resolve_value(root, info):
        """Return a constant."""
        del root, info
        return Query.VALUE
