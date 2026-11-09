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

import asyncio
import contextvars
import hashlib
import os
import threading

import channels.db
import channels.layers
import django.db
import django.db.backends.sqlite3.base
import django.utils.translation
import graphene
import graphql
import pytest
from django.contrib.auth.models import User

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
async def test_call_maybe_sync_awaits_an_async_callback_in_the_loop():
    """An async callback is awaited in the event loop thread, not offloaded."""
    loop_thread = threading.current_thread()

    async def callback(value):
        return threading.current_thread(), value

    thread, value = await call_maybe_sync(callback, 42)

    assert value == 42
    assert thread is loop_thread


@pytest.fixture
def closable_sqlite(monkeypatch):
    """Let the test database connections of the worker threads close.

    The tests run on an in-memory SQLite database, and Django's SQLite
    backend ignores `close()` on an in-memory database. With that guard off,
    `close_old_connections` closes a worker's connection as it does with
    Postgres in evapacs-back. The database survives because the test thread
    keeps its own connection open.
    """
    monkeypatch.setattr(
        django.db.backends.sqlite3.base.DatabaseWrapper,
        "is_in_memory_db",
        lambda self: False,
    )
    assert django.db.connection.settings_dict["CONN_MAX_AGE"] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("subprotocol", ["graphql-transport-ws", "graphql-ws"])
async def test_sync_callbacks_query_the_db_and_close_the_connection(
    gql, closable_sqlite, subprotocol
):
    """Sync `subscribe` and `publish` query the ORM and leave no connection open.

    The client gets the user count that `publish` reads from the database.
    With CONN_MAX_AGE = 0 the connection each worker thread opened for its
    query is closed once the callback returns, as Django does at the end of
    an HTTP request.
    """
    del closable_sqlite
    OnUserCount.connections.clear()
    user_count = await channels.db.database_sync_to_async(User.objects.count)()
    client = gql(
        subscription=Subscription,
        consumer_attrs={"strict_ordering": True},
        subprotocol=subprotocol,
    )
    await client.connect_and_init()

    sub_id = await client.start(query="subscription { on_user_count { count } }")
    await client.assert_no_messages()
    await OnUserCount.broadcast_async(payload=None)
    resp = await client.receive_next(sub_id)
    assert resp["data"]["on_user_count"]["count"] == user_count

    assert set(OnUserCount.connections) == {"subscribe", "publish"}
    for callback, connection in OnUserCount.connections.items():
        assert connection.connection is None, f"{callback} left its connection open!"

    await client.finalize()


@pytest.mark.asyncio
@pytest.mark.parametrize("subprotocol", ["graphql-transport-ws", "graphql-ws"])
async def test_failed_sync_subscribe_closes_the_connection(
    gql, closable_sqlite, subprotocol
):
    """A sync `subscribe` that queries the ORM and raises still closes it."""
    del closable_sqlite
    OnUserCount.connections.clear()
    client = gql(
        subscription=Subscription,
        consumer_attrs={"strict_ordering": True},
        subprotocol=subprotocol,
    )
    await client.connect_and_init()

    sub_id = await client.start(
        query="subscription { on_user_count(fail: true) { count } }"
    )
    errors, data = await client.receive_error(sub_id)
    assert data is None
    assert errors == [{"message": f"RuntimeError: {OnUserCount.ERROR}"}]
    if subprotocol == "graphql-ws":
        await client.receive_complete(sub_id)

    connection = OnUserCount.connections["subscribe"]
    assert connection.connection is None, "subscribe left its connection open!"

    await client.finalize()


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

    0.9.1 answered the same way, and the front waits for the complete. The
    error is "<class name>: <message>", without locations, path or extensions.
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
    assert errors == [{"message": f"RuntimeError: {OnFailingSubscribe.ERROR}"}]
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
    assert errors == [{"message": f"RuntimeError: {OnFailingSubscribe.ERROR}"}]
    await client.assert_no_messages()

    await client.finalize()


@pytest.mark.asyncio
async def test_failed_subscribe_is_logged_as_error(gql, caplog):
    """The failure is logged as ERROR, as 0.9.1 logged it, so Sentry gets it."""
    client = gql(
        subscription=Subscription,
        consumer_attrs={"strict_ordering": True},
        subprotocol="graphql-ws",
    )
    await client.connect_and_init()

    sub_id = await client.start(query="subscription { on_failing_subscribe { value } }")
    await client.receive_error(sub_id)
    await client.receive_complete(sub_id)

    records = [
        record
        for record in caplog.records
        if record.getMessage().startswith("GraphQL resolver failed on operation")
    ]
    assert [record.levelname for record in records] == ["ERROR"]

    await client.finalize()


@pytest.mark.asyncio
async def test_resolver_error_log_has_no_query_text(gql, caplog):
    """The log has the original exception, not the query graphql-core 3 adds.

    The query can carry patient data in its literals.
    """
    client = gql(
        query=Query,
        subscription=Subscription,
        consumer_attrs={"strict_ordering": True},
        subprotocol="graphql-ws",
    )
    await client.connect_and_init()

    op_id = await client.start(query='query { failing(text: "ZZZTEST") }')
    await client.receive_error(op_id)
    await client.receive_complete(op_id)

    messages = [
        record.getMessage()
        for record in caplog.records
        if record.getMessage().startswith("GraphQL resolver failed on operation")
    ]
    assert len(messages) == 1
    assert f"RuntimeError: {Query.ERROR}" in messages[0]
    assert "ZZZTEST" not in messages[0]

    await client.finalize()


# ------------------------------------------------ STOP DURING SUBSCRIBE


@pytest.mark.asyncio
@pytest.mark.parametrize("subprotocol", ["graphql-transport-ws", "graphql-ws"])
async def test_stop_while_subscribe_runs_unsubscribes(gql, subprotocol):
    """A STOP that arrives while `subscribe` runs in the pool still unsubscribes.

    Before the fix the STOP found no operation, sent no `complete`, and later
    broadcasts reached a subscription the client had stopped.
    """
    OnSlowSubscribe.reset()
    client = gql(subscription=Subscription, subprotocol=subprotocol)
    await client.connect_and_init()

    sub_id = await client.start(query="subscription { on_slow_subscribe { value } }")
    entered = await asyncio.get_running_loop().run_in_executor(
        None, OnSlowSubscribe.entered.wait, 5
    )
    assert entered, "subscribe never started!"
    await client.complete(sub_id)
    # The consumer takes the STOP while `subscribe` still waits in the pool.
    await asyncio.sleep(0.2)
    OnSlowSubscribe.release.set()

    await client.receive_complete(sub_id)
    await OnSlowSubscribe.broadcast_async(payload="late")
    await client.assert_no_messages()
    assert OnSlowSubscribe.unsubscribed_calls == 1

    await client.finalize()


@pytest.mark.asyncio
@pytest.mark.parametrize("subprotocol", ["graphql-transport-ws", "graphql-ws"])
async def test_disconnect_while_subscribe_runs_leaves_no_pending_task(gql, subprotocol):
    """A disconnect while `subscribe` runs in the pool leaves no task behind.

    Before the fix the task waiting for the registration stayed pending
    forever, because the cancelled subscribe never cancelled it.
    """
    OnSlowSubscribe.reset()
    client = gql(subscription=Subscription, subprotocol=subprotocol)
    await client.connect_and_init()

    await client.start(query="subscription { on_slow_subscribe { value } }")
    entered = await asyncio.get_running_loop().run_in_executor(
        None, OnSlowSubscribe.entered.wait, 5
    )
    assert entered, "subscribe never started!"
    await client.finalize()
    OnSlowSubscribe.release.set()
    await asyncio.sleep(0.2)

    pending = [
        task
        for task in asyncio.all_tasks()
        if task.get_coro().__qualname__ == "Event.wait"
    ]
    assert not pending, f"Tasks still wait for the registration: {pending}"


# --------------------------------------------------- FAILED NOTIFICATION


@pytest.mark.asyncio
@pytest.mark.parametrize("subprotocol", ["graphql-transport-ws", "graphql-ws"])
async def test_failed_notification_ends_the_operation(gql, subprotocol):
    """A notification that fails outside the resolvers ends its operation.

    The client gets the error, `unsubscribed` runs, and the operation id is
    free again. Before the fix the id stayed taken: graphql-ws answered
    "already exists" and graphql-transport-ws closed the socket with 4409.
    """
    OnEvent.threads.clear()
    FailingNotificationContext.fail = False
    client = gql(
        subscription=Subscription,
        consumer_attrs={"execution_context_class": FailingNotificationContext},
        subprotocol=subprotocol,
    )
    await client.connect_and_init()
    query = "subscription { on_event(user_id: 1) { value } }"

    sub_id = await client.start(query=query)
    await client.assert_no_messages()
    FailingNotificationContext.fail = True
    await OnEvent.broadcast_async(group="user_1", payload="hello")
    errors, data = await client.receive_error(sub_id)
    assert data is None
    assert errors[0]["message"] == f"RuntimeError: {FailingNotificationContext.ERROR}"
    if subprotocol == "graphql-ws":
        await client.receive_complete(sub_id)
    FailingNotificationContext.fail = False
    await client.assert_no_messages()
    assert "unsubscribed" in OnEvent.threads

    await client.start(query=query, msg_id=sub_id)
    await client.assert_no_messages()
    await OnEvent.broadcast_async(group="user_1", payload="again")
    resp = await client.receive_next(sub_id)
    assert resp["data"]["on_event"]["value"] == "again"

    await client.finalize()


# ---------------------------------- POOL SIZE, BACKPRESSURE AND CONTEXT


@pytest.fixture(name="fresh_pool")
def fixture_fresh_pool(monkeypatch):
    """Give the test its own pool, created on first use, and shut it down after."""
    # pylint: disable=protected-access
    monkeypatch.setattr(GraphqlWsConsumer, "_workers", None)
    yield monkeypatch
    if GraphqlWsConsumer._workers is not None:
        GraphqlWsConsumer._workers.shutdown(wait=True)


@pytest.mark.asyncio
async def test_pool_default_size_is_the_executor_default(fresh_pool):
    """Without `max_worker_threads` the pool has min(32, CPUs + 4) threads."""
    # pylint: disable=protected-access
    fresh_pool.setattr(GraphqlWsConsumer, "max_worker_threads", None)

    await call_maybe_sync(lambda: None)

    workers = GraphqlWsConsumer._workers
    assert workers._max_workers == min(32, (os.cpu_count() or 1) + 4)


@pytest.mark.asyncio
async def test_max_worker_threads_caps_the_pool(fresh_pool):
    """With `max_worker_threads = 1` two sync callbacks run one after the other."""
    fresh_pool.setattr(GraphqlWsConsumer, "max_worker_threads", 1)
    lock = threading.Lock()
    running = []
    overlaps = []

    def callback():
        with lock:
            running.append(1)
            overlaps.append(len(running))
        threading.Event().wait(0.1)
        with lock:
            running.pop()
        return threading.current_thread().name

    names = await asyncio.gather(call_maybe_sync(callback), call_maybe_sync(callback))

    assert overlaps == [1, 1]
    assert names[0] == names[1]


@pytest.mark.asyncio
async def test_full_pool_queues_notifications_without_dropping_them(gql, fresh_pool):
    """With every thread busy, notifications wait for one and all arrive in order."""
    fresh_pool.setattr(GraphqlWsConsumer, "max_worker_threads", 1)
    client = gql(subscription=Subscription, subprotocol="graphql-transport-ws")
    await client.connect_and_init()
    sub_id = await client.start(query="subscription { on_event(user_id: 1) { value } }")
    await client.assert_no_messages()

    release = threading.Event()
    blocker = asyncio.create_task(call_maybe_sync(release.wait, 5))
    payloads = [f"n{number}" for number in range(5)]
    for payload in payloads:
        await OnEvent.broadcast_async(group="user_1", payload=payload)
    await client.assert_no_messages()
    release.set()
    assert await blocker

    received = [
        (await client.receive_next(sub_id))["data"]["on_event"]["value"]
        for _ in payloads
    ]
    assert received == payloads

    await client.finalize()


REQUEST_ID: contextvars.ContextVar = contextvars.ContextVar("request_id")


@pytest.mark.asyncio
async def test_pool_runs_each_call_in_a_copy_of_the_caller_context():
    """The worker sees the caller's context vars, and its changes stay in it.

    The active Django translation is an asgiref `Local`, which lives in a
    context var, so a sync callback answers in the caller's language.
    """

    def callback():
        seen = (REQUEST_ID.get(None), django.utils.translation.get_language())
        REQUEST_ID.set("changed in the worker")
        return seen

    token = REQUEST_ID.set("from the caller")
    try:
        with django.utils.translation.override("es"):
            seen = await call_maybe_sync(callback)
        assert REQUEST_ID.get() == "from the caller"
    finally:
        REQUEST_ID.reset(token)

    assert seen == ("from the caller", "es")


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


# ------------------------------------------ QUERIES OVER THE WEBSOCKET


@pytest.mark.asyncio
@pytest.mark.parametrize("subprotocol", ["graphql-transport-ws", "graphql-ws"])
async def test_query_over_the_socket_runs_in_the_pool(
    gql, closable_sqlite, subprotocol
):
    """A query sent over the socket runs its ORM resolver in the pool.

    0.9.1 ran the whole operation in the pool. In the event loop thread
    Django raises SynchronousOnlyOperation for the ORM query.
    """
    del closable_sqlite
    Query.threads.clear()
    user_count = await channels.db.database_sync_to_async(User.objects.count)()
    client = gql(
        query=Query,
        subscription=Subscription,
        consumer_attrs={"strict_ordering": True},
        subprotocol=subprotocol,
    )
    await client.connect_and_init()

    op_id = await client.start(query="query { user_count }")
    resp = await client.receive_next(op_id)
    assert resp == {"data": {"user_count": user_count}}
    await client.receive_complete(op_id)
    assert Query.threads["user_count"].startswith("GraphqlWs.")

    await client.finalize()


# --------------------------------------------------- PUBLISH RETURNS NONE


@pytest.mark.asyncio
@pytest.mark.parametrize("subprotocol", ["graphql-transport-ws", "graphql-ws"])
async def test_publish_returning_none_sends_null(gql, subprotocol):
    """A `publish` that returns `None` sends the field as null, as 0.9.1 did.

    1.0.0rc7 dropped such a notification, and the client got nothing.
    """
    client = gql(
        subscription=Subscription,
        consumer_attrs={"strict_ordering": True},
        subprotocol=subprotocol,
    )
    await client.connect_and_init()

    sub_id = await client.start(query="subscription { on_nothing { value } }")
    await client.assert_no_messages()
    await OnNothing.broadcast_async(payload=None)
    resp = await client.receive_next(sub_id)
    assert resp == {"data": {"on_nothing": None}}

    await client.finalize()


@pytest.mark.asyncio
@pytest.mark.parametrize("subprotocol", ["graphql-transport-ws", "graphql-ws"])
async def test_async_publish_returning_skip_sends_nothing(gql, subprotocol):
    """An async `publish` that returns `SKIP` sends nothing, as in 0.9.1."""
    client = gql(
        subscription=Subscription,
        consumer_attrs={"strict_ordering": True},
        subprotocol=subprotocol,
    )
    await client.connect_and_init()

    sub_id = await client.start(query="subscription { on_skipped { value } }")
    await client.assert_no_messages()
    await OnSkipped.broadcast_async(payload="skip")
    await client.assert_no_messages("SKIP sent a notification!")
    await OnSkipped.broadcast_async(payload="send")
    resp = await client.receive_next(sub_id)
    assert resp == {"data": {"on_skipped": {"value": "send"}}}

    await client.finalize()


@pytest.mark.asyncio
@pytest.mark.parametrize("subprotocol", ["graphql-transport-ws", "graphql-ws"])
async def test_publish_exception_is_sent_unlocated(gql, subprotocol):
    """An exception of `publish` goes out as "<class name>: <message>", as in 0.9.1.

    0.9.1 ran `publish` outside the GraphQL execution, so the error had no
    `locations` or `path`.
    """
    client = gql(
        subscription=Subscription,
        consumer_attrs={"strict_ordering": True},
        subprotocol=subprotocol,
    )
    await client.connect_and_init()

    sub_id = await client.start(query="subscription { on_skipped { value } }")
    await client.assert_no_messages()
    await OnSkipped.broadcast_async(payload="fail")
    with pytest.raises(channels_graphql_ws.GraphqlWsResponseError) as exc_info:
        await client.receive_next(sub_id)
    assert exc_info.value.response["payload"] == {
        "data": {"on_skipped": None},
        "errors": [{"message": f"RuntimeError: {OnSkipped.ERROR}"}],
    }

    await client.finalize()


@pytest.mark.asyncio
@pytest.mark.parametrize("subprotocol", ["graphql-transport-ws", "graphql-ws"])
async def test_child_field_error_is_logged_and_not_sent(gql, caplog, subprotocol):
    """A field below `publish` that raises is only logged, as in 0.9.1.

    graphql-core 2 logged it and 0.9.1 sent the notification with the
    field as null and no errors.
    """
    client = gql(
        subscription=Subscription,
        consumer_attrs={"strict_ordering": True},
        subprotocol=subprotocol,
    )
    await client.connect_and_init()

    sub_id = await client.start(query="subscription { on_skipped { value broken } }")
    await client.assert_no_messages()
    await OnSkipped.broadcast_async(payload="ZZZTEST")
    resp = await client.receive_next(sub_id)
    assert resp == {"data": {"on_skipped": {"value": "ZZZTEST", "broken": None}}}

    executor_logs = [
        record
        for record in caplog.records
        if record.name == "graphql.execution.executor"
    ]
    assert [record.getMessage() for record in executor_logs] == [
        "An error occurred while resolving field OnSkipped.broken"
    ]
    assert executor_logs[0].levelname == "ERROR"
    assert executor_logs[0].exc_info[1].args == (OnSkipped.CHILD_ERROR,)

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


class OnUserCount(channels_graphql_ws.Subscription):
    """Subscription whose callbacks query the database."""

    # pylint: disable=arguments-differ

    ERROR = "No access!"

    connections: dict = {}

    count = graphene.Int()

    class Arguments:
        """Subscription arguments."""

        fail = graphene.Boolean(default_value=False)

    @staticmethod
    def subscribe(root, info, fail):
        """Check access with a query, as evapacs-back does."""
        del root, info
        OnUserCount.connections["subscribe"] = django.db.connections["default"]
        User.objects.count()
        if fail:
            raise RuntimeError(OnUserCount.ERROR)
        return []

    @staticmethod
    def publish(payload, info, fail):
        """Send the user count read from the database."""
        del payload, info, fail
        OnUserCount.connections["publish"] = django.db.connections["default"]
        return OnUserCount(count=User.objects.count())


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


class FailingNotificationContext(graphql.ExecutionContext):
    """Execution context that fails, on demand, outside the resolvers."""

    ERROR = "Notification failed!"

    fail = False

    def __init__(self, *args, **kwds):
        """Raise while `fail` is on."""
        if FailingNotificationContext.fail:
            raise RuntimeError(FailingNotificationContext.ERROR)
        super().__init__(*args, **kwds)


class OnSlowSubscribe(channels_graphql_ws.Subscription):
    """Subscription whose `subscribe` waits until the test releases it."""

    # pylint: disable=arguments-differ

    entered = threading.Event()
    release = threading.Event()
    unsubscribed_calls = 0

    value = graphene.String()

    @classmethod
    def reset(cls):
        """Prepare a new run."""
        cls.entered = threading.Event()
        cls.release = threading.Event()
        cls.unsubscribed_calls = 0

    @staticmethod
    def subscribe(root, info):
        """Wait in the pool, as a slow access check does."""
        del root, info
        OnSlowSubscribe.entered.set()
        assert OnSlowSubscribe.release.wait(5), "the test never released subscribe!"
        return []

    @staticmethod
    def publish(payload, info):
        """Send the payload."""
        del info
        return OnSlowSubscribe(value=payload)

    @staticmethod
    def unsubscribed(root, info):
        """Count the calls."""
        del root, info
        OnSlowSubscribe.unsubscribed_calls += 1


class OnNothing(channels_graphql_ws.Subscription):
    """Subscription whose `publish` returns `None`."""

    # pylint: disable=arguments-differ

    value = graphene.String()

    @staticmethod
    def publish(payload, info):
        """Send nothing."""
        del payload, info


class OnSkipped(channels_graphql_ws.Subscription):
    """Subscription whose async `publish` skips "skip" and fails on "fail"."""

    # pylint: disable=arguments-differ

    ERROR = "Publish failed!"
    CHILD_ERROR = "Child failed!"

    value = graphene.String()
    broken = graphene.String()

    @staticmethod
    async def publish(payload, info):
        """Skip "skip", send anything else."""
        del info
        if payload == "skip":
            return OnSkipped.SKIP
        if payload == "fail":
            raise RuntimeError(OnSkipped.ERROR)
        return OnSkipped(value=payload)

    @staticmethod
    def resolve_broken(root, info):
        """Fail, as a child field resolver can."""
        del root, info
        raise RuntimeError(OnSkipped.CHILD_ERROR)


class Subscription(graphene.ObjectType):
    """Root subscription."""

    on_event = OnEvent.Field()
    on_nothing = OnNothing.Field()
    on_skipped = OnSkipped.Field()
    on_failing_subscribe = OnFailingSubscribe.Field()
    on_slow_subscribe = OnSlowSubscribe.Field()
    on_user_count = OnUserCount.Field()


class Query(graphene.ObjectType):
    """Root query."""

    VALUE = "Hi!"
    ERROR = "Resolver failed!"

    threads: dict = {}

    value = graphene.String()
    user_count = graphene.Int()
    failing = graphene.String(text=graphene.String())

    @staticmethod
    def resolve_value(root, info):
        """Return a constant."""
        del root, info
        return Query.VALUE

    @staticmethod
    def resolve_user_count(root, info):
        """Record the thread and count the users with the ORM."""
        del root, info
        Query.threads["user_count"] = threading.current_thread().name
        return User.objects.count()

    @staticmethod
    def resolve_failing(root, info, text):
        """Fail, as a resolver that rejects its input does."""
        del root, info, text
        raise RuntimeError(Query.ERROR)
