"""Waiter lifetime is independent of shared execution and actor ownership."""

import asyncio
import gc
import weakref
from concurrent.futures import CancelledError
from contextvars import ContextVar
from threading import Event, Thread
from threading import enumerate as threads
from types import SimpleNamespace

import pytest
from test_handles import start

import graph_sail as api
from graph_sail._completion import _await_snapshot, _CompletionHub, _Snapshot


@pytest.fixture(autouse=True)
def no_owned_threads_left():
    before = {thread.ident for thread in threads() if thread.name.startswith("graph-sail")}
    yield
    assert {thread.ident for thread in threads() if thread.name.startswith("graph-sail")} == before


def test_actor_waiter_cancel_is_local_and_completed_value_is_borrowed():
    call = api.ActorCall(1, "read")
    value = [1, 2]

    async def scenario():
        waiting = asyncio.create_task(call.result_async())
        await asyncio.sleep(0)
        waiting.cancel("only-this-waiter")
        with pytest.raises(asyncio.CancelledError, match="only-this-waiter"):
            await waiting
        assert not call.cancelled() and not call.done()
        call._set_result(value)
        assert await call.result_async() is value
        assert call.result() is value

    asyncio.run(scenario())


def test_actor_async_failure_reads_do_not_grow_source_traceback():
    call = api.ActorCall(1, "read")
    source = ValueError("original failure")
    call._future.set_exception(source)
    original_traceback = source.__traceback__

    async def scenario():
        observed = []
        for _ in range(20):
            with pytest.raises(api.AsyncSourceError) as caught:
                await call.result_async()
            observed.append(caught.value)
            assert caught.value.__cause__ is source
            assert source.__traceback__ is original_traceback
        assert len({id(error) for error in observed}) == 20

    asyncio.run(scenario())


def test_awaiter_cap_and_repeated_cancel_release_subscriptions():
    call = api.ActorCall(1, "read")

    async def scenario():
        tasks = [asyncio.create_task(call.result_async()) for _ in range(256)]
        try:
            await asyncio.sleep(0)
            with pytest.raises(api.AsyncWaitLimitError):
                await call.result_async()
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        assert call._completion.waiter_count == 0
        for _ in range(50):
            task = asyncio.create_task(call.result_async())
            await asyncio.sleep(0)
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            assert call._completion.waiter_count == 0
        assert not call.cancelled()

    asyncio.run(scenario())


def test_partial_graph_wait_does_not_block_event_loop_or_cancel_slow_branch():
    entered, release = Event(), Event()

    def slow(_):
        entered.set()
        assert release.wait(10)
        return 9

    handle = start({"fast": lambda _: 3, "slow": slow})
    try:
        assert entered.wait(5)

        async def scenario():
            assert await handle.node("fast").result_async(5) == 3
            assert await handle.wait_async(
                ("slow", "fast"), count=2, timeout=0.01
            ) == api.WaitResult(("fast",), ("slow",))
            with pytest.raises(TimeoutError):
                await handle.node("slow").result_async(0)
            assert not handle._stop.is_set()
            release.set()
            assert (await handle.result_async(5)).outputs == {"fast": 3, "slow": 9}
            assert not handle.closed

        asyncio.run(scenario())
    finally:
        release.set()
        handle.close()
    assert handle.closed


@pytest.mark.parametrize("method", ["actor", "node", "execution", "wait"])
@pytest.mark.parametrize(
    "timeout", [True, False, -1, 86_401, float("nan"), float("inf"), 10**400, "1"]
)
def test_async_timeout_validation_precedes_ready_fast_path(method, timeout):
    call = api.ActorCall(1, "read")
    call._set_result(3)
    with start({"a": lambda _: 3}) as handle:
        handle.result(5)

        async def scenario():
            operation = {
                "actor": lambda: call.result_async(timeout),
                "node": lambda: handle.node("a").result_async(timeout),
                "execution": lambda: handle.result_async(timeout),
                "wait": lambda: handle.wait_async(timeout=timeout),
            }[method]
            with pytest.raises(api.ValidationError):
                await operation()
            assert handle._completion.waiter_count == 0

        asyncio.run(scenario())


@pytest.mark.parametrize(
    "source",
    [
        ValueError("private"),
        StopIteration("source iterator"),
        KeyboardInterrupt("stop"),
        SystemExit(3),
        asyncio.CancelledError("source only"),
    ],
)
def test_repeated_actor_source_failures_have_fresh_wrappers_and_preserve_traceback(source):
    try:
        raise source
    except BaseException:
        pass
    original = source.__traceback__
    call = api.ActorCall(1, "read")

    async def scenario():
        first = asyncio.create_task(call.result_async())
        await asyncio.sleep(0)
        call._set_exception(source)
        errors = []
        expected = (
            api.AsyncSourceError if isinstance(source, Exception) else api.AsyncSourceControlError
        )
        for index in range(30):
            with pytest.raises(expected) as caught:
                await (first if index == 0 else call.result_async())
            assert caught.value.__cause__ is source
            assert source.__traceback__ is original
            errors.append(caught.value)
        assert len({id(error) for error in errors}) == 30
        assert call._completion.waiter_count == 0

    asyncio.run(scenario())


def test_explicit_actor_request_cancel_is_distinct_from_waiter_cancel():
    call = api.ActorCall(1, "read")

    async def scenario():
        waiting = asyncio.create_task(call.result_async())
        await asyncio.sleep(0)
        assert call.cancel()
        assert call.cancel()  # Preserve concurrent Future's existing repeat semantics.
        with pytest.raises(api.AsyncSourceCancelledError):
            await waiting
        with pytest.raises(api.AsyncSourceCancelledError):
            await call.result_async(0)
        assert call._completion.epoch == 1

    asyncio.run(scenario())
    with pytest.raises(CancelledError):
        call.result()


def test_running_actor_request_cannot_be_cancelled_by_explicit_queue_cancel():
    call = api.ActorCall(1, "read")
    assert call._future.set_running_or_notify_cancel()
    assert not call.cancel()
    assert call._completion.epoch == 0
    call._set_result(None)
    assert asyncio.run(call.result_async(0)) is None


def test_timeout_and_cancel_churn_adds_no_threads_tasks_or_source_callbacks():
    call = api.ActorCall(1, "read")
    before = {thread.ident for thread in threads()}

    async def scenario():
        loop = asyncio.get_running_loop()
        assert loop._default_executor is None
        current = asyncio.current_task()
        for _ in range(30):
            with pytest.raises(TimeoutError):
                await call.result_async(0.001)
            assert call._completion.waiter_count == call._completion.loop_count == 0
        for _ in range(300):
            waiting = asyncio.create_task(call.result_async())
            await asyncio.sleep(0)
            waiting.cancel("local")
            with pytest.raises(asyncio.CancelledError, match="local"):
                await waiting
        assert asyncio.all_tasks() == {current}
        assert call._future._done_callbacks == []
        assert loop._default_executor is None

    asyncio.run(scenario())
    assert {thread.ident for thread in threads()} == before
    assert not call.done()


def test_snapshot_epoch_closes_completion_before_subscription_race(monkeypatch):
    call = api.ActorCall(1, "read")
    acquire = call._completion.acquire

    def complete_before_acquire(loop):
        call._set_result(73)
        return acquire(loop)

    monkeypatch.setattr(call._completion, "acquire", complete_before_acquire)
    assert asyncio.run(call.result_async(1)) == 73
    assert call._completion.waiter_count == 0


def test_snapshot_epoch_closes_completion_inside_stale_snapshot_race():
    hub = _CompletionHub()
    calls = 0

    def snapshot():
        nonlocal calls
        calls += 1
        if calls == 1:
            hub.notify(terminal=True)
            return _Snapshot(False)
        return _Snapshot(True, 19)

    assert asyncio.run(_await_snapshot(hub, snapshot, 1)) == 19
    assert calls == 2
    assert hub.waiter_count == 0


def test_many_source_notifications_coalesce_one_loop_pump():
    hub = _CompletionHub()

    async def scenario():
        loop = asyncio.get_running_loop()
        leases = [hub.acquire(loop) for _ in range(20)]
        futures = [loop.create_future() for _ in leases]
        for lease, future in zip(leases, futures, strict=True):
            assert lease.arm(hub.epoch, future)
        for _ in range(100):
            hub.notify()
        assert hub.pending_pumps == 1
        await asyncio.gather(*futures)
        assert hub.pending_pumps == 0
        for lease in leases:
            lease.close()
        assert hub.waiter_count == hub.loop_count == 0

    asyncio.run(scenario())


class PausedLoop:
    """Deterministic posting-only loop double; no task execution or polling."""

    def __init__(self):
        self.callbacks = []
        self.closed = False
        self.reject_post = False

    def is_closed(self):
        return self.closed

    def call_soon_threadsafe(self, callback, *args, context):
        if self.reject_post:
            raise RuntimeError("loop closed during post")
        self.callbacks.append((callback, args, context))

    def pump(self):
        callback, args, context = self.callbacks.pop(0)
        context.run(callback, *args)


def test_pending_empty_loop_slots_are_bounded_and_reusable_after_delivery():
    hub = _CompletionHub()
    loops = [PausedLoop() for _ in range(257)]

    async def scenario():
        actual = asyncio.get_running_loop()
        futures = []
        for loop in loops[:256]:
            lease = hub.acquire(loop)
            future = actual.create_future()
            futures.append(future)
            assert lease.arm(hub.epoch, future)
            hub.notify()
            lease.close()
        assert hub.waiter_count == 0 and hub.loop_count == hub.pending_pumps == 256
        with pytest.raises(api.AsyncWaitLimitError, match="loop slots"):
            hub.acquire(loops[-1])
        # The same paused loop reuses its one pending callback across churn.
        for _ in range(300):
            lease = hub.acquire(loops[0])
            lease.arm(hub.epoch, futures[0])
            hub.notify()
            lease.close()
        assert len(loops[0].callbacks) == 1
        loops[0].pump()
        lease = hub.acquire(loops[-1])
        lease.close()
        for loop in loops[1:256]:
            loop.pump()
        assert hub.loop_count == hub.pending_pumps == 0

    asyncio.run(scenario())


def test_posting_has_empty_context_and_no_strong_loop_or_task_ownership():
    hub = _CompletionHub()
    loop = PausedLoop()
    marker = ContextVar("application_secret", default="empty")

    async def scenario(active_loop):
        lease = hub.acquire(active_loop)
        future = asyncio.get_running_loop().create_future()
        lease.arm(hub.epoch, future)
        token = marker.set("not-retained")
        try:
            hub.notify()
        finally:
            marker.reset(token)
        context = active_loop.callbacks[0][2]
        assert list(context.items()) == []
        assert context.run(marker.get) == "empty"
        reference = weakref.ref(future)
        del future
        gc.collect()
        assert reference() is None
        active_loop.pump()
        lease.close()

    asyncio.run(scenario(loop))
    lease = hub.acquire(loop)
    reference = weakref.ref(loop)
    del loop
    gc.collect()
    assert reference() is None
    assert hub.waiter_count == hub.loop_count == 0
    lease.close()


@pytest.mark.parametrize("race", ["before_post", "after_post", "expired_hub"])
def test_loop_close_or_source_collection_does_not_poison_other_waiters(race):
    hub = _CompletionHub()
    loop = PausedLoop()

    async def scenario():
        lease = hub.acquire(loop)
        future = asyncio.get_running_loop().create_future()
        lease.arm(hub.epoch, future)
        if race == "before_post":
            loop.reject_post = True
        hub.notify()
        if race == "after_post":
            loop.closed = True
        if race != "expired_hub":
            assert hub.waiter_count == hub.loop_count == 0
            with pytest.raises(RuntimeError, match="closed event-loop"):
                lease.arm(hub.epoch, future)
        lease.close()
        future.cancel()

    asyncio.run(scenario())
    if race == "expired_hub":
        reference = weakref.ref(hub)
        del hub
        gc.collect()
        assert reference() is None
        loop.pump()
    elif race == "after_post":
        loop.pump()  # Abandoned callback identity cannot disturb a new slot.


def test_completion_from_other_thread_wakes_two_real_event_loops():
    call = api.ActorCall(1, "read")
    armed = [Event(), Event()]
    outcomes, errors = [], []
    value = {"shared": 11}

    def worker(index):
        async def scenario():
            waiting = asyncio.create_task(call.result_async(5))
            await asyncio.sleep(0)
            armed[index].set()
            outcomes.append(await waiting)

        try:
            asyncio.run(scenario())
        except BaseException as error:
            errors.append(error)

    workers = [Thread(target=worker, args=(index,)) for index in range(2)]
    try:
        for worker_thread in workers:
            worker_thread.start()
        assert all(event.wait(5) for event in armed)
        assert call._completion.loop_count == 2
        call._set_result(value)
    finally:
        if not call.done():
            call.cancel()
        for worker_thread in workers:
            worker_thread.join(10)
    assert not any(worker_thread.is_alive() for worker_thread in workers)
    assert not errors and len(outcomes) == 2
    assert all(result is value for result in outcomes)
    assert call._completion.waiter_count == call._completion.loop_count == 0


def test_closed_real_loop_pending_callback_is_pruned_without_source_cancel():
    hub = _CompletionHub()
    loop = asyncio.new_event_loop()
    try:
        lease = hub.acquire(loop)
        future = loop.create_future()
        lease.arm(hub.epoch, future)
        hub.notify()
        assert hub.pending_pumps == 1
    finally:
        loop.close()
    assert hub.waiter_count == hub.loop_count == 0
    lease.close()
    future.cancel()


def idle_handle(nodes=("a", "b")):
    """Unit-level controller state, with no scheduler or OS thread started."""
    return api.ExecutionHandle(SimpleNamespace(order=nodes), lambda: None, Event())


def test_all_node_and_execution_waits_share_one_capacity():
    handle = idle_handle()

    async def scenario():
        methods = (
            lambda: handle.result_async(),
            lambda: handle.node("a").result_async(),
            lambda: handle.node("b").execution_async(),
            lambda: handle.wait_async(),
        )
        tasks = [asyncio.create_task(methods[index % 4]()) for index in range(256)]
        try:
            await asyncio.sleep(0)
            assert handle._completion.waiter_count == 256
            for method in methods:
                with pytest.raises(api.AsyncWaitLimitError):
                    await method()
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        assert handle._completion.waiter_count == handle._completion.loop_count == 0
        assert not handle._stop.is_set()

    asyncio.run(scenario())


@pytest.mark.parametrize("status", ["succeeded", "failed", "skipped", "cancelled"])
def test_async_node_metadata_and_wait_readiness_do_not_confuse_success(status):
    handle = idle_handle()
    terminal = api.TaskExecution("b", "cpu", status, (), "test outcome")
    value = [2]

    async def scenario():
        metadata = asyncio.create_task(handle.node("b").execution_async(1))
        await asyncio.sleep(0)
        handle._publish(terminal, value)
        assert await metadata is terminal
        assert await handle.wait_async(("b", "a"), timeout=0) == api.WaitResult(("b",), ("a",))
        assert await handle.wait_async((), count=0, timeout=0) == api.WaitResult((), ())
        if status == "succeeded":
            assert await handle.node("b").result_async(0) is value
        else:
            errors = []
            for _ in range(3):
                with pytest.raises(api.TaskNotSuccessful) as caught:
                    await handle.node("b").result_async(0)
                assert caught.value.execution is terminal
                errors.append(caught.value)
            assert len({id(error) for error in errors}) == 3
        assert handle._completion.waiter_count == 0

    asyncio.run(scenario())


@pytest.mark.parametrize("control", [False, True])
def test_async_controller_failure_does_not_accumulate_source_traceback(control):
    handle = idle_handle()
    source = (
        KeyboardInterrupt("controller stopped") if control else RuntimeError("controller failed")
    )

    def explode():
        raise source

    async def scenario():
        waiting = asyncio.create_task(handle.result_async(1))
        await asyncio.sleep(0)
        handle._drive(explode)
        traceback = source.__traceback__
        wrapper = api.AsyncSourceControlError if control else api.AsyncSourceError
        with pytest.raises(wrapper) as caught:
            await waiting
        assert caught.value.__cause__ is source
        methods = (
            lambda: handle.result_async(0),
            lambda: handle.node("a").result_async(0),
            lambda: handle.node("a").execution_async(0),
            lambda: handle.wait_async(count=2, timeout=0),
        )
        observed = []
        for _ in range(10):
            for method in methods:
                with pytest.raises(wrapper) as caught:
                    await method()
                assert caught.value.__cause__ is source
                assert source.__traceback__ is traceback
                observed.append(caught.value)
        assert len({id(error) for error in observed}) == 40
        assert await handle.wait_async(count=0) == api.WaitResult((), ("a", "b"))
        assert handle._completion.waiter_count == 0

    asyncio.run(scenario())


@pytest.mark.parametrize("method", ["result", "node"])
def test_invalid_finished_controller_state_is_never_fabricated_success(method):
    handle = idle_handle()
    handle._drive(lambda: None)

    async def scenario():
        with pytest.raises(api.AsyncSourceError) as caught:
            await (handle.result_async() if method == "result" else handle.node("a").result_async())
        assert isinstance(caught.value.__cause__, RuntimeError)
        assert "no result" in str(caught.value.__cause__) or "no terminal" in str(
            caught.value.__cause__
        )

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "selection,count",
    [
        (["a"], 1),
        (("a", "a"), 1),
        (("missing",), 1),
        ((True,), 1),
        (("a",), True),
        (("a",), -1),
        (("a",), 2),
        ((), 1),
    ],
)
def test_wait_async_reuses_exact_selection_and_count_admission(selection, count):
    handle = idle_handle()
    with pytest.raises(api.ValidationError):
        asyncio.run(handle.wait_async(selection, count=count))
    assert handle._completion.waiter_count == 0


def test_manually_constructed_invalid_node_handle_revalidates_selection():
    handle = idle_handle()
    node = api.NodeHandle(handle, "missing")
    with pytest.raises(api.ValidationError):
        asyncio.run(node.execution_async())


def test_wait_deadline_returns_latest_partial_snapshot_after_nonterminal_wake():
    handle = idle_handle()

    async def scenario():
        task = asyncio.create_task(handle.wait_async(count=2, timeout=0.01))
        await asyncio.sleep(0)
        handle._publish(api.TaskExecution("a", "cpu", "failed", (), "known failure"), None)
        assert await task == api.WaitResult(("a",), ("b",))
        assert not handle._stop.is_set()
        assert handle._completion.waiter_count == 0

    asyncio.run(scenario())


def test_result_at_deadline_is_observed_without_discarding_terminal_value(monkeypatch):
    import graph_sail._completion as module

    call = api.ActorCall(1, "read")
    original = module.asyncio.timeout_at

    class CompleteOnTimeoutExit:
        async def __aenter__(self):
            self.context = original(asyncio.get_running_loop().time())
            return await self.context.__aenter__()

        async def __aexit__(self, *args):
            call._set_result(42)
            return await self.context.__aexit__(*args)

    monkeypatch.setattr(module.asyncio, "timeout_at", lambda _: CompleteOnTimeoutExit())
    assert asyncio.run(call.result_async(1)) == 42


def test_partial_deadline_while_snapshot_remains_pending_releases_capacity(monkeypatch):
    import graph_sail._completion as module

    hub = _CompletionHub()
    snapshots = 0

    def snapshot():
        nonlocal snapshots
        snapshots += 1
        return _Snapshot(False, snapshots)

    class ImmediateTimeout:
        async def __aenter__(self):
            raise TimeoutError

        async def __aexit__(self, *_):
            return False

    monkeypatch.setattr(module.asyncio, "timeout_at", lambda _: ImmediateTimeout())
    assert asyncio.run(_await_snapshot(hub, snapshot, 1, partial=True)) == 2
    assert hub.waiter_count == hub.loop_count == 0


def test_closed_slot_pruning_and_reused_token_do_not_accept_stale_release():
    hub = _CompletionHub()
    old_loop, new_loop = PausedLoop(), PausedLoop()
    old = hub.acquire(old_loop)
    old_loop.closed = True
    new = hub.acquire(new_loop)
    assert old.token == new.token
    old.close()
    assert hub.waiter_count == 1
    new.close()
    new.close()
    assert hub.waiter_count == hub.loop_count == 0


@pytest.mark.parametrize("boundary", ["snapshot", "acquire", "arm"])
def test_real_thread_completion_during_admission_cannot_lose_wakeup(monkeypatch, boundary):
    call = api.ActorCall(1, "read")
    at_boundary, completed = Event(), Event()
    errors = []

    def producer():
        try:
            assert at_boundary.wait(5)
            call._set_result(51)
        except BaseException as error:
            errors.append(error)
        finally:
            completed.set()

    def rendezvous():
        at_boundary.set()
        assert completed.wait(5)

    if boundary == "snapshot":
        original = call._async_snapshot

        def snapshot():
            observed = original()
            if not observed.ready:
                rendezvous()
            return observed

        monkeypatch.setattr(call, "_async_snapshot", snapshot)
    elif boundary == "acquire":
        original = call._completion.acquire

        def acquire(loop):
            rendezvous()
            return original(loop)

        monkeypatch.setattr(call._completion, "acquire", acquire)
    else:
        original = call._completion.arm

        def arm(*args):
            rendezvous()
            return original(*args)

        monkeypatch.setattr(call._completion, "arm", arm)
    worker = Thread(target=producer)
    worker.start()
    try:
        assert asyncio.run(call.result_async(1)) == 51
    finally:
        at_boundary.set()
        worker.join(10)
    assert not worker.is_alive() and not errors
    assert call._completion.waiter_count == 0


def test_completed_source_is_readable_before_its_delayed_notification():
    call = api.ActorCall(1, "read")

    async def scenario():
        waiting = asyncio.create_task(call.result_async(1))
        await asyncio.sleep(0)
        call._future.set_result(23)  # Freeze the exact state-publication/notify gap.
        assert await call.result_async(0) == 23
        assert not waiting.done() and call._completion.waiter_count == 1
        call._completion.notify(terminal=True)
        assert await waiting == 23
        assert call._completion.waiter_count == 0

    asyncio.run(scenario())


def test_final_notification_overtaking_node_notification_preserves_terminal(monkeypatch):
    handle = idle_handle()
    published, release = Event(), Event()
    original = handle._completion.notify
    errors = []

    def notify(*, terminal=False):
        if not terminal:
            published.set()
            assert release.wait(5)
        original(terminal=terminal)

    monkeypatch.setattr(handle._completion, "notify", notify)
    terminal = api.TaskExecution("a", "cpu", "succeeded", (), "finished")

    def producer():
        try:
            handle._publish(terminal, 31)
        except BaseException as error:
            errors.append(error)

    worker = Thread(target=producer)

    async def scenario():
        waiting = asyncio.create_task(handle.node("a").result_async(1))
        await asyncio.sleep(0)
        worker.start()
        assert published.wait(5)
        handle._drive(lambda: "final-description")
        assert await waiting == 31
        assert await handle.node("a").execution_async() is terminal
        assert await handle.result_async() == "final-description"

    try:
        asyncio.run(scenario())
    finally:
        release.set()
        worker.join(10)
    assert not worker.is_alive() and not errors
    assert handle._completion.epoch == 1 and handle._completion.waiter_count == 0


def test_immediate_ready_reads_never_acquire_a_waiter_slot(monkeypatch):
    call = api.ActorCall(1, "read")
    call._set_result(5)
    handle = idle_handle(("a",))
    handle._publish(api.TaskExecution("a", "cpu", "succeeded", (), "finished"), 7)
    handle._drive(lambda: "final-description")

    def forbidden(_):
        pytest.fail("immediate result attempted to acquire capacity")

    monkeypatch.setattr(call._completion, "acquire", forbidden)
    monkeypatch.setattr(handle._completion, "acquire", forbidden)

    async def scenario():
        for _ in range(300):
            assert await call.result_async(0) == 5
            assert await handle.node("a").result_async(0) == 7
            assert (await handle.node("a").execution_async()).status == "succeeded"
            assert await handle.wait_async() == api.WaitResult(("a",), ())
            assert await handle.result_async(0) == "final-description"

    asyncio.run(scenario())


@pytest.mark.parametrize("error", [KeyboardInterrupt("post"), SystemExit(3)])
def test_posting_control_exceptions_are_not_swallowed(monkeypatch, error):
    hub = _CompletionHub()
    loop = PausedLoop()

    def interrupted(*args, **kwargs):
        raise error

    monkeypatch.setattr(loop, "call_soon_threadsafe", interrupted)

    async def scenario():
        lease = hub.acquire(loop)
        future = asyncio.get_running_loop().create_future()
        lease.arm(hub.epoch, future)
        try:
            with pytest.raises(type(error)) as caught:
                hub.notify()
            assert caught.value is error
        finally:
            loop.closed = True
            lease.close()
            future.cancel()
        assert hub.waiter_count == hub.loop_count == 0

    asyncio.run(scenario())


def test_wait_zero_deadline_returns_empty_snapshot_without_subscription():
    handle = idle_handle()
    assert asyncio.run(handle.wait_async(count=2, timeout=0)) == api.WaitResult((), ("a", "b"))
    assert handle._completion.waiter_count == 0


def test_delivery_does_not_arm_another_in_progress_admission():
    hub = _CompletionHub()

    async def scenario():
        loop = asyncio.get_running_loop()
        armed, not_yet_armed = hub.acquire(loop), hub.acquire(loop)
        future = loop.create_future()
        armed.arm(hub.epoch, future)
        hub.notify()
        await future
        assert not_yet_armed.waiter.future is None
        armed.close()
        not_yet_armed.close()
        assert hub.waiter_count == 0

    asyncio.run(scenario())


def test_loop_closing_and_pruning_between_notification_and_failed_post(monkeypatch):
    hub = _CompletionHub()
    loop = PausedLoop()

    def close_during_post(*args, **kwargs):
        loop.closed = True
        assert hub.waiter_count == 0  # Concurrent observer already pruned the slot.
        raise RuntimeError("closed during post")

    monkeypatch.setattr(loop, "call_soon_threadsafe", close_during_post)

    async def scenario():
        lease = hub.acquire(loop)
        future = asyncio.get_running_loop().create_future()
        lease.arm(hub.epoch, future)
        hub.notify()
        lease.close()
        future.cancel()
        assert hub.loop_count == 0

    asyncio.run(scenario())


def test_second_loop_can_be_collected_between_notify_snapshot_and_post(monkeypatch):
    hub = _CompletionHub()
    loops = [PausedLoop(), PausedLoop()]
    post = loops[0].call_soon_threadsafe
    reference = weakref.ref(loops[1])

    def collect_second(*args, **kwargs):
        loops.pop()
        assert reference() is None
        return post(*args, **kwargs)

    monkeypatch.setattr(loops[0], "call_soon_threadsafe", collect_second)

    async def scenario():
        actual = asyncio.get_running_loop()
        first, second = hub.acquire(loops[0]), hub.acquire(loops[1])
        futures = [actual.create_future(), actual.create_future()]
        first.arm(hub.epoch, futures[0])
        second.arm(hub.epoch, futures[1])
        hub.notify()
        assert hub.waiter_count == 1
        loops[0].pump()
        assert futures[0].done() and not futures[1].done()
        first.close()
        second.close()
        futures[1].cancel()
        assert hub.loop_count == 0

    asyncio.run(scenario())


def test_stdlib_wait_for_timeout_does_not_cancel_the_shared_actor_source():
    call = api.ActorCall(1, "read")

    async def scenario():
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(call.result_async(), timeout=0.001)
        assert not call.done() and call._completion.waiter_count == 0
        call._set_result(11)
        assert await call.result_async() == 11

    asyncio.run(scenario())
