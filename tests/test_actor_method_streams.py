"""Behavioral missing-profile REDs and real-spawn integration."""

import pickle
import time

import pytest

import graph_sail as gs
from graph_sail.actors import _pack, _request_fields, _startup_fields
from tests.actor_stream_functions import Counter, LegacyFactory


def actor(**options):
    definition = gs.ActorDefinition(
        Counter,
        ("add", "state", "block"),
        stream_methods=("values", "wait", "illegal", "close_wait", "control"),
    )
    return gs.ProcessActor(gs.ActorRegistry({"counter": definition}), "counter", **options)


def until(predicate):
    deadline = time.monotonic() + 10
    while not predicate():
        if time.monotonic() >= deadline:
            pytest.fail("fixture did not reach its synchronization point")
        time.sleep(0.005)


def test_legacy_wire_is_unchanged():
    startup = (Counter, ("add", "state"), (), {})
    assert _pack(startup, 4096) == pickle.dumps(startup, protocol=5)
    assert _startup_fields(startup) == startup
    request = (1, "add", (2,), {})
    assert _request_fields(request, {"add": Counter().add}) == request
    with pytest.raises(gs.ActorDiedError):
        _startup_fields(("graph-sail.actor-stream.v1", *startup))


def test_legacy_factory_equality_is_never_a_new_profile_hook():
    registry = gs.ActorRegistry({"legacy": gs.ActorDefinition(LegacyFactory, ("state",))})
    with gs.ProcessActor(registry, "legacy") as owner:
        assert owner.submit("state").result(5)[:3] == (0, 0, 1)


def test_same_instance_state_cleanup_and_reuse():
    with actor(args=(10,)) as owner:
        assert owner.submit("add", args=(2,)).result(5) == 12
        with owner.stream("values", args=(3,)) as stream:
            assert [item.value for item in stream] == [13, 14, 15]
            assert stream.completion(5).state.actor_reusable_at_release
        assert owner.submit("state").result(5)[:4] == (115, 1, 1, owner.pid)
        with owner.stream("values", args=(1,)) as stream:
            assert next(stream).value == 116
            assert list(stream) == []
        assert owner.submit("add").result(5) == 216


def test_exact_cap_has_no_peek_and_clean_finish_reuses():
    with actor() as owner:
        with owner.stream("values", args=(4,), config=gs.TaskStreamConfig(max_yields=1)) as stream:
            assert [item.value for item in stream] == [1]
            assert stream.completion(5).status == "limited"
        assert owner.submit("state").result(5)[:3] == (101, 1, 1)


@pytest.mark.parametrize("failure", ["pickle", "large", "source"])
def test_failed_yield_keeps_finish_sequence_and_same_state(failure):
    expected = gs.ActorRemoteError if failure == "source" else gs.ActorSerializationError
    with actor(config=gs.ActorConfig(max_message_bytes=1024)) as owner:
        with owner.stream("values", args=(3, failure)) as stream:
            assert next(stream).value == 1
            with pytest.raises(expected):
                stream.next(5)
            assert stream.state.generator_closed
            assert stream.state.actor_reusable_at_release
        assert owner.submit("state").result(5)[:4] == (102, 1, 1, owner.pid)


def test_positive_finish_deadline_is_distinct_from_zero_shutdown():
    cls = gs.ActorStreamConfig
    assert cls(shutdown_timeout_seconds=0).finish_timeout_seconds == 5
    for invalid in (0, True, float("nan"), float("inf")):
        with pytest.raises(gs.ValidationError):
            cls(finish_timeout_seconds=invalid)


def test_idle_lease_backpressure_and_late_close_cannot_damage_reuse(tmp_path):
    with actor() as owner:
        marker = tmp_path / "advanced"
        stream = owner.stream(
            "values", args=(3, None, str(marker)), config=gs.TaskStreamConfig(max_buffered=1)
        )
        try:
            until(marker.exists)
            with pytest.raises(gs.ActorStreamBusyError):
                owner.submit("add")
            with pytest.raises(gs.ActorStreamBusyError):
                owner.stream("values")
            assert marker.read_text() == "1"
            assert [item.value for item in stream] == [1, 2, 3]
        finally:
            stream.close()
        with owner.stream("values", args=(1,)) as newer:
            stream.close()
            assert not stream.cancel()
            assert [item.value for item in newer] == [104]


def test_failed_driver_start_retains_explicit_cleanup(monkeypatch):
    import graph_sail.task_streams as mailbox

    with actor() as owner:
        original = mailbox.Thread.start

        def fail_start(thread):
            if thread.name == "graph-sail-stream":
                raise KeyboardInterrupt("start boundary")
            return original(thread)

        monkeypatch.setattr(mailbox.Thread, "start", fail_start)
        with pytest.raises(KeyboardInterrupt) as caught:
            owner.stream("values")
        retained = caught.value.actor_stream_cleanup
        retained.close()
        assert retained.closed
        assert owner.submit("add").result(5) == 0
