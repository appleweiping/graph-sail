"""Exact new-profile wire and private sequence ownership, no fake process claims."""

import inspect
import os
import pickle

import pytest

import graph_sail as gs
from graph_sail.actor_streams import _PROFILE, _ActorSession, _profile_startup
from tests.actor_stream_functions import Counter, InvalidStream


class NeverCancelled:
    def poll(self, timeout=0):
        return False

    def close(self):
        pass


def session():
    instance = Counter()
    return instance, _ActorSession(instance, ("values", "illegal"), NeverCancelled(), 1024)


def send(worker, request, operation, payload, stream=1):
    return pickle.loads(worker.dispatch((request, _PROFILE, stream, operation, payload)))


@pytest.mark.parametrize("failure", ["pickle", "large", "source"])
def test_failed_second_yield_keeps_exact_finish_sequence(failure):
    instance, worker = session()
    pid = os.getpid()
    assert send(worker, 1, "open", ("values", (3, failure), {}, 10)) == (
        1,
        "ok",
        ("opened", pid, 1, 0),
    )
    assert send(worker, 2, "advance", 0) == (2, "ok", ("yield", pid, 1, 0, 1))
    failed = send(worker, 3, "advance", 1)
    assert failed[:2] == (3, "ok")
    assert failed[2][:4] == ("error", pid, 1, 1)
    assert failed[2][4] == ("advance" if failure == "source" else "serialization")
    assert instance.value == (102 if failure == "source" else 2)
    with pytest.raises(gs.ActorDiedError):
        send(worker, 4, "advance", 1)
    assert send(worker, 5, "finish", 1) == (5, "ok", ("closed", pid, 1, 1))
    assert instance.state()[:3] == (102, 1, 1)
    assert send(worker, 6, "finish", 1)[2] == ("closed", pid, 1, 1)
    assert instance.closed == 1


def test_cap_no_peek_open_error_eof_and_nonreused_generation():
    instance, worker = session()
    send(worker, 1, "open", ("values", (1,), {}, 1))
    send(worker, 2, "advance", 0)
    with pytest.raises(gs.ActorDiedError):
        send(worker, 3, "advance", 1)
    send(worker, 4, "finish", 1)
    assert instance.value == 101
    with pytest.raises(gs.ActorDiedError):
        send(worker, 5, "open", ("values", (), {}, 1))
    assert send(worker, 6, "open", ("values", (), {"wrong": 1}, 1), stream=2)[2][4] == "open"
    assert send(worker, 7, "finish", 0, stream=2)[2][0] == "closed"
    send(worker, 8, "open", ("values", (0,), {}, 2), stream=3)
    assert send(worker, 9, "advance", 0, stream=3)[2] == ("eof", os.getpid(), 3, 0)
    send(worker, 10, "finish", 0, stream=3)
    assert instance.closed == 2


@pytest.mark.parametrize(
    "frame",
    [
        None,
        (),
        (True, _PROFILE, 1, "open", ()),
        (1, "bad", 1, "open", ()),
        (1, _PROFILE, True, "open", ()),
        (1, _PROFILE, 1, "bad", ()),
        (1, _PROFILE, 1, "open", ()),
        (1, _PROFILE, 1, "open", ("bad", (), {}, 1)),
    ],
)
def test_malformed_private_request_fails_closed(frame):
    _, worker = session()
    with pytest.raises(gs.ActorDiedError):
        worker.dispatch(frame)


@pytest.mark.parametrize("sequence", [True, -1, 1, "0", None])
def test_strict_sequence_binding(sequence):
    _, worker = session()
    send(worker, 1, "open", ("values", (), {}, 5))
    try:
        with pytest.raises(gs.ActorDiedError):
            send(worker, 2, "advance", sequence)
    finally:
        worker.shutdown()


def test_illegal_close_has_no_false_receipt_or_second_close():
    _, worker = session()
    send(worker, 1, "open", ("illegal", (), {}, 2))
    send(worker, 2, "advance", 0)
    assert send(worker, 3, "finish", 1)[2][4] == "finish"
    assert inspect.getgeneratorstate(worker._generator) == inspect.GEN_SUSPENDED
    with pytest.raises(gs.ActorDiedError):
        send(worker, 4, "finish", 1)
    # The test directly owns this deliberately illegal local generator.
    worker._generator.close()


@pytest.mark.parametrize("method", ["ordinary", "coroutine", "asynchronous", "static"])
def test_non_native_bound_generator_rejected(method):
    with pytest.raises(TypeError):
        _ActorSession(InvalidStream(), (method,), NeverCancelled(), 1024)


def test_legacy_startup_cannot_select_new_profile():
    with pytest.raises(gs.ActorDiedError):
        _profile_startup((Counter, ("state",), (), {}))
    assert _profile_startup((_PROFILE, Counter, ("state",), ("values",), (), {}))[2] == ("values",)
