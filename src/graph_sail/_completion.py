"""Bounded event-loop notifications, not an executor or a source Future bridge.

Only library-created loop pumps run here. Caller cancellation never reaches the
shared source. Runtime loops/futures are trusted implementations, not sandboxes.
"""

from __future__ import annotations

import asyncio
import weakref
from collections.abc import Callable
from contextvars import Context
from dataclasses import dataclass, field
from threading import Lock

from graph_sail.errors import GraphSailError

MAX_ASYNC_WAITERS = 256


class AsyncWaitLimitError(GraphSailError):
    """The shared handle's bounded waiter or loop-slot capacity is occupied."""


class AsyncSourceError(GraphSailError):
    """Fresh async wrapper; __cause__ is the unmodified shared ordinary failure."""


class AsyncSourceControlError(BaseException):
    """Fresh control wrapper; source controls are not ordinary task failures."""


class AsyncSourceCancelledError(GraphSailError):
    """The actor request was explicitly cancelled, not this coroutine's wait."""


@dataclass(frozen=True, slots=True)
class _Snapshot:
    ready: bool
    value: object = None
    failure: BaseException | None = None
    cancelled: bool = False


@dataclass(slots=True)
class _LoopSlot:
    key: int
    loop: weakref.ReferenceType[asyncio.AbstractEventLoop]
    tokens: set[int] = field(default_factory=set)
    posted: bool = False


@dataclass(slots=True)
class _Waiter:
    slot: _LoopSlot
    future: weakref.ReferenceType[asyncio.Future[None]] | None = None


class _CompletionHub:
    """At most 256 live leases and 256 loop slots, including pending empty pumps."""

    def __init__(self) -> None:
        self._lock = Lock()
        self._epoch = 0
        self._terminal = False
        self._loops: dict[int, _LoopSlot] = {}
        self._waiters: dict[int, _Waiter] = {}

    @property
    def epoch(self) -> int:
        with self._lock:
            return self._epoch

    @property
    def waiter_count(self) -> int:
        with self._lock:
            self._prune()
            return len(self._waiters)

    @property
    def loop_count(self) -> int:
        with self._lock:
            self._prune()
            return len(self._loops)

    @property
    def pending_pumps(self) -> int:
        with self._lock:
            return sum(slot.posted for slot in self._loops.values())

    def _discard(self, slot: _LoopSlot) -> None:
        if self._loops.get(slot.key) is slot:
            del self._loops[slot.key]
            for token in slot.tokens:
                self._waiters.pop(token, None)

    def _prune(self) -> None:
        for slot in tuple(self._loops.values()):
            loop = slot.loop()
            if loop is None or loop.is_closed():
                self._discard(slot)

    def acquire(self, loop: asyncio.AbstractEventLoop) -> _Lease:
        with self._lock:
            self._prune()
            if len(self._waiters) >= MAX_ASYNC_WAITERS:
                raise AsyncWaitLimitError("shared handle already has 256 async waiters")
            slot = self._loops.get(id(loop))
            if slot is None:
                if len(self._loops) >= MAX_ASYNC_WAITERS:
                    raise AsyncWaitLimitError("shared handle already has 256 pending loop slots")
                slot = _LoopSlot(id(loop), weakref.ref(loop))
                self._loops[slot.key] = slot
            # Reuse one of a fixed set of token integers; cancellation churn does
            # not create an ever-growing counter or per-source callback list.
            token = next(index for index in range(MAX_ASYNC_WAITERS) if index not in self._waiters)
            waiter = _Waiter(slot)
            self._waiters[token] = waiter
            slot.tokens.add(token)
            return _Lease(self, token, waiter)

    def release(self, token: int, expected: _Waiter) -> None:
        with self._lock:
            if self._waiters.get(token) is not expected:
                return
            del self._waiters[token]
            slot = expected.slot
            slot.tokens.discard(token)
            if not slot.tokens and not slot.posted:
                self._discard(slot)

    def arm(self, token: int, expected: _Waiter, epoch: int, future: asyncio.Future[None]) -> bool:
        with self._lock:
            if self._waiters.get(token) is not expected:
                raise RuntimeError("async waiter lost its closed event-loop subscription")
            if self._epoch != epoch:
                return False
            expected.future = weakref.ref(future)
            return True

    def notify(self, *, terminal: bool = False) -> None:
        scheduled: list[_LoopSlot] = []
        with self._lock:
            if self._terminal:
                return
            self._terminal = terminal
            self._epoch += 1
            self._prune()
            for slot in self._loops.values():
                if not slot.posted and any(
                    self._waiters[token].future is not None for token in slot.tokens
                ):
                    slot.posted = True
                    scheduled.append(slot)
        for slot in scheduled:
            loop = slot.loop()
            if loop is not None:
                try:
                    loop.call_soon_threadsafe(_pump, weakref.ref(self), slot, context=Context())
                    continue
                except RuntimeError:
                    # A loop closed between observation and thread-safe posting.
                    # Its abandoned waiters cannot be resumed off-loop. Other
                    # loops and shared execution retain their own lifetimes.
                    pass
            with self._lock:
                self._discard(slot)

    def deliver(self, slot: _LoopSlot) -> None:
        futures: list[weakref.ReferenceType[asyncio.Future[None]]] = []
        with self._lock:
            if self._loops.get(slot.key) is not slot:
                return
            slot.posted = False
            for token in slot.tokens:
                waiter = self._waiters[token]
                if waiter.future is not None:
                    futures.append(waiter.future)
                    waiter.future = None
            if not slot.tokens:
                self._discard(slot)
        # Only the owning event loop runs this method. No source value/exception
        # is placed on these private notification Futures.
        for reference in futures:
            future = reference()
            if future is not None and not future.done():
                future.set_result(None)


@dataclass(slots=True)
class _Lease:
    hub: _CompletionHub
    token: int
    waiter: _Waiter

    def arm(self, epoch: int, future: asyncio.Future[None]) -> bool:
        return self.hub.arm(self.token, self.waiter, epoch, future)

    def close(self) -> None:
        self.hub.release(self.token, self.waiter)


def _pump(reference: weakref.ReferenceType[_CompletionHub], slot: _LoopSlot) -> None:
    hub = reference()
    if hub is not None:
        hub.deliver(slot)


def _value(snapshot: _Snapshot) -> object:
    if snapshot.cancelled:
        raise AsyncSourceCancelledError("shared actor request was explicitly cancelled")
    if snapshot.failure is not None:
        if isinstance(snapshot.failure, Exception):
            raise AsyncSourceError("shared source failed; inspect __cause__") from snapshot.failure
        raise AsyncSourceControlError(
            "shared source raised a control exception"
        ) from snapshot.failure
    return snapshot.value


async def _await_snapshot(
    hub: _CompletionHub,
    snapshot: Callable[[], _Snapshot],
    timeout: float | None,
    *,
    partial: bool = False,
) -> object:
    """Observe source state without transferring cancellation or close ownership."""
    loop = asyncio.get_running_loop()
    deadline = None if timeout is None else loop.time() + timeout
    lease: _Lease | None = None
    try:
        while True:
            epoch = hub.epoch
            observed = snapshot()
            if observed.ready:
                return _value(observed)
            if deadline is not None and loop.time() >= deadline:
                if partial:
                    return _value(observed)
                raise TimeoutError("async result is not ready")
            if lease is None:
                lease = hub.acquire(loop)
            future: asyncio.Future[None] = loop.create_future()
            if not lease.arm(epoch, future):
                continue
            try:
                if deadline is None:
                    await future
                else:
                    async with asyncio.timeout_at(deadline):
                        await future
            except TimeoutError:
                observed = snapshot()
                if observed.ready or partial:
                    return _value(observed)
                raise TimeoutError("async result is not ready") from None
    finally:
        if lease is not None:
            lease.close()
