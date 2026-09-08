"""Context-owned local immutable byte objects, never persistent pickle.

Small references may travel through trusted local actor pipes. Readers obtain
bounded, independently verified bytes from files, not zero-copy shared memory.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import struct
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from threading import RLock
from typing import BinaryIO
from uuid import uuid4

from .errors import GraphSailError, ValidationError

_MAX_OBJECT = 64 * 1024 * 1024
_MAX_STORE = 1024 * 1024 * 1024
_HEADER = struct.Struct("<8s16s16sQ32s")
_MARKER = struct.Struct("<8s16s")


class ObjectStoreError(GraphSailError):
    """Local object storage or cleanup failed; inspect stats before retrying."""


class ObjectUnavailableError(ObjectStoreError):
    """The owner/store/object has closed, been released, or is absent."""


class ObjectIntegrityError(ObjectStoreError):
    """A reference, file identity, header or content commitment does not agree."""


class ObjectCapacityError(ObjectStoreError):
    """An object, store or read exceeds its independent resource budget."""


def _count(value: object, name: str, maximum: int, minimum: int = 0) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValidationError(f"{name} must be an integer in {minimum}..{maximum}")
    return value


def _hex(value: object, name: str, length: int) -> str:
    if type(value) is not str or re.fullmatch(rf"[0-9a-f]{{{length}}}", value) is None:
        raise ValidationError(f"{name} must be {length} lowercase hexadecimal characters")
    return value


@dataclass(frozen=True, slots=True)
class ObjectRef:
    store_id: str
    object_id: str
    sha256: str
    size_bytes: int

    def __post_init__(self) -> None:
        _hex(self.store_id, "store_id", 32)
        _hex(self.object_id, "object_id", 32)
        _hex(self.sha256, "sha256", 64)
        _count(self.size_bytes, "size_bytes", _MAX_OBJECT)

    def to_dict(self) -> dict[str, str | int]:
        return {
            "kind": "graph-sail-byte-object",
            "schema_version": "1.0",
            "store_id": self.store_id,
            "object_id": self.object_id,
            "sha256": self.sha256,
            "size_bytes": self.size_bytes,
        }

    def to_json(self) -> str:
        return json.dumps(_reference(self).to_dict(), sort_keys=True, separators=(",", ":"))

    @classmethod
    def from_json(cls, value: str | bytes) -> ObjectRef:
        """Strict, duplicate-rejecting reference transport, at most 1 KiB UTF-8."""
        if type(value) not in (str, bytes) or len(value) > 1024:
            raise ValidationError("object reference JSON must be at most 1024 UTF-8 bytes")
        try:
            data = value.encode("utf-8") if isinstance(value, str) else value
            if len(data) > 1024:
                raise ValidationError("object reference JSON must be at most 1024 UTF-8 bytes")
            return cls.from_dict(
                json.loads(
                    data.decode("utf-8"),
                    object_pairs_hook=_json_pairs,
                    parse_int=_json_integer,
                    parse_float=_invalid_number,
                    parse_constant=_invalid_number,
                )
            )
        except (ValueError, RecursionError) as exc:
            raise ValidationError("invalid object reference JSON") from exc

    @classmethod
    def from_dict(cls, value: object) -> ObjectRef:
        names = {"kind", "schema_version", "store_id", "object_id", "sha256", "size_bytes"}
        if (
            type(value) is not dict
            or set(value) != names
            or type(value["kind"]) is not str
            or type(value["schema_version"]) is not str
            or value["kind"] != "graph-sail-byte-object"
            or value["schema_version"] != "1.0"
        ):
            raise ValidationError("object reference requires its exact versioned fields")
        return cls(value["store_id"], value["object_id"], value["sha256"], value["size_bytes"])


def _json_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValidationError("object reference JSON contains duplicate fields")
        result[key] = value
    return result


def _json_integer(value: str) -> int:
    if len(value) > 10:
        raise ValidationError("object reference integer is out of bounds")
    return int(value)


def _invalid_number(value: str) -> int:
    raise ValidationError("object reference JSON cannot contain floats or nonfinite numbers")


def _reference(value: ObjectRef) -> ObjectRef:
    if type(value) is not ObjectRef:
        raise ValidationError("reference must be ObjectRef")
    return ObjectRef(value.store_id, value.object_id, value.sha256, value.size_bytes)


@dataclass(frozen=True, slots=True)
class ObjectStoreConfig:
    max_object_bytes: int = _MAX_OBJECT
    max_store_bytes: int = _MAX_STORE
    max_objects: int = 1024

    def __post_init__(self) -> None:
        _count(self.max_object_bytes, "max_object_bytes", _MAX_OBJECT)
        _count(self.max_store_bytes, "max_store_bytes", _MAX_STORE, _HEADER.size)
        _count(self.max_objects, "max_objects", 1024, 1)


@dataclass(frozen=True, slots=True)
class ObjectStoreStats:
    active_objects: int
    active_file_bytes: int
    pending_cleanup_objects: int
    pending_cleanup_reserved_bytes: int
    reserved_bytes: int
    closed: bool

    def __post_init__(self) -> None:
        for name in ("active_objects", "pending_cleanup_objects"):
            _count(getattr(self, name), name, 1024)
        for name in ("active_file_bytes", "pending_cleanup_reserved_bytes", "reserved_bytes"):
            _count(getattr(self, name), name, _MAX_STORE)
        if (
            type(self.closed) is not bool
            or self.active_objects + self.pending_cleanup_objects > 1024
            or self.active_file_bytes + self.pending_cleanup_reserved_bytes != self.reserved_bytes
            or (
                self.closed
                and (self.active_objects or self.pending_cleanup_objects or self.reserved_bytes)
            )
            or self.active_file_bytes < self.active_objects * _HEADER.size
            or self.pending_cleanup_reserved_bytes < self.pending_cleanup_objects * _HEADER.size
            or self.active_file_bytes > self.active_objects * (_HEADER.size + _MAX_OBJECT)
            or self.pending_cleanup_reserved_bytes
            > self.pending_cleanup_objects * (_HEADER.size + _MAX_OBJECT)
            or bool(self.active_objects) != bool(self.active_file_bytes)
            or bool(self.pending_cleanup_objects) != bool(self.pending_cleanup_reserved_bytes)
        ):
            raise ValidationError("object store statistics have inconsistent fields")

    def to_dict(self) -> dict[str, int | bool]:
        return {name: getattr(self, name) for name in self.__dataclass_fields__}


def _absolute(value: str | Path) -> Path:
    if not isinstance(value, (str, Path)) or not str(value) or "\x00" in str(value):
        raise ValidationError("object-store location must be a nonempty filesystem path")
    try:
        return Path(value).absolute()
    except OSError as exc:
        raise ObjectStoreError("cannot bind object-store location") from exc


def _identity(info: os.stat_result, *, directory: bool = False) -> tuple[int, int]:
    if not (stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode)):
        raise ObjectIntegrityError(
            "object-store paths must be nonsymlink regular files/directories"
        )
    return info.st_dev, info.st_ino


def _snapshot(info: os.stat_result) -> tuple[int, int, int, int]:
    return (*_identity(info), info.st_size, info.st_mtime_ns)


@contextmanager
def _closing(handle: BinaryIO) -> Iterator[BinaryIO]:
    primary: BaseException | None = None
    try:
        yield handle
    except BaseException as exc:
        primary = exc
        raise
    finally:
        try:
            handle.close()
        except Exception:
            if primary is None:
                raise
            primary.add_note("object file cleanup also failed")


@contextmanager
def _regular(path: Path, size: int) -> Iterator[BinaryIO]:
    before = _snapshot(path.lstat())
    if before[2] != size:
        raise ObjectIntegrityError("object-store file length differs from the expected size")
    with _closing(path.open("rb")) as handle:
        if _snapshot(os.fstat(handle.fileno())) != before:
            raise ObjectIntegrityError("object-store file changed while opening")
        yield handle
        if _snapshot(os.fstat(handle.fileno())) != before or _snapshot(path.lstat()) != before:
            raise ObjectIntegrityError("object-store file changed during reading")


@dataclass(frozen=True, slots=True)
class LocalObjectClient:
    """Read-only, independently bounded locator; safe to pass to a trusted local actor.

    References contain no path. The caller explicitly supplies and trusts this
    directory. This client does not authenticate an owner or acquire a lease.
    """

    directory: Path
    store_id: str
    max_object_bytes: int = _MAX_OBJECT
    _directory_identity: tuple[int, int] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "directory", _absolute(self.directory))
        _hex(self.store_id, "store_id", 32)
        _count(self.max_object_bytes, "max_object_bytes", _MAX_OBJECT)
        try:
            object.__setattr__(
                self, "_directory_identity", _identity(self.directory.lstat(), directory=True)
            )
            self._active()
        except FileNotFoundError as exc:
            raise ObjectUnavailableError("object store is unavailable") from exc
        except OSError as exc:
            raise ObjectStoreError("cannot inspect object store") from exc

    def _active(self) -> None:
        if _identity(self.directory.lstat(), directory=True) != self._directory_identity:
            raise ObjectIntegrityError("object-store directory identity changed")
        with _regular(self.directory / "active", _MARKER.size) as marker:
            if marker.read(_MARKER.size) != _MARKER.pack(b"GSSTR001", bytes.fromhex(self.store_id)):
                raise ObjectIntegrityError("object-store marker does not match the expected store")

    def get(self, reference: ObjectRef) -> bytes:
        ref = _reference(reference)
        if ref.store_id != self.store_id:
            raise ObjectIntegrityError("reference belongs to a different store")
        if ref.size_bytes > self.max_object_bytes:
            raise ObjectCapacityError("object exceeds the client's independent read limit")
        try:
            self._active()
            path = self.directory / (ref.object_id + ".object")
            expected_header = _HEADER.pack(
                b"GSOBJ001",
                bytes.fromhex(ref.store_id),
                bytes.fromhex(ref.object_id),
                ref.size_bytes,
                bytes.fromhex(ref.sha256),
            )
            with _regular(path, _HEADER.size + ref.size_bytes) as source:
                if source.read(_HEADER.size) != expected_header:
                    raise ObjectIntegrityError(
                        "object header does not match the supplied reference"
                    )
                # Allocation uses the independently bounded reference size, never
                # an unchecked integer supplied by the file header.
                result = source.read(ref.size_bytes)
                if len(result) != ref.size_bytes or source.read(1):
                    raise ObjectIntegrityError("object payload length changed during reading")
                if hashlib.sha256(result).hexdigest() != ref.sha256:
                    raise ObjectIntegrityError("object payload digest does not match its reference")
            self._active()
            return result
        except FileNotFoundError as exc:
            raise ObjectUnavailableError("object was released or its store is unavailable") from exc
        except OSError as exc:
            raise ObjectStoreError("cannot read local byte object") from exc


@dataclass(slots=True)
class _Reservation:
    reference: ObjectRef
    stage: Path
    destination: Path
    identity: tuple[int, int] | None = None
    stage_created: bool = False
    destination_created: bool = False
    active: bool = False

    @property
    def file_bytes(self) -> int:
        return _HEADER.size + self.reference.size_bytes


class LocalObjectStore:
    """One owner PID, serialized thread-safe mutations, explicitly closed lifetime.

    A new private directory is exclusively created under an existing parent.
    Repeated put of equal bytes creates distinct references, not deduplication.
    """

    def __init__(self, parent: str | Path, config: ObjectStoreConfig | None = None) -> None:
        self._parent = _absolute(parent)
        if config is not None and type(config) is not ObjectStoreConfig:
            raise ValidationError("config must be ObjectStoreConfig")
        self._config = ObjectStoreConfig() if config is None else config
        self._pid = os.getpid()
        self._lock = RLock()
        self._directory: Path | None = None
        self._directory_identity: tuple[int, int] | None = None
        self._store_id = uuid4().hex
        self._marker_identity: tuple[int, int] | None = None
        self._reservations: dict[str, _Reservation] = {}
        self._entered = self._stopping = self._closed = False

    def _owner(self) -> None:
        if os.getpid() != self._pid:
            raise ObjectStoreError("object-store owner cannot be inherited by another process")

    def _root(self) -> Path:
        if self._directory is None:
            raise ObjectUnavailableError("object-store context has not been entered")
        try:
            if _identity(self._directory.lstat(), directory=True) != self._directory_identity:
                raise ObjectIntegrityError("owned object-store directory identity changed")
        except FileNotFoundError as exc:
            raise ObjectUnavailableError("owned object-store directory is absent") from exc
        except OSError as exc:
            raise ObjectStoreError("cannot inspect owned object-store directory") from exc
        return self._directory

    def _writable(self) -> Path:
        self._owner()
        if not self._entered or self._stopping or self._closed:
            raise ObjectUnavailableError("object-store owner is not active")
        return self._root()

    @property
    def directory(self) -> Path:
        self._owner()
        if self._directory is None:
            raise ObjectUnavailableError("object-store context has not been entered")
        return self._directory

    @property
    def store_id(self) -> str:
        self._owner()
        return self._store_id

    @property
    def config(self) -> ObjectStoreConfig:
        return self._config

    @property
    def stats(self) -> ObjectStoreStats:
        self._owner()
        with self._lock:
            active = [item for item in self._reservations.values() if item.active]
            pending = [item for item in self._reservations.values() if not item.active]
            active_bytes = sum(item.file_bytes for item in active)
            pending_bytes = sum(item.file_bytes for item in pending)
            return ObjectStoreStats(
                len(active),
                active_bytes,
                len(pending),
                pending_bytes,
                active_bytes + pending_bytes,
                self._closed,
            )

    def client(self, *, max_object_bytes: int | None = None) -> LocalObjectClient:
        self._owner()
        with self._lock:
            root = self._writable()
            return LocalObjectClient(
                root,
                self._store_id,
                self.config.max_object_bytes if max_object_bytes is None else max_object_bytes,
            )

    def __enter__(self) -> LocalObjectStore:
        self._owner()
        with self._lock:
            if self._entered or self._closed:
                raise ObjectUnavailableError("object-store context cannot be entered twice")
            try:
                self._directory = Path(
                    tempfile.mkdtemp(prefix="graph-sail-objects-", dir=self._parent)
                )
                self._directory_identity = _identity(self._directory.lstat(), directory=True)
                marker = self._directory / "active"
                with _closing(marker.open("xb")) as handle:
                    self._marker_identity = _identity(os.fstat(handle.fileno()))
                    if (
                        handle.write(_MARKER.pack(b"GSSTR001", bytes.fromhex(self._store_id)))
                        != _MARKER.size
                    ):
                        raise ObjectStoreError("object-store marker write was incomplete")
                    handle.flush()
                    os.fsync(handle.fileno())
                self._entered = True
                return self
            except BaseException as primary:
                try:
                    self.close()
                except Exception:
                    primary.add_note("object-store setup cleanup also failed")
                if isinstance(primary, OSError):
                    raise ObjectStoreError("cannot create local object store") from primary
                raise

    @staticmethod
    def _unlink_owned(path: Path, identity: tuple[int, int] | None) -> None:
        try:
            current = path.lstat()
        except FileNotFoundError:
            return
        if identity is None or _identity(current) != identity:
            raise ObjectIntegrityError("refusing to remove a replaced or unknown object-store file")
        path.unlink()

    def _retire(self, reservation: _Reservation) -> list[str]:
        reservation.active = False
        errors = []
        for name in ("stage", "destination"):
            flag = name + "_created"
            if not getattr(reservation, flag):
                continue
            path = getattr(reservation, name)
            try:
                self._unlink_owned(path, reservation.identity)
            except Exception:
                errors.append(path.name)
            else:
                setattr(reservation, flag, False)
        if not reservation.stage_created and not reservation.destination_created:
            del self._reservations[reservation.reference.object_id]
        return errors

    def put(self, data: bytes) -> ObjectRef:
        self._owner()
        if type(data) is not bytes:
            raise ValidationError(
                "local objects must be exact immutable bytes, not pickled objects"
            )
        with self._lock:
            root = self._writable()
            if len(data) > self.config.max_object_bytes:
                raise ObjectCapacityError("object exceeds the store's per-object limit")
            if (
                len(self._reservations) >= self.config.max_objects
                or self.stats.reserved_bytes + _HEADER.size + len(data)
                > self.config.max_store_bytes
            ):
                raise ObjectCapacityError("object store capacity is reserved or exhausted")
            ref = ObjectRef(
                self._store_id, uuid4().hex, hashlib.sha256(data).hexdigest(), len(data)
            )
            if ref.object_id in self._reservations:
                raise ObjectIntegrityError("generated object identity is already reserved")
            # Admit all paths and bookkeeping before external creation. Existing
            # foreign paths are never marked as ours when exclusive creation fails.
            reservation = _Reservation(
                ref, root / ("." + ref.object_id + ".stage"), root / (ref.object_id + ".object")
            )
            self._reservations[ref.object_id] = reservation
            descriptor: int | None = None
            try:
                descriptor = os.open(
                    reservation.stage,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0),
                    0o600,
                )
                reservation.stage_created = True
                reservation.identity = _identity(os.fstat(descriptor))
                handle = os.fdopen(descriptor, "wb")
                descriptor = None
                with _closing(handle):
                    header = _HEADER.pack(
                        b"GSOBJ001",
                        bytes.fromhex(ref.store_id),
                        bytes.fromhex(ref.object_id),
                        ref.size_bytes,
                        bytes.fromhex(ref.sha256),
                    )
                    if handle.write(header) != len(header) or handle.write(data) != len(data):
                        raise ObjectStoreError("object staging write was incomplete")
                    handle.flush()
                    os.fsync(handle.fileno())
                os.link(reservation.stage, reservation.destination)
                reservation.destination_created = True
                self._unlink_owned(reservation.stage, reservation.identity)
                reservation.stage_created = False
                reservation.active = True
                return ref
            except BaseException as primary:
                if descriptor is not None:
                    try:
                        os.close(descriptor)
                    except Exception:
                        primary.add_note("object staging descriptor cleanup also failed")
                errors = self._retire(reservation)
                if errors:
                    primary.add_note("object staging cleanup incomplete; capacity remains reserved")
                if isinstance(primary, OSError):
                    raise ObjectStoreError("cannot stage or publish byte object") from primary
                raise

    def release(self, reference: ObjectRef) -> bool:
        self._owner()
        ref = _reference(reference)
        with self._lock:
            self._writable()
            if ref.store_id != self._store_id:
                raise ObjectIntegrityError("cannot release another store's object")
            reservation = self._reservations.get(ref.object_id)
            if reservation is None:
                return False
            if reservation.reference != ref:
                raise ObjectIntegrityError("release reference does not match the owned object")
            if self._retire(reservation):
                raise ObjectStoreError("object release incomplete; capacity remains reserved")
            return True

    def retry_cleanup(self) -> None:
        self._owner()
        with self._lock:
            self._root()
            errors = []
            for reservation in tuple(self._reservations.values()):
                if not reservation.active:
                    errors.extend(self._retire(reservation))
            if errors:
                raise ObjectStoreError(
                    "object cleanup remains incomplete; capacity is still reserved"
                )

    def close(self) -> None:
        self._owner()
        with self._lock:
            if self._closed:
                return
            self._stopping = True
            if self._directory is None:
                self._closed = True
                return
            try:
                root = self._root()
                errors = []
                if self._marker_identity is not None:
                    try:
                        self._unlink_owned(root / "active", self._marker_identity)
                    except Exception:
                        errors.append("active marker")
                    else:
                        self._marker_identity = None
                for reservation in tuple(self._reservations.values()):
                    errors.extend(self._retire(reservation))
                if not errors:
                    # Never recursively delete unknown files, even in our directory.
                    root.rmdir()
                    self._closed = True
                else:
                    raise ObjectStoreError(
                        "object-store close incomplete; retry or inspect owned directory"
                    )
            except OSError as exc:
                raise ObjectStoreError(
                    "cannot remove object-store directory; unknown files are preserved"
                ) from exc

    def __exit__(
        self, exception_type: object, exception: BaseException | None, traceback: object
    ) -> None:
        try:
            self.close()
        except Exception:
            if exception is None:
                raise
            exception.add_note("object-store context cleanup also failed; inspect owner stats")
