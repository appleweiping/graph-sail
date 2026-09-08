from __future__ import annotations

import hashlib
import json
import os
import stat
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from dataclasses import FrozenInstanceError, replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from graph_sail import (
    ActorConfig,
    ActorDefinition,
    ActorRegistry,
    ActorRemoteError,
    ActorSerializationError,
    GreedyPlanner,
    LocalObjectClient,
    LocalObjectStore,
    ObjectCapacityError,
    ObjectIntegrityError,
    ObjectRef,
    ObjectStoreConfig,
    ObjectStoreError,
    ObjectStoreStats,
    ObjectUnavailableError,
    ProcessActor,
    TaskRegistry,
    ValidationError,
    execute_graph,
    graph_from_dict,
)
from graph_sail import objects as module


def reference():
    return ObjectRef("a" * 32, "b" * 32, hashlib.sha256(b"abc").hexdigest(), 3)


def object_path(store, ref):
    return store.directory / (ref.object_id + ".object")


class ByteReader:
    def __init__(self, directory, store_id):
        self.client = LocalObjectClient(Path(directory), store_id)
        self.reads = 0

    def summarize(self, ref):
        payload = self.client.get(ref)
        self.reads += 1
        return (
            os.getpid(),
            len(payload),
            sum(payload),
            hashlib.sha256(payload).hexdigest(),
            self.reads,
        )


def test_actual_actor_moves_only_small_reference_and_reuses_verified_large_object(tmp_path):
    payload = bytes(range(256)) * 8192
    assert len(payload) == 2 * 1024 * 1024
    expected_sum = 32640 * 8192
    with LocalObjectStore(tmp_path) as store:
        ref = store.put(payload)
        registry = ActorRegistry({"reader": ActorDefinition(ByteReader, ("summarize",))})
        with ProcessActor(
            registry,
            "reader",
            args=(str(store.directory), store.store_id),
            config=ActorConfig(max_message_bytes=4096),
        ) as actor:
            for count in (1, 2):
                result = actor.submit("summarize", args=(ref,)).result(timeout=15)
                assert result == (
                    actor.pid,
                    len(payload),
                    expected_sum,
                    hashlib.sha256(payload).hexdigest(),
                    count,
                )
                assert result[0] != os.getpid()
            with pytest.raises(ActorSerializationError):
                actor.submit("summarize", args=(payload,))
            assert store.release(ref)
            with pytest.raises(ActorRemoteError, match="ObjectUnavailableError"):
                actor.submit("summarize", args=(ref,)).result(timeout=15)
            empty = store.put(b"")
            assert actor.submit("summarize", args=(empty,)).result(timeout=15)[1:3] == (0, 0)
        assert actor.exitcode == 0
    assert not store.directory.exists()


def test_fresh_process_reads_json_reference_without_persistent_pickle(tmp_path):
    with LocalObjectStore(tmp_path) as store:
        ref = store.put(b"\x00\xffhello\x80")
        program = (
            "import json,sys,hashlib; from pathlib import Path; "
            "from graph_sail import LocalObjectClient,ObjectRef; "
            "ref=ObjectRef.from_dict(json.loads(sys.argv[3])); "
            "data=LocalObjectClient(Path(sys.argv[1]),sys.argv[2]).get(ref); "
            "print(json.dumps([len(data),sum(data),hashlib.sha256(data).hexdigest()]))"
        )
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                program,
                str(store.directory),
                store.store_id,
                json.dumps(ref.to_dict()),
            ],
            capture_output=True,
            text=True,
            timeout=20,
            check=True,
        )
        assert json.loads(result.stdout) == [8, 915, ref.sha256]


def test_graph_tasks_explicitly_pass_refs_and_read_dependency_bytes(tmp_path):
    graph = graph_from_dict(
        {
            "name": "byte-flow",
            "devices": [{"name": "cpu", "memory_mb": 10}],
            "nodes": [
                {"id": name, "kind": "bytes", "memory_mb": 1, "latency_ms": {"cpu": 1}}
                for name in ("source", "sum")
            ],
            "edges": [{"source": "source", "target": "sum"}],
        }
    )
    with LocalObjectStore(tmp_path) as store:
        client = store.client()
        registry = TaskRegistry(
            {
                "source": lambda context: store.put(bytes(range(10))),
                "sum": lambda context: sum(client.get(context.dependencies["source"])),
            }
        )
        result = execute_graph(graph, registry, GreedyPlanner().plan(graph).placements)
        assert result.status == "succeeded" and result.outputs["sum"] == 45
        assert isinstance(result.outputs["source"], ObjectRef)
        assert store.stats.active_objects == 1
        assert "sha256" not in json.dumps(result.to_dict())


def test_lifecycle_identity_immutability_empty_binary_and_no_dedup(tmp_path):
    store = LocalObjectStore(tmp_path)
    assert store.stats == ObjectStoreStats(0, 0, 0, 0, 0, False)
    for operation in (
        lambda: store.directory,
        lambda: store.client(),
        lambda: store.put(b"a"),
        store.retry_cleanup,
    ):
        with pytest.raises(ObjectUnavailableError):
            operation()
    with store:
        directory = store.directory
        client = store.client()
        for payload in (b"", b"\x00\xff\x80", b"same", b"same"):
            ref = store.put(payload)
            assert client.get(ref) == payload
            assert ObjectRef.from_dict(json.loads(json.dumps(ref.to_dict()))) == ref
            with pytest.raises(FrozenInstanceError):
                ref.size_bytes = 1
        assert store.stats.active_objects == 4
        assert store.stats.reserved_bytes == 4 * 80 + 11
        assert len(list(directory.glob("*.object"))) == 4
        assert list(directory.glob("*.stage")) == []
        snapshot = store.stats.to_dict()
        snapshot["closed"] = True
        assert not store.stats.closed
        assert store.release(ref)
        assert not store.release(ref)
        assert store.put(b"same").object_id != ref.object_id
        with pytest.raises(ObjectUnavailableError):
            client.get(ref)
        with pytest.raises(ObjectUnavailableError):
            store.__enter__()
    assert store.stats.closed and store.stats.reserved_bytes == 0 and not directory.exists()
    store.close()
    with pytest.raises(ObjectUnavailableError):
        client.get(ref)
    with pytest.raises(ObjectUnavailableError):
        store.__enter__()


@pytest.mark.parametrize(
    "field,value",
    [
        ("store_id", "../"),
        ("object_id", "B" * 32),
        ("sha256", "a" * 63),
        ("size_bytes", True),
        ("size_bytes", -1),
        ("size_bytes", 2**1000),
        ("store_id", []),
        ("object_id", 0),
        ("sha256", None),
    ],
)
def test_ref_rejects_malformed_field_types_and_paths(field, value):
    with pytest.raises(ValidationError):
        replace(reference(), **{field: value})


@pytest.mark.parametrize("value", [None, [], {}, {"extra": 1}, {1: "x"}])
def test_ref_requires_exact_json_document(value):
    with pytest.raises(ValidationError):
        ObjectRef.from_dict(value)


def test_ref_rejects_unknown_or_missing_fields_versions_and_subclasses(tmp_path):
    valid = reference().to_dict()
    for changed in (
        dict(valid, extra=0),
        dict(valid, kind="other"),
        dict(valid, schema_version=True),
    ):
        with pytest.raises(ValidationError):
            ObjectRef.from_dict(changed)
    del valid["sha256"]
    with pytest.raises(ValidationError):
        ObjectRef.from_dict(valid)
    with LocalObjectStore(tmp_path) as store:
        with pytest.raises(ValidationError):
            store.client().get({})
        with pytest.raises(ValidationError):
            store.put(bytearray(b"a"))


@pytest.mark.parametrize(
    "field,value",
    [
        ("max_object_bytes", True),
        ("max_object_bytes", -1),
        ("max_object_bytes", 2**1000),
        ("max_store_bytes", 79),
        ("max_store_bytes", False),
        ("max_store_bytes", 2**1000),
        ("max_objects", 0),
        ("max_objects", 1025),
        ("max_objects", 1.0),
    ],
)
def test_config_strict_resource_bounds(field, value):
    with pytest.raises(ValidationError):
        ObjectStoreConfig(**{field: value})


@pytest.mark.parametrize(
    "options",
    [
        {"active_objects": True},
        {"active_objects": 1025},
        {"active_file_bytes": -1},
        {"closed": 0},
        {"reserved_bytes": 1},
        {"active_file_bytes": 80, "reserved_bytes": 80},
        {"active_objects": 1},
        {"pending_cleanup_objects": 1},
        {"pending_cleanup_reserved_bytes": 80, "reserved_bytes": 80},
        {"active_objects": 1, "active_file_bytes": 80, "reserved_bytes": 80, "closed": True},
        {
            "active_objects": 1024,
            "active_file_bytes": 81920,
            "pending_cleanup_objects": 1,
            "pending_cleanup_reserved_bytes": 80,
            "reserved_bytes": 82000,
        },
    ],
)
def test_stats_reject_inconsistent_derived_fields(options):
    with pytest.raises(ValidationError):
        replace(ObjectStoreStats(0, 0, 0, 0, 0, False), **options)


def test_capacity_counts_header_per_object_and_release_frees_capacity(tmp_path):
    with LocalObjectStore(tmp_path, ObjectStoreConfig(3, 166, 2)) as store:
        a, b = store.put(b"abc"), store.put(b"abc")
        assert a != b and store.stats.reserved_bytes == 166
        with pytest.raises(ObjectCapacityError):
            store.put(b"")
        with pytest.raises(ObjectCapacityError):
            store.put(b"abcd")
        store.release(a)
        c = store.put(b"\x00")
        assert store.stats.reserved_bytes == 164 and store.client().get(c) == b"\x00"
    with LocalObjectStore(tmp_path, ObjectStoreConfig(0, 80, 1)) as store:
        assert store.client().get(store.put(b"")) == b""
        with pytest.raises(ObjectCapacityError):
            store.put(b"x")


def test_concurrent_threads_reserve_once_and_enforce_capacity(tmp_path):
    with LocalObjectStore(tmp_path, ObjectStoreConfig(1, 405, 5)) as store:

        def attempt(index):
            try:
                return store.put(bytes([index]))
            except ObjectCapacityError:
                return None

        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(attempt, range(12)))
        refs = [ref for ref in results if ref is not None]
        assert len(refs) == 5 and len({ref.object_id for ref in refs}) == 5
        assert store.stats.reserved_bytes == 405
        assert len({store.client().get(ref) for ref in refs}) == 5


def test_client_limit_checks_before_any_file_access_and_header_never_controls_allocation(
    tmp_path, monkeypatch
):
    with LocalObjectStore(tmp_path) as store:
        ref = store.put(b"abcd")
        client = store.client(max_object_bytes=3)
        with monkeypatch.context() as patch:
            patch.setattr(Path, "lstat", lambda path: pytest.fail("limit must precede file IO"))
            with pytest.raises(ObjectCapacityError):
                client.get(ref)
        path = object_path(store, ref)
        original = path.read_bytes()
        malformed = bytearray(original)
        malformed[40:48] = (2**64 - 1).to_bytes(8, "little")
        path.write_bytes(malformed)
        with pytest.raises(ObjectIntegrityError, match="header"):
            store.client().get(ref)
        path.write_bytes(original + b"extra")
        with pytest.raises(ObjectIntegrityError, match="length"):
            store.client().get(ref)


def test_every_header_and_payload_byte_is_bound_and_wrong_refs_are_rejected(tmp_path):
    with LocalObjectStore(tmp_path) as store, LocalObjectStore(tmp_path) as other:
        ref = store.put(b"\x00\xffabc")
        client = store.client()
        path = object_path(store, ref)
        original = path.read_bytes()
        for index in range(len(original)):
            changed = bytearray(original)
            changed[index] ^= 1
            path.write_bytes(changed)
            with pytest.raises(ObjectIntegrityError):
                client.get(ref)
        path.write_bytes(original)
        assert client.get(ref) == b"\x00\xffabc"
        with pytest.raises(ObjectIntegrityError, match="different store"):
            other.client().get(ref)
        with pytest.raises(ObjectIntegrityError):
            other.release(ref)
        for altered in (replace(ref, sha256="f" * 64), replace(ref, size_bytes=0)):
            with pytest.raises(ObjectIntegrityError):
                client.get(altered)
            with pytest.raises(ObjectIntegrityError):
                store.release(altered)


def test_marker_and_directory_identity_checked_before_and_after_read(tmp_path, monkeypatch):
    with LocalObjectStore(tmp_path) as store:
        ref = store.put(b"abc")
        client = store.client()
        marker = store.directory / "active"
        original = marker.read_bytes()
        marker.write_bytes(b"wrong" + original[5:])
        with pytest.raises(ObjectIntegrityError, match="marker"):
            client.get(ref)
        marker.write_bytes(original)
        digest = module.hashlib.sha256

        def revoke(data):
            marker.unlink()
            return digest(data)

        with monkeypatch.context() as patch:
            patch.setattr(module.hashlib, "sha256", revoke)
            with pytest.raises(ObjectUnavailableError):
                client.get(ref)
        # The removed marker is not recreated by reads or writes.
        assert not marker.exists()


def test_client_and_owner_paths_stay_bound_when_cwd_changes(tmp_path, monkeypatch):
    (tmp_path / "parent").mkdir()
    (tmp_path / "elsewhere").mkdir()
    monkeypatch.chdir(tmp_path)
    store = LocalObjectStore("parent")
    monkeypatch.chdir(tmp_path / "elsewhere")
    with store:
        ref = store.put(b"bound")
        monkeypatch.chdir(store.directory.parent)
        client = LocalObjectClient(Path(store.directory.name), store.store_id)
        monkeypatch.chdir(tmp_path / "elsewhere")
        assert client.directory.is_absolute() and client.get(ref) == b"bound"
        assert store.directory.parent == tmp_path / "parent"


def test_failed_release_retains_capacity_until_explicit_cleanup_retry(tmp_path, monkeypatch):
    with LocalObjectStore(tmp_path, ObjectStoreConfig(3, 83, 1)) as store:
        ref = store.put(b"abc")
        path = object_path(store, ref)
        real_unlink = Path.unlink

        def deny(selected, *args, **kwargs):
            if selected == path:
                raise PermissionError("in use")
            return real_unlink(selected, *args, **kwargs)

        with monkeypatch.context() as patch:
            patch.setattr(Path, "unlink", deny)
            with pytest.raises(ObjectStoreError, match="capacity remains"):
                store.release(ref)
            assert store.stats == ObjectStoreStats(0, 0, 1, 83, 83, False)
            with pytest.raises(ObjectCapacityError):
                store.put(b"abc")
            with pytest.raises(ObjectStoreError, match="still reserved"):
                store.retry_cleanup()
        store.retry_cleanup()
        assert store.stats.reserved_bytes == 0 and not path.exists()
        assert not store.release(ref)
        assert store.client().get(store.put(b"new")) == b"new"


def test_staged_hardlinks_are_one_reservation_and_failed_cleanup_is_not_free_capacity(
    tmp_path, monkeypatch
):
    with LocalObjectStore(tmp_path, ObjectStoreConfig(3, 83, 1)) as store:
        real_unlink = Path.unlink

        def deny(selected, *args, **kwargs):
            if selected.suffix in (".stage", ".object"):
                assert store.stats.reserved_bytes == 83
                paths = list(store.directory.glob("*"))
                data_paths = [path for path in paths if path.name != "active"]
                assert len({path.stat().st_ino for path in data_paths}) == 1
                raise PermissionError("keep both links")
            return real_unlink(selected, *args, **kwargs)

        with monkeypatch.context() as patch:
            patch.setattr(Path, "unlink", deny)
            with pytest.raises(ObjectStoreError):
                store.put(b"abc")
            assert store.stats == ObjectStoreStats(0, 0, 1, 83, 83, False)
            assert len(list(store.directory.iterdir())) == 3
        store.retry_cleanup()
        assert store.stats.reserved_bytes == 0 and list(store.directory.iterdir()) == [
            store.directory / "active"
        ]


@pytest.mark.parametrize("operation", ["fsync", "link", "open", "fdopen"])
def test_failed_publish_never_returns_ref_and_rolls_back_staging(tmp_path, monkeypatch, operation):
    with LocalObjectStore(tmp_path) as store:

        def fail(*args, **kwargs):
            raise OSError("controlled failure")

        with monkeypatch.context() as patch:
            patch.setattr(module.os, operation, fail)
            with pytest.raises(ObjectStoreError):
                store.put(b"abc")
        assert store.stats.reserved_bytes == 0
        assert list(store.directory.iterdir()) == [store.directory / "active"]


def test_unknown_files_are_preserved_and_close_can_retry_after_user_removes_them(tmp_path):
    store = LocalObjectStore(tmp_path).__enter__()
    ref = store.put(b"abc")
    client = store.client()
    unknown = store.directory / "user-note.txt"
    unknown.write_bytes(b"keep me")
    with pytest.raises(ObjectStoreError, match="unknown files are preserved"):
        store.close()
    assert not store.stats.closed and store.stats.reserved_bytes == 0
    assert unknown.read_bytes() == b"keep me"
    with pytest.raises(ObjectUnavailableError):
        client.get(ref)
    with pytest.raises(ObjectUnavailableError):
        store.put(b"new")
    unknown.unlink()
    store.close()
    assert store.stats.closed


def test_replaced_file_is_never_deleted_as_if_owned(tmp_path):
    store = LocalObjectStore(tmp_path).__enter__()
    ref = store.put(b"abc")
    path = object_path(store, ref)
    old = tmp_path / "original-object"
    path.rename(old)
    path.write_bytes(b"unrelated")
    with pytest.raises(ObjectStoreError, match="capacity remains"):
        store.release(ref)
    assert path.read_bytes() == b"unrelated" and store.stats.reserved_bytes == 83
    with pytest.raises(ObjectStoreError):
        store.close()
    assert not store.stats.closed and path.exists()
    path.unlink()
    store.close()
    assert old.exists()  # Owner never searches other paths for its moved inode.


def test_failed_marker_close_is_reported_and_retry_revokes_store(tmp_path, monkeypatch):
    store = LocalObjectStore(tmp_path).__enter__()
    marker = store.directory / "active"
    real_unlink = Path.unlink

    def deny(path, *args, **kwargs):
        if path == marker:
            raise PermissionError("busy marker")
        return real_unlink(path, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(Path, "unlink", deny)
        with pytest.raises(ObjectStoreError, match="close incomplete"):
            store.close()
        assert not store.stats.closed and marker.exists()
    store.close()
    assert store.stats.closed and not store.directory.exists()


@pytest.mark.parametrize("failure", [RuntimeError("body"), KeyboardInterrupt(), SystemExit(7)])
def test_context_cleanup_failure_preserves_body_exception_identity(tmp_path, monkeypatch, failure):
    store = LocalObjectStore(tmp_path)
    with pytest.raises(type(failure)) as caught, store:
        with monkeypatch.context() as patch:
            patch.setattr(store, "close", lambda: (_ for _ in ()).throw(OSError("close")))
            store.__exit__(type(failure), failure, None)
        raise failure
    assert caught.value is failure
    assert "cleanup also failed" in " ".join(failure.__notes__)


def test_inherited_owner_is_refused_before_acquiring_potentially_inherited_lock(
    tmp_path, monkeypatch
):
    with LocalObjectStore(tmp_path) as store:
        ref = store.put(b"abc")

        class NeverLock:
            def __enter__(self):
                pytest.fail("must reject inherited owner before locking")

            def __exit__(self, *args):
                pass

        with monkeypatch.context() as patch:
            patch.setattr(store, "_lock", NeverLock())
            patch.setattr(module.os, "getpid", lambda: store._pid + 1)
            for operation in (
                store.close,
                store.client,
                store.retry_cleanup,
                store.__enter__,
                lambda: store.put(b""),
                lambda: store.release(ref),
                lambda: store.directory,
                lambda: store.store_id,
                lambda: store.stats,
            ):
                with pytest.raises(ObjectStoreError, match="inherited"):
                    operation()


@pytest.mark.parametrize("field", ["active", "object"])
def test_nonregular_and_symlink_file_modes_fail_before_open(tmp_path, monkeypatch, field):
    with LocalObjectStore(tmp_path) as store:
        ref = store.put(b"abc")
        client = store.client()
        selected = store.directory / "active" if field == "active" else object_path(store, ref)
        real_lstat = Path.lstat
        for mode in (stat.S_IFLNK, stat.S_IFDIR, stat.S_IFIFO):

            def lstat(path, mode=mode):
                if path == selected:
                    return SimpleNamespace(st_mode=mode)
                return real_lstat(path)

            with monkeypatch.context() as patch:
                patch.setattr(Path, "lstat", lstat)
                with pytest.raises(ObjectIntegrityError, match="nonsymlink regular"):
                    client.get(ref)


@pytest.mark.parametrize("path", ["", "\x00", None, 3, []])
def test_invalid_locations_are_rejected_without_io(path):
    with pytest.raises(ValidationError):
        LocalObjectStore(path)


def test_invalid_config_and_absent_parent_cleanup_are_explicit(tmp_path):
    with pytest.raises(ValidationError):
        LocalObjectStore(tmp_path, {})
    store = LocalObjectStore(tmp_path / "absent")
    with pytest.raises(ObjectStoreError, match="cannot create"):
        store.__enter__()
    assert store.stats.closed
    before_enter = LocalObjectStore(tmp_path)
    before_enter.close()
    assert before_enter.stats.closed
    with pytest.raises(ObjectUnavailableError):
        before_enter.__enter__()


def test_reader_absent_store_and_files_are_domain_errors(tmp_path):
    with pytest.raises(ObjectUnavailableError):
        LocalObjectClient(tmp_path / "absent", "a" * 32)
    with LocalObjectStore(tmp_path) as store:
        client = store.client()
        with pytest.raises(ObjectUnavailableError):
            client.get(replace(reference(), store_id=store.store_id))
        with pytest.raises(ValidationError):
            store.client(max_object_bytes=True)


@pytest.mark.parametrize("phase", ["open", "after-read", "short-read", "trailing-read"])
def test_same_opened_file_detects_observed_change_and_incomplete_reads(
    tmp_path, monkeypatch, phase
):
    with LocalObjectStore(tmp_path) as store:
        ref = store.put(b"abc")
        client = store.client()
        path = object_path(store, ref)
        real_open = Path.open
        original = path.read_bytes()
        changed = tmp_path / "moved-object"

        class Reader:
            def __init__(self, handle):
                self.handle = handle

            def fileno(self):
                return self.handle.fileno()

            def close(self):
                self.handle.close()

            def read(self, count):
                result = self.handle.read(count)
                if count == ref.size_bytes:
                    if phase == "after-read":
                        info = path.stat()
                        os.utime(path, ns=(info.st_atime_ns, info.st_mtime_ns + 1_000_000))
                    if phase == "short-read":
                        return result[:-1]
                if count == 1 and phase == "trailing-read":
                    return b"x"
                return result

        def opening(selected, *args, **kwargs):
            if selected == path:
                if phase == "open":
                    selected.rename(changed)
                    with real_open(selected, "wb") as replacement:
                        replacement.write(original)
                return Reader(real_open(selected, *args, **kwargs))
            return real_open(selected, *args, **kwargs)

        with monkeypatch.context() as patch:
            patch.setattr(Path, "open", opening)
            with pytest.raises(ObjectIntegrityError):
                client.get(ref)
        if phase == "open":
            path.unlink()
            changed.rename(path)
        assert client.get(ref) == b"abc"


def test_replaced_root_is_not_read_or_removed(tmp_path):
    store = LocalObjectStore(tmp_path).__enter__()
    ref = store.put(b"abc")
    client = store.client()
    original = store.directory
    relocated = tmp_path / "moved-store"
    original.rename(relocated)
    original.mkdir()
    try:
        with pytest.raises(ObjectIntegrityError, match="directory identity"):
            client.get(ref)
        with pytest.raises(ObjectIntegrityError, match="directory identity"):
            store.put(b"new")
        with pytest.raises(ObjectIntegrityError):
            store.close()
        assert original.exists() and relocated.exists()
    finally:
        original.rmdir()
        relocated.rename(original)
        store.close()


def test_absent_owned_root_and_os_inspection_errors_are_explicit(tmp_path, monkeypatch):
    with LocalObjectStore(tmp_path) as store:
        real_lstat = Path.lstat

        def denied(path):
            if path == store.directory:
                raise PermissionError("inspection denied")
            return real_lstat(path)

        with monkeypatch.context() as patch:
            patch.setattr(Path, "lstat", denied)
            with pytest.raises(ObjectStoreError, match="cannot inspect owned"):
                store.put(b"a")
            with pytest.raises(ObjectStoreError, match="cannot inspect object"):
                LocalObjectClient(store.directory, store.store_id)
    missing = LocalObjectStore(tmp_path).__enter__()
    (missing.directory / "active").unlink()
    missing.directory.rmdir()
    with pytest.raises(ObjectUnavailableError, match="directory is absent"):
        missing.put(b"a")
    with pytest.raises(ObjectUnavailableError):
        missing.retry_cleanup()


def test_reader_io_errors_are_domain_errors_and_location_binding_failure_is_explicit(
    tmp_path, monkeypatch
):
    with LocalObjectStore(tmp_path) as store:
        ref = store.put(b"abc")
        client = store.client()
        with monkeypatch.context() as patch:
            patch.setattr(
                Path, "open", lambda *args, **kwargs: (_ for _ in ()).throw(OSError("IO"))
            )
            with pytest.raises(ObjectStoreError, match="cannot read"):
                client.get(ref)
    with monkeypatch.context() as patch:
        patch.setattr(Path, "absolute", lambda path: (_ for _ in ()).throw(OSError("cwd")))
        with pytest.raises(ObjectStoreError, match="cannot bind"):
            LocalObjectStore(tmp_path)


@pytest.mark.parametrize(
    "primary", [None, RuntimeError("read"), KeyboardInterrupt(), SystemExit(3)]
)
def test_file_cleanup_preserves_primary_and_reports_ordinary_failure(primary):
    class Handle:
        def close(self):
            raise OSError("close")

    expected = OSError if primary is None else type(primary)
    with pytest.raises(expected) as caught, module._closing(Handle()):
        if primary is not None:
            raise primary
    if primary is not None:
        assert caught.value is primary and "cleanup also failed" in primary.__notes__[0]


def test_cleanup_does_not_swallow_process_control_exceptions():
    interrupt = KeyboardInterrupt()

    class Handle:
        def close(self):
            raise interrupt

    with pytest.raises(KeyboardInterrupt) as caught, module._closing(Handle()):
        pass
    assert caught.value is interrupt


@pytest.mark.parametrize("failure", [OSError("fsync"), KeyboardInterrupt(), SystemExit(3)])
def test_setup_failure_removes_created_directory_and_preserves_interrupt(
    tmp_path, monkeypatch, failure
):
    store = LocalObjectStore(tmp_path)
    with monkeypatch.context() as patch:
        patch.setattr(module.os, "fsync", lambda fd: (_ for _ in ()).throw(failure))
        expected = ObjectStoreError if isinstance(failure, OSError) else type(failure)
        with pytest.raises(expected) as caught:
            store.__enter__()
    if not isinstance(failure, OSError):
        assert caught.value is failure
    assert store.stats.closed and not store.directory.exists() and list(tmp_path.iterdir()) == []


def test_setup_cleanup_failure_is_attached_and_can_be_retried(tmp_path, monkeypatch):
    store = LocalObjectStore(tmp_path)
    failure = KeyboardInterrupt()
    real_close = store.close
    with monkeypatch.context() as patch:
        patch.setattr(module.os, "fsync", lambda fd: (_ for _ in ()).throw(failure))
        patch.setattr(store, "close", lambda: (_ for _ in ()).throw(OSError("close")))
        with pytest.raises(KeyboardInterrupt) as caught:
            store.__enter__()
    assert caught.value is failure and "setup cleanup" in failure.__notes__[0]
    real_close()
    assert store.stats.closed


def test_publish_interrupt_rolls_back_and_descriptor_cleanup_error_preserves_primary(
    tmp_path, monkeypatch
):
    with LocalObjectStore(tmp_path) as store:
        primary = KeyboardInterrupt()
        real_close = os.close

        def failing_close(fd):
            real_close(fd)
            raise OSError("close after actually releasing descriptor")

        with monkeypatch.context() as patch:
            patch.setattr(module.os, "fdopen", lambda *args: (_ for _ in ()).throw(primary))
            patch.setattr(module.os, "close", failing_close)
            with pytest.raises(KeyboardInterrupt) as caught:
                store.put(b"abc")
        assert caught.value is primary and "descriptor cleanup" in primary.__notes__[0]
        assert store.stats.reserved_bytes == 0


def test_cleanup_retry_does_not_release_active_objects(tmp_path):
    with LocalObjectStore(tmp_path) as store:
        ref = store.put(b"abc")
        store.retry_cleanup()
        assert store.stats.active_objects == 1 and store.client().get(ref) == b"abc"


def test_owner_exit_surfaces_cleanup_failure_when_body_succeeds(tmp_path, monkeypatch):
    store = LocalObjectStore(tmp_path).__enter__()
    with monkeypatch.context() as patch:
        patch.setattr(store, "close", lambda: (_ for _ in ()).throw(OSError("close")))
        with pytest.raises(OSError, match="close"):
            store.__exit__(None, None, None)
    store.close()


def test_pre_admitted_bookkeeping_does_not_insert_mapping_entries_after_link(tmp_path, monkeypatch):
    with LocalObjectStore(tmp_path) as store:

        class NoMoreAllocation(dict):
            def __setitem__(self, key, value):
                raise MemoryError("no new reservation allocation after publication")

        real_link = os.link

        def link(source, destination):
            reservation = next(iter(store._reservations.values()))
            assert reservation.stage == source and reservation.destination == destination
            assert reservation.stage_created and not reservation.destination_created
            real_link(source, destination)
            store._reservations = NoMoreAllocation(store._reservations)

        with monkeypatch.context() as patch:
            patch.setattr(module.os, "link", link)
            ref = store.put(b"abc")
        assert store.stats.reserved_bytes == 83 and store.client().get(ref) == b"abc"
        assert store.release(ref) and store.stats.reserved_bytes == 0


def test_failed_bookkeeping_allocation_happens_before_file_creation(tmp_path, monkeypatch):
    with LocalObjectStore(tmp_path) as store:
        with monkeypatch.context() as patch:
            patch.setattr(
                module, "_Reservation", lambda *args: (_ for _ in ()).throw(MemoryError())
            )
            with pytest.raises(MemoryError):
                store.put(b"abc")
        assert store.stats.reserved_bytes == 0
        assert list(store.directory.iterdir()) == [store.directory / "active"]


def test_identity_capture_failure_keeps_capacity_and_refuses_unverified_unlink(
    tmp_path, monkeypatch
):
    with LocalObjectStore(tmp_path, ObjectStoreConfig(3, 83, 1)) as store:
        real_identity = module._identity

        def identity(info, *, directory=False):
            if not directory:
                raise MemoryError("identity bookkeeping")
            return real_identity(info, directory=True)

        with monkeypatch.context() as patch:
            patch.setattr(module, "_identity", identity)
            with pytest.raises(MemoryError):
                store.put(b"abc")
        assert store.stats == ObjectStoreStats(0, 0, 1, 83, 83, False)
        with pytest.raises(ObjectStoreError):
            store.retry_cleanup()
        with pytest.raises(ObjectCapacityError):
            store.put(b"")
        staged = list(store.directory.glob("*.stage"))
        assert len(staged) == 1
        staged[0].unlink()  # Explicit operator removal, never implicit adoption.
        store.retry_cleanup()
        assert store.stats.reserved_bytes == 0


@pytest.mark.parametrize("collision", ["stage", "destination"])
def test_exclusive_creation_collision_preserves_foreign_file(tmp_path, monkeypatch, collision):
    with LocalObjectStore(tmp_path) as store:
        object_id = "f" * 32
        name = "." + object_id + ".stage" if collision == "stage" else object_id + ".object"
        foreign = store.directory / name
        foreign.write_bytes(b"foreign bytes")
        with monkeypatch.context() as patch:
            patch.setattr(module, "uuid4", lambda: SimpleNamespace(hex=object_id))
            with pytest.raises(ObjectStoreError):
                store.put(b"abc")
        assert foreign.read_bytes() == b"foreign bytes" and store.stats.reserved_bytes == 0
        assert len(list(store.directory.iterdir())) == 2
        foreign.unlink()


def test_uuid_collision_cannot_overwrite_existing_owned_reservation(tmp_path, monkeypatch):
    with LocalObjectStore(tmp_path) as store:
        original = store.put(b"abc")
        with monkeypatch.context() as patch:
            patch.setattr(module, "uuid4", lambda: SimpleNamespace(hex=original.object_id))
            with pytest.raises(ObjectIntegrityError, match="already reserved"):
                store.put(b"xyz")
        assert store.stats.reserved_bytes == 83 and store.client().get(original) == b"abc"


@pytest.mark.parametrize("partial_call", [1, 2])
def test_short_header_or_payload_write_never_publishes_ref(tmp_path, monkeypatch, partial_call):
    with LocalObjectStore(tmp_path) as store:
        real_fdopen = os.fdopen

        class ShortWriter:
            def __init__(self, fd):
                self.handle = real_fdopen(fd, "wb")
                self.calls = 0

            def write(self, data):
                self.calls += 1
                return self.handle.write(data[:-1] if self.calls == partial_call else data)

            def close(self):
                self.handle.close()

        with monkeypatch.context() as patch:
            patch.setattr(module.os, "fdopen", lambda fd, mode: ShortWriter(fd))
            with pytest.raises(ObjectStoreError, match="write was incomplete"):
                store.put(b"abc")
        assert store.stats.reserved_bytes == 0
        assert list(store.directory.iterdir()) == [store.directory / "active"]


@pytest.mark.parametrize(
    "value",
    [
        b"\xff",
        "\ud800",
        "{",
        "[]",
        "null",
        "0",
        "true",
        "1.5",
        "1e99999",
        "NaN",
        reference().to_json()[:-1] + ',"size_bytes":3}',
        '{"size_bytes":999999999999999999999}',
        " " * 1025,
        "\u00e9" * 600,
        [],
        "[" * 500 + "]" * 500,
    ],
)
def test_strict_json_transport_rejects_duplicate_numbers_encoding_depth_and_size(value):
    with pytest.raises(ValidationError):
        ObjectRef.from_json(value)


def test_json_transport_is_bounded_detached_and_round_trips():
    ref = reference()
    rendered = ref.to_json()
    assert len(rendered.encode("utf-8")) < 1024
    assert ObjectRef.from_json(rendered) == ref
    assert ObjectRef.from_json(rendered.encode("utf-8")) == ref
    assert json.loads(rendered) == ref.to_dict()


@pytest.mark.parametrize("selected", ["object", "active", "directory"])
def test_real_symlink_is_refused_without_following_or_deleting_target(tmp_path, selected):
    with LocalObjectStore(tmp_path) as store:
        ref = store.put(b"abc")
        client = store.client()
        path = (
            store.directory
            if selected == "directory"
            else store.directory / "active"
            if selected == "active"
            else object_path(store, ref)
        )
        original = tmp_path / "retained-original"
        path.rename(original)
        try:
            try:
                path.symlink_to(original, target_is_directory=selected == "directory")
            except (OSError, NotImplementedError) as exc:
                pytest.skip(f"host cannot create test symlink: {type(exc).__name__}")
            with pytest.raises(ObjectIntegrityError, match="nonsymlink regular"):
                client.get(ref)
            assert original.exists()
        finally:
            if path.is_symlink():
                path.unlink()
            original.rename(path)
        assert client.get(ref) == b"abc"


def test_short_marker_write_never_opens_a_live_store(tmp_path, monkeypatch):
    real_open = Path.open

    class ShortMarker:
        def __init__(self, handle):
            self.handle = handle

        def fileno(self):
            return self.handle.fileno()

        def write(self, data):
            return self.handle.write(data[:-1])

        def close(self):
            self.handle.close()

    def opening(path, *args, **kwargs):
        handle = real_open(path, *args, **kwargs)
        return ShortMarker(handle) if path.name == "active" else handle

    store = LocalObjectStore(tmp_path)
    with monkeypatch.context() as patch:
        patch.setattr(Path, "open", opening)
        with pytest.raises(ObjectStoreError, match="marker write was incomplete"):
            store.__enter__()
    assert store.stats.closed and not store.directory.exists()


@pytest.mark.parametrize("field", ["kind", "schema_version"])
def test_reference_document_metadata_requires_exact_strings(field):
    class DerivedString(str):
        pass

    document = reference().to_dict()
    document[field] = DerivedString(document[field])
    with pytest.raises(ValidationError, match="exact versioned fields"):
        ObjectRef.from_dict(document)


def test_stats_totals_respect_count_times_per_object_ceiling_not_one_object_ceiling():
    maximum_file = 64 * 1024 * 1024 + 80
    for active in (True, False):
        values = (
            (1, maximum_file + 1, 0, 0, maximum_file + 1, False)
            if active
            else (0, 0, 1, maximum_file + 1, maximum_file + 1, False)
        )
        with pytest.raises(ValidationError, match="inconsistent"):
            ObjectStoreStats(*values)
    valid = ObjectStoreStats(2, 2 * maximum_file, 3, 3 * maximum_file, 5 * maximum_file, False)
    assert valid.reserved_bytes > maximum_file


def test_documented_wire_profile_is_exact_without_writer_constants(tmp_path):
    with LocalObjectStore(tmp_path) as store:
        ref = store.put(b"abc")
        digest = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        assert ref.sha256 == digest
        assert (store.directory / "active").read_bytes() == b"GSSTR001" + bytes.fromhex(
            store.store_id
        )
        expected = (
            b"GSOBJ001"
            + bytes.fromhex(store.store_id)
            + bytes.fromhex(ref.object_id)
            + b"\x03\x00\x00\x00\x00\x00\x00\x00"
            + bytes.fromhex(digest)
            + b"abc"
        )
        assert object_path(store, ref).read_bytes() == expected
