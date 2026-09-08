"""Offline 2 MiB object read through a 4 KiB actor message boundary."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory

from graph_sail import (
    ActorConfig,
    ActorDefinition,
    ActorRegistry,
    ActorSerializationError,
    LocalObjectClient,
    LocalObjectStore,
    ObjectRef,
    ObjectStoreConfig,
    ProcessActor,
)


class ByteSummary:
    def __init__(self, directory: str, store_id: str) -> None:
        self.reader = LocalObjectClient(Path(directory), store_id, max_object_bytes=2 * 1024 * 1024)

    def summarize(self, reference: ObjectRef) -> dict[str, int | str]:
        data = self.reader.get(reference)
        return {
            "pid": os.getpid(),
            "bytes": len(data),
            "sum": sum(data),
            "sha256": hashlib.sha256(data).hexdigest(),
        }


def main() -> None:
    data = bytes(range(256)) * 8192
    registry = ActorRegistry({"summary": ActorDefinition(ByteSummary, ("summarize",))})
    with (
        TemporaryDirectory(prefix="graph-sail-example-") as parent,
        LocalObjectStore(
            parent, ObjectStoreConfig(max_object_bytes=len(data), max_store_bytes=len(data) + 80)
        ) as store,
    ):
        reference = store.put(data)
        assert ObjectRef.from_json(reference.to_json()) == reference
        # Workers finish before the owning store revokes and removes its files.
        with ProcessActor(
            registry,
            "summary",
            args=(str(store.directory), store.store_id),
            config=ActorConfig(max_message_bytes=4096),
        ) as actor:
            result = actor.submit("summarize", args=(reference,)).result(timeout=15)
            assert result["pid"] == actor.pid != os.getpid()
            assert result["bytes"] == 2097152 and result["sum"] == 267386880
            assert result["sha256"] == hashlib.sha256(data).hexdigest()
            try:
                actor.submit("summarize", args=(data,))
            except ActorSerializationError:
                pass
            else:
                raise AssertionError("2 MiB bytes unexpectedly fit a 4 KiB actor message")
            print(
                json.dumps(
                    {
                        "summary": result,
                        "reference_json_bytes": len(reference.to_json()),
                        "actor_message_limit": 4096,
                        "store": store.stats.to_dict(),
                    },
                    indent=2,
                )
            )
        assert actor.exitcode == 0
        store.release(reference)
    assert store.stats.closed and not store.directory.exists()
    print("Actor joined; object released; store closed without retained files.")


if __name__ == "__main__":
    main()
