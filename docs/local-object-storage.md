# Local immutable byte objects

`LocalObjectStore` stores actual byte content in exclusive local files. A small
`ObjectRef` can be passed to a task or process actor; a separately configured
`LocalObjectClient` opens and verifies its bytes. This avoids sending the large
payload through the actor's private pipe on every call. It does **not** promise
zero-copy, shared memory, implicit dereferencing or distributed ownership.

Run the offline example:

```bash
python examples/local_objects.py
```

It writes a generated 2 MiB byte sequence, reads it in a distinct actor process
through a 4 KiB message ceiling, checks its length, literal expected sum and
digest, and proves that directly submitting the bytes exceeds that ceiling.
The example joins the worker before releasing its object and closing the store.
It downloads nothing and uses only the standard library.

## API and ownership

```python
from tempfile import TemporaryDirectory
from graph_sail import LocalObjectStore, ObjectRef, ObjectStoreConfig

with (
    TemporaryDirectory() as parent,
    LocalObjectStore(
        parent, ObjectStoreConfig(max_object_bytes=1024, max_store_bytes=4096, max_objects=8)
    ) as owner,
):
    reference = owner.put(b"immutable payload")
    reader = owner.client(max_object_bytes=1024)
    assert reader.get(ObjectRef.from_json(reference.to_json())) == b"immutable payload"
    assert owner.release(reference) is True
    assert owner.release(reference) is False
```

The existing parent directory is explicitly caller-selected. Entering the store
creates a new private directory and an `active` marker; it never opens another
store for writing. Locations are bound to absolute paths when constructed,
without resolving final symlinks. A store cannot be re-entered. Mutations are
serialized between owner threads and rejected from an inherited owner PID before
locking. Pass the read-only client or its directory/store ID to a trusted local
worker, not the owner. Actor factories remain explicitly registered and trusted.

`put` accepts exact immutable `bytes`, not mutable arrays or arbitrary Python
objects. Each call creates a fresh UUID reference; equal bytes are deliberately
not deduplicated. `release` returns whether it removed an owned reservation and
never releases another store's reference. Identity uniqueness uses random UUIDs;
an active UUID collision is refused before changing the existing reservation.
There is no automatic reference counting or lease: copying or serializing a
reference does not prolong its lifetime. End all outstanding users before
releasing an object or closing its owner.

`LocalObjectClient(directory, store_id, max_object_bytes=...)` is read-only and
independently bounded. It returns a fresh immutable `bytes` snapshot on each
`get`. A DAG can explicitly return an `ObjectRef` from one callable and use
`reader.get(context.dependencies["source"])` in its consumer; ordinary task
outputs and actor arguments are otherwise unchanged. There is no implicit
top-level/nested-reference resolution or DAG memory-budget adjustment.

The frozen reference, config, client and statistics records validate their field
contracts. `ObjectRef.to_dict/from_dict` exchange exact versioned fields;
`to_json/from_json` additionally enforce a 1 KiB UTF-8 transport ceiling, reject
duplicate keys, floats/nonfinite numbers, wrong field types, excessive integers,
unknown keys and malformed encoding. Use `from_json` for external JSON text;
`from_dict` cannot detect duplicate keys already discarded by another parser.
Reference data names a store and commitment, never an arbitrary file path.

## Limits and accounting

| Setting | Default and hard ceiling | Meaning |
| --- | --- | --- |
| `max_object_bytes` | 64 MiB | Payload per put/read; a client can choose a smaller independent cap; zero permits only empty payloads |
| `max_store_bytes` | 1 GiB | Sum of reserved object payloads plus their 80-byte headers; minimum 80 bytes |
| `max_objects` | 1,024 | Active plus failed-cleanup reservations |
| Reference JSON | 1 KiB | Encoded reference text, not object payload |

`stats` distinguishes active objects, pending cleanup objects, their reserved
file-byte totals, and actual successful `closed` state. A staging file and its
published hard link share one inode and one reservation, not two capacities.
The full planned size remains reserved even if a failure wrote only a partial
file. Successful removal releases the reservation. The fixed 24-byte marker,
directory entries, allocation-unit rounding, filesystem journals and caller
inputs are not included in this logical file-byte budget. This is not a disk
quota or process-RSS cap. In-memory API limits apply to already-created Python
arguments; readers check the independent reference size and observed file
length before allocating the payload. Hashing and each read still cost O(n),
and multiple simultaneous clients can each allocate their own bounded copy.

The staging file is opened exclusively, written completely, flushed, fsynced,
closed, and hard-linked to a new final name without replacement. If hard links
are unsupported, publication fails; there is no unsafe overwrite fallback.
Both paths and capacity bookkeeping exist before file creation. No post-link
mapping insertion is required. Partial header/payload writes are failures.
No directory fsync or power-loss durable registry is provided: this is a
context-lifetime object store, not a persistent database or recovery service.

## Verification and failure semantics

Every read verifies the directory identity and active marker, opens a nonsymlink
regular object file, checks its exact expected length, matches the opened file
identity/size/mtime with the pre-open observation, checks the complete header,
reads only the independently capped length and verifies SHA-256. File identity,
length and mtime are checked again through both the same open descriptor and
the path before return. A final marker check rejects observed owner revocation.
Corruption is never returned as a successful partial result.

The binary format is internal version 1: an 80-byte little-endian header contains
8-byte magic `GSOBJ001`, 16-byte store UUID, 16-byte object UUID, uint64 payload
length and 32-byte SHA-256, followed by exactly that many bytes. The marker is
`GSSTR001` followed by the 16-byte store UUID. SHA-256 binds bytes to a supplied
reference; it does not authenticate who supplied that reference. Store IDs and
locators are not secrets or authorization tokens. No persistent file is unpickled.
The actor's existing private-pipe serialization still trusts local Python code.

Successful `release` prevents a subsequent `get` from reading that file;
successful `close` revokes the marker and removes all tracked files/directory.
A read racing release/close may either finish with fully verified bytes or raise
an availability/integrity/storage error. There is deliberately no linearizable
lease: revocation immediately after a final marker check cannot retract bytes
already read. Previously returned bytes remain valid caller-owned snapshots.

If a remove fails, the store does not claim that it closed or that capacity is
free. `retry_cleanup()` retries pending object removals without touching active
ones; `close()` also retries marker/directory cleanup. Once close has started,
new puts and new owner clients are refused, even if cleanup failed. A failed
release may leave readable bytes; a failed marker revocation may leave the
marker present. Inspect `stats` and the directory before deciding to retry.

Cleanup only removes tracked, identity-matching regular files. Replaced files,
unknown entries and renamed originals outside the directory are preserved;
there is no recursive delete. If identity capture itself fails after exclusive
creation, capacity remains reserved, but cleanup refuses to guess ownership.
An operator must inspect/remove the ambiguous candidate before retrying.
Ordinary cleanup errors do not replace a body exception or process-control
interrupt; context errors carry a cleanup note. Genuine cleanup interrupts are
not silently swallowed.

These identity checks detect observed changes; they do not sandbox a malicious
same-user writer racing path checks and deletion, or guarantee recovery from
hard OOM, power loss, native syscall interruption or owner termination. The
caller must control the selected local directory and its ancestors. Final
symlinks/nonregular files are refused, but ancestor resolution is the operating
system's responsibility. An abruptly killed owner may leave files and a marker
that still permits reads: clients do not authenticate liveness or automatically
reclaim abandoned storage. The owner does not resume a stale store.

## Evidence and remaining scope

`tests/test_objects.py` exercises real spawned actor and fresh-process readers,
an explicit task dependency flow, the 2 MiB/4 KiB transfer boundary, every header
and payload byte, independent client limits, exact JSON, concurrent admission,
staging and publication failures, short writes, identity races, interrupted
cleanup, retained reservations and preservation of foreign files. These are
correctness checks, not performance claims or hostile-filesystem guarantees.

The pinned [Ray object contracts](https://github.com/ray-project/ray/blob/317c2888eade3c294c4fdb46eff9d9ec290b08f2/doc/source/ray-core/objects.rst)
include distributed references and lifecycle management. This independently
authored local byte backend does not implement those full contracts. Distributed
ownership/GC, spill/reconstruction, zero-copy arrays, network transport, implicit
dereferencing, arbitrary object serialization and cluster integration remain
open in the [whole-repository audit](parity-runtime.md).
