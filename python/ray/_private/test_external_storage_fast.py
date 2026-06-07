"""End-to-end smoke test for ``external_storage_fast.PyArrowFsStorage``.

Runs **without ray.init()** by faking out the plasma client interface
the storage class needs (a thin shim that returns canned objects on
``get_if_local`` and accumulates ``put_file_like_object`` calls).  Lets
us verify the on-disk format + URL bookkeeping in isolation before
wiring into a real Ray cluster.

Usage::

    cd python/ray/_private
    PYTHONPATH=/path/to/ray/python python test_external_storage_fast.py
"""

from __future__ import annotations

import os
import shutil
import tempfile
from dataclasses import dataclass
from typing import List, Optional, Tuple


# ---------------------------------------------------------------------------
# Fakes for the bits of ray._raylet.CoreWorker that PyArrowFsStorage uses
# ---------------------------------------------------------------------------


@dataclass
class _FakeObjectRef:
    """Mimic ``ray.ObjectRef`` just enough for the spill code path."""

    _binary: bytes

    def binary(self) -> bytes:
        return self._binary

    def hex(self) -> str:
        return self._binary.hex()


class _FakeCoreWorker:
    """In-process stand-in for the plasma-backed CoreWorker.

    ``get_if_local`` returns precomputed (buf, metadata, _) triples.
    ``put_file_like_object`` records every restore so the test can
    assert the data round-trips byte-for-byte.
    """

    def __init__(self, store: dict[bytes, Tuple[bytes, bytes]]):
        # ref_binary -> (data, metadata)
        self._store = store
        # Records of restored objects, keyed by ref_binary.
        self.restored: dict[bytes, Tuple[bytes, bytes, bytes]] = {}

    def get_if_local(
        self, object_refs: List[_FakeObjectRef]
    ) -> List[Tuple[bytes, bytes, None]]:
        out: List[Tuple[bytes, bytes, None]] = []
        for ref in object_refs:
            data, metadata = self._store[ref.binary()]
            out.append((data, metadata, None))
        return out

    def put_file_like_object(
        self,
        metadata: bytes,
        data_size: int,
        file_like,
        object_ref: _FakeObjectRef,
        owner_address: bytes,
    ) -> None:
        data = file_like.read()
        assert len(data) == data_size, (
            f"file-like for {object_ref.hex()} produced {len(data)} bytes, "
            f"declared {data_size}"
        )
        self.restored[object_ref.binary()] = (data, metadata, owner_address)


# ---------------------------------------------------------------------------
# Test cases
# ---------------------------------------------------------------------------


def _make_storage(tmpdir: str, fake_worker: _FakeCoreWorker):
    from ray._private import external_storage_fast as esf

    return esf.PyArrowFsStorage(
        node_id="test_node",
        directory_path=tmpdir,
        core_worker_provider=lambda: fake_worker,
    )


def test_roundtrip_single_object() -> None:
    """Spill one object, restore it, assert bytes identical."""
    tmpdir = tempfile.mkdtemp(prefix="esf_test_")
    try:
        ref = _FakeObjectRef(b"\x00" * 28)
        data = b"hello world" * 100
        metadata = b"metadata-string"
        owner = b"owner-address-bytes"

        worker = _FakeCoreWorker({ref.binary(): (data, metadata)})
        storage = _make_storage(tmpdir, worker)

        urls = storage.spill_objects([ref], [owner])
        assert len(urls) == 1, urls
        assert b"?offset=0&size=" in urls[0]

        bytes_restored = storage.restore_spilled_objects([ref], urls)
        assert bytes_restored == len(data), bytes_restored

        restored_data, restored_meta, restored_owner = worker.restored[ref.binary()]
        assert restored_data == data
        assert restored_meta == metadata
        assert restored_owner == owner
        print("OK: roundtrip_single_object")
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_roundtrip_fused_multiple_objects() -> None:
    """Spill many objects into one fused file, restore all out of order."""
    tmpdir = tempfile.mkdtemp(prefix="esf_test_")
    try:
        n = 7
        refs = [_FakeObjectRef(bytes([i]) * 28) for i in range(n)]
        datas = [(f"data-{i}-" * (i + 1)).encode() for i in range(n)]
        metas = [f"meta-{i}".encode() for i in range(n)]
        owners = [f"owner-{i}".encode() for i in range(n)]

        worker = _FakeCoreWorker({r.binary(): (d, m) for r, d, m in zip(refs, datas, metas)})
        storage = _make_storage(tmpdir, worker)

        urls = storage.spill_objects(refs, owners)
        assert len(urls) == n, urls

        # Restore in REVERSE order to exercise the per-file sort by offset.
        bytes_restored = storage.restore_spilled_objects(
            list(reversed(refs)), list(reversed(urls))
        )
        assert bytes_restored == sum(len(d) for d in datas)

        for ref, expected_data, expected_meta, expected_owner in zip(
            refs, datas, metas, owners
        ):
            d, m, o = worker.restored[ref.binary()]
            assert d == expected_data, ref.hex()
            assert m == expected_meta, ref.hex()
            assert o == expected_owner, ref.hex()
        print(f"OK: roundtrip_fused_multiple_objects (n={n})")
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_delete_idempotent() -> None:
    """delete_spilled_objects on already-gone files must not raise."""
    tmpdir = tempfile.mkdtemp(prefix="esf_test_")
    try:
        ref = _FakeObjectRef(b"\x01" * 28)
        worker = _FakeCoreWorker({ref.binary(): (b"data", b"meta")})
        storage = _make_storage(tmpdir, worker)

        urls = storage.spill_objects([ref], [b"owner"])
        storage.delete_spilled_objects(urls)
        # Second delete on same URL — file already gone, must be silent.
        storage.delete_spilled_objects(urls)
        print("OK: delete_idempotent")
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_url_format_matches_legacy() -> None:
    """The URL produced by spill_objects must parse via the legacy
    ``external_storage.parse_url_with_offset`` so cross-mode clusters
    interoperate."""
    from ray._private.external_storage import parse_url_with_offset

    tmpdir = tempfile.mkdtemp(prefix="esf_test_")
    try:
        ref = _FakeObjectRef(b"\x02" * 28)
        worker = _FakeCoreWorker({ref.binary(): (b"payload", b"meta-bytes")})
        storage = _make_storage(tmpdir, worker)

        urls = storage.spill_objects([ref], [b"owner"])
        parsed = parse_url_with_offset(urls[0].decode())
        assert parsed.offset == 0
        # 24-byte header + 5-byte owner + 10-byte meta + 7-byte data = 46
        assert parsed.size == 24 + 5 + 10 + 7, parsed.size
        print("OK: url_format_matches_legacy")
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    test_roundtrip_single_object()
    test_roundtrip_fused_multiple_objects()
    test_delete_idempotent()
    test_url_format_matches_legacy()
    print("\nAll smoke tests passed.")
