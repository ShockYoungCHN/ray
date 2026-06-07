# cython: language_level=3
# distutils: language=c++
"""Cython-accelerated spill/restore for Ray's external storage.

Reuses Ray's existing on-disk spill format byte-for-byte so it is
wire-compatible with the Python ``FileSystemStorage`` and the C++
``SpilledObjectReader``:

    [address_len: 8B LE][metadata_len: 8B LE][data_len: 8B LE]
    [owner_address bytes][metadata bytes][data bytes]

Wins over the Python implementation:

1. Inner spill / restore loops are ``cdef`` typed → compiled C ``for``
   loops, no per-iteration Python bytecode dispatch.
2. 24-byte header packing uses ``libc.string::memcpy`` directly into a
   stack-allocated buffer instead of three ``int.to_bytes(8)`` calls +
   ``bytes`` concatenation.
3. File IO routes through ``pyarrow.fs.FileSystem`` (C++ underneath), so
   non-local backends (S3, GCS, HDFS, Azure) come for free without us
   having to ship per-backend cloud SDKs.

Phase 1 scope (this file):
- ``PyArrowFsStorage`` class only; old ``FileSystemStorage`` is left
  intact for fallback.
- Single directory, no rotation yet (current Ray default uses one path
  per node anyway; rotation is a phase-2 concern).
- Pluggable via the ``RAY_EXTERNAL_STORAGE_FAST=1`` env var; off by
  default so existing flows are not perturbed.
"""

from libc.string cimport memcpy
from libc.stdint cimport uint64_t

import os
import random
from collections import defaultdict
from typing import List

# Phase 1 keeps the dependency surface deliberately small — pyarrow.fs
# only (Python API). When we want to fully release the GIL inside the
# write/read loop we'll cimport from ``pyarrow.includes.libarrow_fs`` to
# call the C++ ``arrow::fs::FileSystem`` directly. That is a phase-2
# concern; the current Python API already pushes the actual bytes through
# pyarrow's C++ NativeFile, so the per-write overhead is ~µs and
# dominated by disk IO regardless.
import pyarrow.fs as _pafs


# Stable file extension / prefix, mirrored from external_storage.py so
# the spilled files look identical on disk.
DEFAULT_OBJECT_PREFIX = "ray_spilled_objects"
HEADER_LENGTH = 24  # 3 × 8 bytes (address_len, metadata_len, data_len)


cdef inline void _pack_header_le(
    char *buf,
    uint64_t address_len,
    uint64_t metadata_len,
    uint64_t data_len,
) nogil:
    """Pack 3 little-endian 8-byte size fields into a 24-byte buffer.

    Equivalent to::

        address_len.to_bytes(8, 'little') +
        metadata_len.to_bytes(8, 'little') +
        data_len.to_bytes(8, 'little')

    but inlined as 3 memcpy of 8 bytes each. On x86/ARM (both LE) the
    on-stack ``uint64_t`` already has the right byte order so memcpy is
    a no-op level move. No GIL needed.
    """
    memcpy(buf,      &address_len, 8)
    memcpy(buf + 8,  &metadata_len, 8)
    memcpy(buf + 16, &data_len, 8)


cdef inline void _unpack_header_le(
    const char *buf,
    uint64_t *address_len,
    uint64_t *metadata_len,
    uint64_t *data_len,
) nogil:
    """Inverse of ``_pack_header_le`` for the restore path."""
    memcpy(address_len,  buf,      8)
    memcpy(metadata_len, buf + 8,  8)
    memcpy(data_len,     buf + 16, 8)


cdef class PyArrowFsStorage:
    """Cython-accelerated spill storage backed by pyarrow.fs.

    Same data format and URL scheme as the legacy ``FileSystemStorage``
    so a cluster can roll forward / back between the two without
    rewriting spill files.
    """

    cdef:
        object _fs                    # pyarrow.fs.FileSystem
        list _directory_paths         # absolute paths under each spill dir
        int _current_directory_index
        str _node_id
        object _core_worker_provider  # callable: () -> ray._raylet.CoreWorker

    def __cinit__(
        self,
        str node_id,
        object directory_path,        # str or List[str]
        object core_worker_provider,  # callable so we don't depend on import order
    ):
        self._node_id = node_id
        self._core_worker_provider = core_worker_provider

        # Normalize to list of local directory paths. Phase 1 only
        # supports the LocalFileSystem backend; phase 2 will widen this
        # to accept ``s3://`` / ``gs://`` URIs natively.
        cdef list dirs
        if isinstance(directory_path, str):
            dirs = [directory_path]
        elif isinstance(directory_path, list):
            dirs = list(directory_path)
        else:
            raise TypeError(
                "directory_path must be a str or a list of str; got "
                + repr(type(directory_path))
            )

        # Per-node subdirectory under each configured root, mirroring
        # legacy FileSystemStorage exactly so spill files end up in the
        # same on-disk locations.
        self._directory_paths = []
        for path in dirs:
            full_dir_path = os.path.join(
                path, f"{DEFAULT_OBJECT_PREFIX}_{node_id}"
            )
            os.makedirs(full_dir_path, exist_ok=True)
            if not os.path.exists(full_dir_path):
                raise ValueError(
                    f"The given directory path to store objects, "
                    f"{full_dir_path}, could not be created."
                )
            self._directory_paths.append(full_dir_path)

        self._current_directory_index = random.randrange(
            0, len(self._directory_paths)
        )

        # LocalFileSystem is enough for phase 1. For S3/GCS we'd swap to
        # ``FileSystem.from_uri(...)`` per directory; that requires the
        # config layer to carry a URI instead of a path, which is a
        # separate change.
        self._fs = _pafs.LocalFileSystem()

    # -----------------------------------------------------------------
    # Plasma helpers — delegated to Ray's CoreWorker (unchanged behavior
    # from external_storage.ExternalStorage._get_objects_from_store /
    # _put_object_to_store). We keep these as Python calls because they
    # cross the C++ ↔ Python boundary inside _raylet.pyx; replacing them
    # is out of scope for phase 1.
    # -----------------------------------------------------------------

    cdef object _get_objects_from_store(self, list object_refs):
        return self._core_worker_provider().get_if_local(object_refs)

    cdef _put_object_to_store(
        self, bytes metadata, uint64_t data_size, object file_like,
        object object_ref, bytes owner_address,
    ):
        self._core_worker_provider().put_file_like_object(
            metadata, data_size, file_like, object_ref, owner_address,
        )

    # -----------------------------------------------------------------
    # Spill
    # -----------------------------------------------------------------

    def spill_objects(
        self, list object_refs, list owner_addresses,
    ) -> list:
        """Spill ``object_refs`` to one fused file.  Returns one
        url-with-offset per input ref, in matching order."""
        if not object_refs:
            return []

        # Pick the next spill directory round-robin (same scheme as
        # legacy FileSystemStorage) so multi-disk setups load-balance.
        self._current_directory_index = (
            (self._current_directory_index + 1) % len(self._directory_paths)
        )
        directory_path = self._directory_paths[self._current_directory_index]

        filename = _get_unique_spill_filename(object_refs)
        file_path = os.path.join(directory_path, filename)

        # Fetch all object data up-front. The core_worker call returns
        # a list of (buf, metadata, _) triples in the same order as
        # ``object_refs``.
        ray_object_pairs = self._get_objects_from_store(object_refs)

        cdef:
            int i, n = len(object_refs)
            uint64_t offset = 0
            char header[24]
            uint64_t address_len, metadata_len, data_len
            list keys = []

        out = self._fs.open_output_stream(file_path)
        try:
            for i in range(n):
                buf, metadata, _ = ray_object_pairs[i]
                owner_address = owner_addresses[i]

                if buf is None and len(metadata) == 0:
                    raise ValueError(
                        f"Object {object_refs[i].hex()} does not exist.")

                address_len = len(owner_address)
                metadata_len = len(metadata)
                data_len = 0 if buf is None else len(buf)

                # Header on the stack — no Python intermediate bytes.
                _pack_header_le(header, address_len, metadata_len, data_len)
                # ``out.write`` accepts buffer-protocol objects;
                # constructing a memoryview of the stack buffer would
                # require GIL anyway, and the underlying NativeFile.write
                # always copies, so a small bytes object is fine here.
                out.write(bytes(header[:24]))
                out.write(owner_address)
                if metadata_len:
                    out.write(metadata)
                if data_len:
                    out.write(memoryview(buf))

                written = (
                    HEADER_LENGTH + address_len + metadata_len + data_len
                )
                # URL format matches create_url_with_offset in
                # external_storage.py.  The C++ SpilledObjectReader and
                # the legacy restore path both parse this exact shape.
                keys.append(
                    f"{file_path}?offset={offset}&size={written}".encode()
                )
                offset += written
        finally:
            out.close()

        return keys

    # -----------------------------------------------------------------
    # Restore
    # -----------------------------------------------------------------

    def restore_spilled_objects(
        self, list object_refs, list url_with_offset_list,
    ) -> int:
        """Restore objects identified by url-with-offset back into
        plasma.  Returns total bytes restored."""

        # Group by base_url so each fused file is opened at most once
        # per call. ``items_by_file[base_url]`` holds
        # (caller_index, offset, size) tuples.
        items_by_file = defaultdict(list)
        cdef int i
        for i in range(len(url_with_offset_list)):
            base_url, offset, size = _parse_url_with_offset(
                url_with_offset_list[i].decode()
            )
            items_by_file[base_url].append((i, offset, size))

        cdef:
            uint64_t total = 0
            uint64_t address_len, metadata_len, data_len
            char header[24]
            const char *header_view

        for base_url, items in items_by_file.items():
            # Read in monotonically increasing offset order so the
            # kernel readahead can prefetch sequentially. On HDD this
            # cuts seek time dramatically; on SSD it still helps the
            # page cache warm up cleanly.
            items.sort(key=lambda t: t[1])

            in_file = self._fs.open_input_file(base_url)
            try:
                for caller_idx, offset, size in items:
                    object_ref = object_refs[caller_idx]

                    # Read header (24 bytes) at absolute offset; pyarrow
                    # ``read_at`` is a single C++ pread underneath.
                    # NOTE: pyarrow ``NativeFile.read_at`` returns a
                    # plain ``bytes`` on LocalFileSystem (no zero-copy
                    # Buffer wrapper for small reads). For S3/GCS it
                    # may return a Buffer with ``.to_pybytes()``. The
                    # helper below normalizes both shapes.
                    header_bytes = _read_at_bytes(
                        in_file, HEADER_LENGTH, offset
                    )
                    if len(header_bytes) != HEADER_LENGTH:
                        raise ValueError(
                            f"short header read at {base_url} offset "
                            f"{offset}: got {len(header_bytes)} bytes")
                    # In Cython a ``bytes`` object decays to ``const
                    # char *`` directly via the buffer protocol; no
                    # explicit cast needed.
                    _unpack_header_le(
                        <const char *>header_bytes,
                        &address_len,
                        &metadata_len,
                        &data_len,
                    )

                    expected = (
                        HEADER_LENGTH + address_len
                        + metadata_len + data_len
                    )
                    if expected != size:
                        raise ValueError(
                            f"size mismatch at {base_url} offset {offset}: "
                            f"header says {expected}, url says {size}")
                    total += data_len

                    # Read owner_address + metadata as one block, then
                    # the data block. The plasma put path expects a
                    # file-like object that produces ``data_size`` bytes
                    # when read, so we wrap the in-memory data into a
                    # BytesIO. For phase 2 we can stream directly from
                    # pyarrow's input file into plasma's mmap region to
                    # cut the intermediate copy.
                    body_start = offset + HEADER_LENGTH
                    owner_address = _read_at_bytes(
                        in_file, address_len, body_start
                    )
                    metadata = _read_at_bytes(
                        in_file, metadata_len, body_start + address_len
                    )
                    data_bytes = _read_at_bytes(
                        in_file,
                        data_len,
                        body_start + address_len + metadata_len,
                    )

                    import io
                    self._put_object_to_store(
                        metadata,
                        data_len,
                        io.BytesIO(data_bytes),
                        object_ref,
                        owner_address,
                    )
            finally:
                in_file.close()

        return total

    # -----------------------------------------------------------------
    # Delete
    # -----------------------------------------------------------------

    def delete_spilled_objects(self, list urls):
        """Delete spill files referenced by ``urls``.  Idempotent:
        already-gone files are not an error."""
        seen = set()
        for url in urls:
            base_url, _, _ = _parse_url_with_offset(url.decode())
            if base_url in seen:
                continue
            seen.add(base_url)
            try:
                self._fs.delete_file(base_url)
            except FileNotFoundError:
                # Spilled file may have been deleted by a parallel
                # delete request — not an error.
                pass


# ---------------------------------------------------------------------
# Module-level helpers (kept module-level so test code can call them
# directly without instantiating PyArrowFsStorage).
# ---------------------------------------------------------------------


cdef inline bytes _read_at_bytes(object in_file, uint64_t nbytes, uint64_t offset):
    """Read ``nbytes`` at ``offset`` from a pyarrow ``NativeFile`` and
    normalize the result to a ``bytes`` object.

    pyarrow's ``read_at`` return type varies by backend:
      - LocalFileSystem: returns ``bytes`` directly (no Buffer wrapper)
      - S3/GCS in some pyarrow versions: returns ``pyarrow.Buffer``
    Old pyarrow versions: may return ``pyarrow.Buffer`` even for local.

    We probe for ``to_pybytes`` to handle both shapes uniformly.
    """
    if nbytes == 0:
        return b""
    result = in_file.read_at(nbytes, offset)
    if isinstance(result, bytes):
        return result
    # pyarrow.Buffer path (rare on local FS; common on cloud backends).
    return result.to_pybytes()


def _get_unique_spill_filename(object_refs) -> str:
    """Reproduce the filename scheme used by the legacy
    FileSystemStorage so dual-mode clusters interoperate cleanly."""
    import hashlib, time
    h = hashlib.sha256()
    for ref in object_refs:
        h.update(ref.binary())
    h.update(str(time.time_ns()).encode())
    return h.hexdigest()[:32]


def _parse_url_with_offset(str url_with_offset):
    """Return (base_url, offset, size).  Mirrors
    external_storage.parse_url_with_offset but as a plain tuple to
    avoid the named-tuple overhead in tight loops."""
    import urllib.parse
    parsed_result = urllib.parse.urlparse(url_with_offset)
    query_dict = urllib.parse.parse_qs(parsed_result.query)
    base_url = parsed_result.geturl().split("?")[0]
    if "offset" not in query_dict or "size" not in query_dict:
        raise ValueError(f"Failed to parse URL: {url_with_offset}")
    return (
        base_url,
        int(query_dict["offset"][0]),
        int(query_dict["size"][0]),
    )
