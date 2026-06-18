"""hash_shuffle_v3 — file-transport hash shuffle with an out-of-band side-channel.

Design: design/hash-shuffle-bypass.md.

vs v2 (N×M plasma ObjectRefs + reducer ``ray.get``):
- each MAP task writes ONE file (all its partitions, Arrow IPC) and returns ONE
  small handle (path + per-partition offset index + the source node's fetch
  endpoint + a per-shuffle auth token). Driver tracks O(N) handles, not O(N×M)
  refs; bulk never enters plasma.
- a per-node ``ShuffleManager`` Ray actor runs its OWN socket server (§3.4/§3.5)
  that ``pread``s requested byte-ranges and streams them back. The REDUCE task is
  a client of that server: bytes arrive in its **user space** (NOT plasma →
  preserves 1×, §4.6) and are consumed inline. Cross-node this is the real
  out-of-band transport; single-node it is a loopback socket (still the real
  code path, not a direct ``open``).

Reuses v2's PartitionFn / ReduceFn / MapBlockTransformer contracts verbatim, so
group-by / sort / aggregate / join factories compose unchanged.

Scope: works single-node (one actor) and is structured for multi-node (one actor
per node). No planner/ShuffleStrategy wiring yet (driven by a harness).
"""

# todo: for single node, don't even persist to disk, just read directly into heap
# todo: while doing join, maybe pre-check same-key skew in mapphase would be smart
import os
import pickle
import socket
import socketserver
import struct
import tempfile
import threading
import time
from typing import (
    Callable,
    Dict,
    Generator,
    Iterable,
    List,
    Literal,
    Optional,
    Tuple,
    Union,
)

import pyarrow as pa

import ray
from ray._raylet import (
    StreamingGeneratorStats,  # pyrefly: ignore[missing-module-attribute]
)
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
from ray.data._internal.output_buffer import (
    BlockOutputBuffer,
    OutputBlockSizeOption,
)
from ray.data.block import (
    Block,
    BlockExecStats,
    BlockMetadataWithSchema,
    TaskExecWorkerStats,
)

PartitionFn = Callable[[pa.Table], Dict[int, pa.Table]]
ReduceFn = Callable[[int, List[pa.Table]], Iterable[pa.Table]]
MapBlockTransformer = Callable[[pa.Table], pa.Table]

# Per-RecordBatch IPC compression. None preserves zero-copy mmap into Arrow
# (uncompressed bytes are directly the on-wire IPC buffer). LZ4 is the
# recommended default for cross-node clusters: ~2-5x smaller for tabular data
# with sub-µs/MB decompression. ZSTD trades CPU for higher ratio on slow links.
ShuffleCompression = Optional[Literal["lz4", "zstd"]]

# =============================================================================
# Wire protocol.
#
# Each TCP connection has a one-time handshake, then any number of FETCH
# requests (or CLOSE), then teardown. Keep-alive is the default: clients amortize
# TCP handshake cost across many FETCHes, and a single FETCH can request ranges
# from MULTIPLE source files at once (so a reducer needing data from N sources
# colocated on one ShuffleManager pays one network round-trip total, not N).
#
# All multi-byte integers are big-endian. Lengths use the smallest type that
# fits the field's natural bound (u8 / u16 / u32 / u64).
#
#   ──────────────── Handshake (once per TCP connection) ────────────────
#     client → server:
#       u8[4]   magic       = b'V3SH'
#       u16     token_len
#       bytes   token       (UTF-8, token_len bytes)
#     server → client:
#       u8      status      (_STATUS_OK or error)
#
#   ──────────────── Request frames (any number, after handshake) ────────
#     client → server:
#       u8      opcode      (_OPCODE_FETCH | _OPCODE_CLOSE)
#       if FETCH:
#         u16   num_sources
#         repeat num_sources times:
#           u16    path_len
#           bytes  path (UTF-8)            # must resolve under server base_dir
#           u32    num_ranges
#           repeat num_ranges times:
#             u64  offset
#             u64  length
#       if CLOSE: (no body)
#
#   ──────────────── Response frame (one per FETCH) ──────────────────────
#     server → client:
#       u8      status      (_STATUS_OK or error)
#       if OK:
#         repeat num_sources times (in request order):
#           u32    num_ranges          # == request num_ranges
#           repeat num_ranges times (in request order):
#             u32  data_len            # == requested length unless EOF clamp
#             bytes data
#       else:
#         u32   msg_len
#         bytes msg (UTF-8)
# =============================================================================

# V3SH for version 3 shuffle, application-level handshake code
_PROTO_MAGIC = b"V3SH"

# Opcodes
_OPCODE_FETCH = 0x01
_OPCODE_CLOSE = 0x00

# Status codes
_STATUS_OK = 0x00
_STATUS_AUTH_FAIL = 0x01
_STATUS_PATH_DENIED = 0x02   # path resolves outside server's base_dir
_STATUS_NOT_FOUND = 0x03     # path doesn't exist on disk
_STATUS_READ_ERR = 0x04      # IO error reading file content
_STATUS_PROTOCOL_ERR = 0x05  # malformed frame / unknown opcode / etc.


# ----------------------------------------------------------------- Arrow IPC
def _ipc_buffer(
    table: pa.Table, compression: ShuffleCompression = None
) -> pa.Buffer:
    """Serialize an Arrow ``Table`` to an IPC stream and return the result as
    a zero-copy ``pa.Buffer``.

    combine_chunks() helps compression

    Compression is applied per RecordBatch via Arrow IPC's built-in codec
    flag; the resulting stream still has the standard continuation + schema
    + EOS framing, so format-level corruption detection (``_read_ipc``
    raising ``ArrowInvalid``) is preserved. The reader auto-detects
    compression from stream metadata — no caller coordination needed
    beyond the writer.
    """
    if table.num_columns > 0:
        table = table.combine_chunks()
    sink = pa.BufferOutputStream()
    write_opts = (
        pa.ipc.IpcWriteOptions(compression=compression) if compression else None
    )
    with pa.ipc.new_stream(sink, table.schema, options=write_opts) as w:
        w.write_table(table)
    return sink.getvalue()


def _read_ipc(buf: Union[bytes, "pa.Buffer", memoryview]) -> pa.Table:
    """Decode an IPC stream from bytes or a pa.Buffer view (e.g. mmap).

    pa.ipc.open_stream transparently handles compressed payloads, so this
    works for both uncompressed and lz4/zstd IPC streams.
    """
    if isinstance(buf, (bytes, bytearray)):
        source = pa.py_buffer(buf)
    else:
        source = buf
    with pa.ipc.open_stream(source) as r:
        return r.read_all()


# wire framing
def _recvall(sock: socket.socket, n: int) -> bytes:
    out = bytearray()
    while len(out) < n:
        chunk = sock.recv(n - len(out))
        if not chunk:
            raise ConnectionError("peer closed mid-frame")
        out.extend(chunk)
    return bytes(out)


def _recv_u32(sock) -> int:
    return struct.unpack(">I", _recvall(sock, 4))[0]


def _recv_u8(sock) -> int:
    return _recvall(sock, 1)[0]


def _recv_u16(sock) -> int:
    return struct.unpack(">H", _recvall(sock, 2))[0]


def _recv_u64(sock) -> int:
    return struct.unpack(">Q", _recvall(sock, 8))[0]


# ------------------------------------------------------ merge-on-read (§4.12)
class _ScanReq:
    """One reducer's range request, parked while the coordinator pools it with
    other connections' requests for the same file and serves them in one
    offset-ordered pass."""

    __slots__ = ("path", "ranges", "results", "error", "done")

    def __init__(self, path: str, ranges: List[Tuple[int, int]]):
        self.path = path
        self.ranges = ranges
        self.results: List[Optional[bytes]] = [None] * len(ranges)
        self.error: Optional[Exception] = None
        self.done = threading.Event()


class _ScanCoordinator:
    """Server-side merge-on-read (§4.12). A single scanner thread pools the range
    requests arriving across ALL reducer connections within a short window,
    groups them by file, and reads each file in **ascending-offset order** — one
    near-sequential pass that fans each chunk back to its requesting connection.

    The coordination point is this ONE process (the per-node ShuffleManager), so
    we get cross-reducer sequential reads with NO global/driver barrier and NO
    straggler coupling: late requests simply land in the next scan window. Sorting
    by **physical offset** (not partition number) needs no write-side layout
    contract — it works on the flush-order file (§5.3) as-is, which is why
    offset-sort dominates partition-zoning."""

    def __init__(self, scan_window_s: float = 0.003):
        self._scan_window_s = scan_window_s
        self._lock = threading.Lock()
        self._cv = threading.Condition(self._lock)
        self._pending: List[_ScanReq] = []
        self._stop = False
        # debug/metrics (proof the pooling + ordering actually happened)
        self.scans = 0            # number of offset-ordered scan passes
        self.pooled_reqs = 0      # total requests served via the coordinator
        self.max_batch = 0        # largest cross-connection pool in one scan
        self.bytes_served = 0
        self.last_scan_ascending = True  # were the read offsets monotonic?
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def submit(self, path: str, ranges: List[Tuple[int, int]]) -> List[bytes]:
        req = _ScanReq(path, ranges)
        with self._lock:
            self._pending.append(req)
            self._cv.notify()
        req.done.wait()
        if req.error is not None:
            raise req.error
        return req.results

    def stop(self):
        with self._lock:
            self._stop = True
            self._cv.notify()

    def _run(self):
        while True:
            with self._lock:
                while not self._pending and not self._stop:
                    self._cv.wait()
                if self._stop and not self._pending:
                    return
            # batching window: let concurrent reducers' requests accumulate so the
            # pool spans connections (the cross-reducer win, §4.12 P2).
            if self._scan_window_s:
                time.sleep(self._scan_window_s)
            with self._lock:
                batch, self._pending = self._pending, []
            self._serve_batch(batch)

    def _serve_batch(self, batch: List[_ScanReq]):
        by_path: Dict[str, List[_ScanReq]] = {}
        for req in batch:
            by_path.setdefault(req.path, []).append(req)
        self.scans += 1
        self.pooled_reqs += len(batch)
        self.max_batch = max(self.max_batch, len(batch))
        for path, reqs in by_path.items():
            # Pool every range across all reqs for this file, then sort by offset:
            # one ascending sweep instead of per-connection random seeks.
            items = []  # (offset, length, req, idx_within_req)
            for req in reqs:
                for i, (off, length) in enumerate(req.ranges):
                    items.append((off, length, req, i))
            items.sort(key=lambda t: t[0])
            try:
                last_off = -1
                with open(path, "rb") as f:
                    for off, length, req, i in items:
                        if off < last_off:
                            self.last_scan_ascending = False
                        last_off = off
                        f.seek(off)
                        data = f.read(length)
                        req.results[i] = data
                        self.bytes_served += len(data)
                for req in reqs:
                    req.done.set()
            except Exception as e:
                for req in reqs:
                    req.error = e
                    req.done.set()


# ----------------------------------------------------------------- fetch server
# Request  (client→server): u32 len + payload `token\npath\noff,len;off,len;...`
# Response (server→client): u32 count, then per range: u32 len + raw IPC bytes.
#                           error → u32 _MAGIC_ERR.
class _FetchHandler(socketserver.StreamRequestHandler):
    """Per-connection handler implementing the v1 wire protocol.

    Lifecycle: one handshake → loop of FETCH requests → CLOSE (or peer close).
    Each FETCH can carry multiple source paths, so a reducer with N sources on
    this node pays only one TCP round-trip's worth of handshake/setup overhead
    no matter how many sources it asks for.
    """

    def handle(self):
        srv = self.server
        sock = self.connection
        try:
            if not self._handshake(sock, srv):
                return
            self._serve_loop(sock, srv)
        except (ConnectionError, OSError):
            # Peer closed mid-read, or socket dead — normal teardown.
            pass

    # ── handshake ───────────────────────────────────────────────────────
    @staticmethod
    def _handshake(sock, srv) -> bool:
        """Read magic + token, send status. Returns True on success."""
        magic = _recvall(sock, 4)
        if magic != _PROTO_MAGIC:
            # Send PROTOCOL_ERR even though the peer probably isn't speaking our
            # protocol — best-effort, then bail.
            try:
                sock.sendall(struct.pack(">B", _STATUS_PROTOCOL_ERR))
            except OSError:
                pass
            return False
        token_len = _recv_u16(sock)
        token = _recvall(sock, token_len).decode("utf-8")
        if token != srv.token:
            sock.sendall(struct.pack(">B", _STATUS_AUTH_FAIL))
            return False
        sock.sendall(struct.pack(">B", _STATUS_OK))
        return True

    # ── main request loop ───────────────────────────────────────────────
    def _serve_loop(self, sock, srv):
        while True:
            opcode = _recv_u8(sock)
            if opcode == _OPCODE_CLOSE:
                return
            if opcode != _OPCODE_FETCH:
                self._send_error(
                    sock, _STATUS_PROTOCOL_ERR, f"unknown opcode {opcode}"
                )
                return
            self._handle_fetch(sock, srv)

    # ── FETCH ────────────────────────────────────────────────────────────
    def _handle_fetch(self, sock, srv):
        """Parse a FETCH frame, validate, serve, write response."""
        # Parse the request body.
        num_sources = _recv_u16(sock)
        requests: List[Tuple[str, List[Tuple[int, int]]]] = []
        for _ in range(num_sources):
            path_len = _recv_u16(sock)
            path = _recvall(sock, path_len).decode("utf-8")
            # path-traversal guard: realpath must be inside the server base_dir.
            real = os.path.realpath(path)
            if not real.startswith(srv.base_dir):
                # Drain the rest of the request before answering so the socket
                # remains in a well-defined state for future FETCHes on this
                # connection (the client's protocol contract is "send full
                # request, then read full response").
                num_ranges = _recv_u32(sock)
                _recvall(sock, num_ranges * 16)   # u64 offset + u64 length
                for _ in range(num_sources - len(requests) - 1):
                    pl = _recv_u16(sock)
                    _recvall(sock, pl)
                    nr = _recv_u32(sock)
                    _recvall(sock, nr * 16)
                self._send_error(
                    sock, _STATUS_PATH_DENIED, f"path outside base_dir: {path}"
                )
                return

            num_ranges = _recv_u32(sock)
            ranges = []
            for _ in range(num_ranges):
                offset = _recv_u64(sock)
                length = _recv_u64(sock)
                ranges.append((offset, length))
            requests.append((real, ranges))

        # Serve: either via the per-node ScanCoordinator (offset-sorted batched
        # reads pooled across reducer connections) or via a direct per-source
        # read. Either way, results come back in REQUEST order so the client
        # can map them positionally.
        try:
            if srv.coordinator is not None:
                results_per_source = [
                    srv.coordinator.submit(path, ranges)
                    for path, ranges in requests
                ]
            else:
                results_per_source = [
                    self._read_direct(path, ranges)
                    for path, ranges in requests
                ]
        except FileNotFoundError as e:
            self._send_error(sock, _STATUS_NOT_FOUND, str(e))
            return
        except OSError as e:
            self._send_error(sock, _STATUS_READ_ERR, str(e))
            return

        # Send response.
        sock.sendall(struct.pack(">B", _STATUS_OK))
        for source_results in results_per_source:
            sock.sendall(struct.pack(">I", len(source_results)))
            for data in source_results:
                sock.sendall(struct.pack(">I", len(data)))
                sock.sendall(data)
                srv.bytes_served += len(data)

    @staticmethod
    def _read_direct(path: str, ranges: List[Tuple[int, int]]) -> List[bytes]:
        """Direct (no-coordinator) read path. Sort by offset within the file for
        one near-sequential sweep, but return in original request order."""
        sorted_pairs = sorted(enumerate(ranges), key=lambda p: p[1][0])
        out: List[Optional[bytes]] = [None] * len(ranges)
        with open(path, "rb") as f:
            for orig_idx, (off, length) in sorted_pairs:
                f.seek(off)
                out[orig_idx] = f.read(length)
        return [b for b in out if b is not None] if len(out) == len(ranges) else out  # type: ignore[return-value]

    @staticmethod
    def _send_error(sock, status: int, msg: str):
        payload = msg.encode("utf-8")
        try:
            sock.sendall(struct.pack(">B", status))
            sock.sendall(struct.pack(">I", len(payload)))
            sock.sendall(payload)
        except OSError:
            pass


class _ThreadingServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True
    # many reducers × per-source prefetch open concurrent connections; the default
    # backlog of 5 RSTs them. Size for fan-in (cf. §4.7 fan-out cap is the real bound).
    request_queue_size = 256


@ray.remote
class ShuffleManager:
    """Per-node fetch service (§3.4): owns a socket server that serves byte-ranges
    of local shuffle files to remote reducers. Survives individual map/reduce
    workers; in a real cluster, one per node (NodeAffinity)."""

    def __init__(
        self,
        base_dir: str,
        token: str,
        merge_on_read: bool = True,
        scan_window_s: float = 0.003,
    ):
        self.base_dir = os.path.realpath(base_dir)
        os.makedirs(self.base_dir, exist_ok=True)
        self.token = token
        ip = ray.util.get_node_ip_address()
        self._server = _ThreadingServer((ip, 0), _FetchHandler)
        self._server.token = token
        self._server.base_dir = self.base_dir
        self._server.bytes_served = 0
        # merge-on-read (§4.12): a single per-node coordinator pools fetch
        # requests across connections and serves them in offset order.
        self._server.coordinator = (
            _ScanCoordinator(scan_window_s) if merge_on_read else None
        )
        self._host, self._port = self._server.server_address
        t = threading.Thread(target=self._server.serve_forever, daemon=True)
        t.start()

    def endpoint(self) -> Tuple[str, int]:
        return (self._host, self._port)

    def base(self) -> str:
        return self.base_dir

    def bytes_served(self) -> int:
        return self._server.bytes_served

    def scan_stats(self) -> Dict[str, object]:
        """merge-on-read metrics (§4.12): None if merge-on-read is disabled."""
        c = self._server.coordinator
        if c is None:
            return {"merge_on_read": False}
        return {
            "merge_on_read": True,
            "scans": c.scans,                # offset-ordered scan passes
            "pooled_reqs": c.pooled_reqs,     # requests served via coordinator
            "max_batch": c.max_batch,         # largest cross-connection pool
            "last_scan_ascending": c.last_scan_ascending,
            "bytes_served": c.bytes_served,
        }


# =============================================================================
# Client side of the v1 wire protocol.
#
# ``_ShuffleConnection`` is the primitive: an open, handshake'd TCP connection
# to one ShuffleManager. Once open, you can ``fetch(...)`` or
# ``fetch_to_files(...)`` any number of times, each with potentially many
# source paths and many ranges per source. Use within a ``with`` block to make
# sure the CLOSE opcode is sent and the socket is torn down properly.
#
# The two convenience wrappers ``_fetch_ranges`` and ``_fetch_ranges_to_file``
# preserve the single-source API for tests and ad-hoc callers; they internally
# open a connection, do one FETCH, and close.
# =============================================================================


class _ShuffleConnection:
    """A keep-alive client connection to one ShuffleManager.

    Wraps a TCP socket that has already completed the handshake. Each
    ``fetch*`` call sends a single FETCH frame and reads the matching response
    frame. The socket can carry many such fetches before being closed, so the
    cost of TCP handshake + auth is amortized across all of them.
    """

    __slots__ = ("_sock", "_closed", "_endpoint")

    def __init__(self, sock: socket.socket, endpoint: Tuple[str, int]):
        self._sock = sock
        self._endpoint = endpoint
        self._closed = False

    # ── lifecycle ────────────────────────────────────────────────────────
    def __enter__(self) -> "_ShuffleConnection":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def close(self) -> None:
        if self._closed:
            return
        # Best-effort: send the CLOSE opcode so the server can exit its handler
        # cleanly without seeing a peer-reset.
        try:
            self._sock.sendall(struct.pack(">B", _OPCODE_CLOSE))
        except OSError:
            pass
        try:
            self._sock.close()
        except OSError:
            pass
        self._closed = True

    # ── FETCH (returns bytes per source per range) ───────────────────────
    def fetch(
        self,
        sources: List[Tuple[str, List[Tuple[int, int]]]],
    ) -> List[List[bytes]]:
        """Single FETCH carrying ``sources`` and returning the matching bytes.

        ``sources`` is a list of ``(path, [(offset, length), ...])`` tuples;
        the returned list is parallel — index ``i`` corresponds to
        ``sources[i]``, and each inner list is parallel to that source's
        ``ranges`` argument. Useful for in-memory consumers (tests, direct
        ``_read_ipc(buf)`` callers); production reduce path uses
        :meth:`fetch_to_files` instead to avoid full per-range bytes.
        """
        self._send_fetch_request(sources)
        status = _recv_u8(self._sock)
        if status != _STATUS_OK:
            self._raise_error_response(status)
        out: List[List[bytes]] = []
        for path, ranges in sources:
            n = _recv_u32(self._sock)
            if n != len(ranges):
                # Protocol contract violation: server's per-source range count
                # must match what we sent. Raise instead of ``assert`` so the
                # check survives ``python -O`` (where ``assert`` is stripped
                # and a mismatch would silently corrupt the read stream).
                raise RuntimeError(
                    f"protocol error: server returned {n} ranges for "
                    f"{path!r}, expected {len(ranges)}"
                )
            buf: List[bytes] = []
            for _ in range(n):
                length = _recv_u32(self._sock)
                buf.append(_recvall(self._sock, length))
            out.append(buf)
        return out

    # ── FETCH (streams response into files) ──────────────────────────────
    def fetch_to_files(
        self,
        sources: List[Tuple[str, List[Tuple[int, int]]]],
        out_files: List[str],
        chunk_size: int = 64 * 1024,
    ) -> None:
        """Single FETCH whose per-source response streams into ``out_files[i]``.

        Each output file gets self-contained framing (``u32 num_ranges`` then
        per range ``u32 len`` + raw bytes). Kept around for tests and for any
        caller that wants per-source files (one mmap per source).

        User-space peak is ``chunk_size`` regardless of how big any range is.
        """
        if len(sources) != len(out_files):
            # Caller bug, not a protocol violation — but still must be a real
            # raise so ``python -O`` doesn't swallow it.
            raise ValueError(
                f"sources/out_files length mismatch: "
                f"{len(sources)} vs {len(out_files)}"
            )
        self._send_fetch_request(sources)
        status = _recv_u8(self._sock)
        if status != _STATUS_OK:
            self._raise_error_response(status)

        chunk = bytearray(chunk_size)
        view = memoryview(chunk)
        opened_files: List[str] = []
        try:
            for (path, ranges), out_path in zip(sources, out_files):
                num_ranges = _recv_u32(self._sock)
                if num_ranges != len(ranges):
                    raise RuntimeError(
                        f"protocol error: server returned {num_ranges} "
                        f"ranges for {path!r}, expected {len(ranges)}"
                    )
                with open(out_path, "wb") as f:
                    opened_files.append(out_path)
                    f.write(struct.pack(">I", num_ranges))
                    for _ in range(num_ranges):
                        length = _recv_u32(self._sock)
                        f.write(struct.pack(">I", length))
                        remaining = length
                        while remaining > 0:
                            want = min(remaining, chunk_size)
                            n = self._sock.recv_into(view[:want], want)
                            if n == 0:
                                raise ConnectionError(
                                    "peer closed mid-fetch"
                                )
                            f.write(view[:n])
                            remaining -= n
        except Exception:
            # Best-effort cleanup so failed multi-source fetches don't leak
            # partial files (the connection itself is unusable past this point;
            # caller's ``with`` block will close it).
            for p in opened_files:
                try:
                    os.unlink(p)
                except OSError:
                    pass
            raise

    # ── FETCH (streams response APPENDED into one open file) ────────────
    def fetch_into(
        self,
        sources: List[Tuple[str, List[Tuple[int, int]]]],
        out_file_obj,
        chunk_size: int = 64 * 1024,
    ) -> None:
        """Single FETCH whose entire response streams into ``out_file_obj``
        as a flat sequence of ``(u32 len + ipc_bytes)*`` records, one record
        per shard across all sources.

        The reader walks the file as a simple stream:
            while file.tell() < file_size:
                length = u32; data = file.read(length); decode(data)

        Source ordering across calls is preserved (sources within a FETCH
        are written in request order; calls happen sequentially), so a
        """
        self._send_fetch_request(sources)
        status = _recv_u8(self._sock)
        if status != _STATUS_OK:
            self._raise_error_response(status)

        chunk = bytearray(chunk_size)
        view = memoryview(chunk)
        for path, ranges in sources:
            num_ranges = _recv_u32(self._sock)
            if num_ranges != len(ranges):
                raise RuntimeError(
                    f"protocol error: server returned {num_ranges} ranges "
                    f"for {path!r}, expected {len(ranges)}"
                )
            for _ in range(num_ranges):
                length = _recv_u32(self._sock)
                out_file_obj.write(struct.pack(">I", length))
                remaining = length
                while remaining > 0:
                    want = min(remaining, chunk_size)
                    n = self._sock.recv_into(view[:want], want)
                    if n == 0:
                        raise ConnectionError("peer closed mid-fetch")
                    out_file_obj.write(view[:n])
                    remaining -= n

    # internals
    def _send_fetch_request(
        self, sources: List[Tuple[str, List[Tuple[int, int]]]]
    ) -> None:
        """Serialize a FETCH frame over the wire (see protocol comment above)."""
        sock = self._sock
        sock.sendall(struct.pack(">B", _OPCODE_FETCH))
        sock.sendall(struct.pack(">H", len(sources)))
        for path, ranges in sources:
            path_bytes = path.encode("utf-8")
            sock.sendall(struct.pack(">H", len(path_bytes)))
            sock.sendall(path_bytes)
            sock.sendall(struct.pack(">I", len(ranges)))
            for offset, length in ranges:
                sock.sendall(struct.pack(">QQ", offset, length))

    def _raise_error_response(self, status: int) -> None:
        """Read error message body and raise an appropriate Python exception."""
        msg_len = _recv_u32(self._sock)
        msg = _recvall(self._sock, msg_len).decode("utf-8", errors="replace")
        if status == _STATUS_AUTH_FAIL:
            raise PermissionError(f"ShuffleManager auth failed: {msg}")
        if status == _STATUS_PATH_DENIED:
            raise PermissionError(f"path denied by ShuffleManager: {msg}")
        if status == _STATUS_NOT_FOUND:
            raise FileNotFoundError(msg)
        if status == _STATUS_READ_ERR:
            raise OSError(msg)
        raise RuntimeError(f"ShuffleManager error status={status}: {msg}")


def open_shuffle_connection(
    endpoint: Tuple[str, int],
    token: str,
) -> _ShuffleConnection:
    """Open a TCP connection to ``endpoint`` and complete the v1 handshake.

    Raises ``PermissionError`` on auth failure, ``ConnectionError`` if the
    server isn't reachable, ``RuntimeError`` on protocol errors.
    """
    sock = socket.create_connection(endpoint)
    try:
        token_bytes = token.encode("utf-8")
        sock.sendall(_PROTO_MAGIC)
        sock.sendall(struct.pack(">H", len(token_bytes)))
        sock.sendall(token_bytes)
        status = _recv_u8(sock)
        if status == _STATUS_OK:
            return _ShuffleConnection(sock, endpoint)
        if status == _STATUS_AUTH_FAIL:
            sock.close()
            raise PermissionError("ShuffleManager handshake: bad token")
        sock.close()
        raise RuntimeError(
            f"ShuffleManager handshake: unexpected status {status}"
        )
    except Exception:
        try:
            sock.close()
        except OSError:
            pass
        raise


# map / reduce task body
ShuffleHandle = dict  # {path, index:{pid:[(off,len)]}, endpoint:(host,port), token}


@ray.remote
def v3_map_task(
    *blocks: pa.Table,
    partition_fn: PartitionFn,
    num_partitions: int,
    out_dir: str,
    map_id: int,
    shuffle_id: str,
    token: str,
    transformer: MapBlockTransformer = None,
    pool_budget_bytes: int = 16 * 1024 * 1024,
    compression: ShuffleCompression = None,
    fsync_on_close: bool = True,
) -> ShuffleHandle:
    """Streaming write with a SHARED, resource-accounted staging pool, sealed
    via atomic ``rename``.

    **One knob — ``pool_budget_bytes`` — bounds both ends of the map:**

    *Output side.* All post-hash partition buckets share one byte-accounted pool
    of size ``pool_budget_bytes``. On overflow the LARGEST bucket is spilled
    (flushed) to the file, then processing continues — spill-on-overflow, not
    unbounded accumulation. So total staging is bounded by ``pool_budget_bytes``
    **independent of the partition count M** (a fixed per-partition threshold
    would be M × threshold — wrong for large M).

    *Input side (auto, no separate knob).* The v2 ``PartitionFn`` contract is
    ``Table → Dict[pid, Table]`` — one call materializes **all M shards at once**
    (≈ a full copy of its input) and the returned dict pins them in RAM regardless
    of how fast the pool flushes. So calling it on a whole large block forces a
    ~2×S peak the pool *cannot* reduce (measured: pool 256MB→4KB all stay at
    2.0×S). The fix is to feed ``partition_fn`` in row-batches small enough that
    one batch's transient shards stay ~pool-bounded — and we size that batch
    **from the pool budget**: ``batch_rows ≈ pool_budget_bytes / avg_row_bytes``.
    If the whole block already fits the pool (the common target-block case) we skip
    batching entirely — no extra ``partition_fn`` calls, no extra chunks.

    Net: **map peak ≈ S (input block, the floor) + O(pool_budget_bytes)**, set by
    the single pool knob. Shrinking the pool lowers the peak *and* auto-refines the
    input batch; it costs more, smaller chunks (the read-side fragmentation that
    §4.12 merge-on-read / §5.9 sort-merge fold back).

    The blocking ``f.write`` provides natural OS backpressure (slow disk → write
    blocks → the map throttles itself).

    **File-level seal via atomic ``rename``.**  Writes go to ``map_{i}.shf.tmp``
    first; once all flushes are done we run a final ``f.flush()`` (+ optional
    ``os.fdatasync`` when ``fsync_on_close``), a size sanity check against the
    index, then ``os.rename(tmp, final)``. POSIX guarantees ``rename`` is
    atomic, so reducers (or the ShuffleManager serving them) see either
      * NO file at the final path — handle hasn't been published yet, or
      * a COMPLETE, size-validated file — every byte in the index is on disk.
    This is a stronger guarantee than Arrow IPC's per-shard magic alone: the
    framing detects corrupted shards but cannot tell a truncated file
    (mapper crashed mid-write) from a complete one until you actually try to
    decode the truncated tail.

    ``fsync_on_close=True`` (the default) trades one ``fdatasync`` worth of
    latency for durability against node crash between handle return and disk
    writeback. Same-node readers go through page cache and don't need this,
    but cross-node reducers (and Ray lineage's "did the task actually
    succeed" semantics) do.

    Caveat: this still holds up to M staging buffers; for very large M the real
    answer is sort-spill-merge (one bounded sort buffer, spill sorted runs, merge
    to one partition-contiguous file — §5.9), which gets low peak AND ~M chunks at
    once. Not in this PoC.
    """
    # Lookup-or-create the local node's ShuffleManager. ``get_if_exists=True``
    # makes this idempotent: concurrent mappers on the same node share one
    # actor; cross-node task retry just spawns a fresh manager on the new
    # node. The returned ActorHandle goes into the ShuffleHandle so Ray
    # ref-counting keeps the manager alive until all reducers have dropped
    # their handle refs (binds file lifetime to index lifetime).
    node_id = ray.get_runtime_context().get_node_id()
    manager = ShuffleManager.options(
        name=f"shuffle_mgr:{shuffle_id}:{node_id}",
        namespace="ray_data_shuffle_v3",
        get_if_exists=True,
        max_restarts=-1,
        scheduling_strategy=NodeAffinitySchedulingStrategy(
            node_id, soft=False
        ),
        num_cpus=0,
    ).remote(out_dir, token)

    os.makedirs(out_dir, exist_ok=True)
    final_path = os.path.join(out_dir, f"map_{map_id}.shf")
    # Write to a temp sibling first; only ``rename`` once we've verified the
    # full file. Same directory so ``rename`` stays a metadata-only operation
    # on the destination filesystem (no cross-device copy fallback).
    tmp_path = final_path + ".tmp"
    # index = {partition id, {offset, length}}
    index: Dict[int, List[Tuple[int, int]]] = {}
    staging: Dict[int, List[pa.Table]] = {}
    staging_bytes: Dict[int, int] = {}
    peak_inflight = 0  # max bytes of partition output held at once (excludes input)

    def _partition_units(blk):
        """Yield (pid, shard).
        yield whole-block when the block already fits the pool (no overhead),
        else split into pool-sized row-batches (bounds the 2×S partition spike
        to S + O(pool))."""
        if blk.num_rows == 0:
            return
        avg_row = max(1, blk.nbytes // blk.num_rows)
        batch_rows = max(1, pool_budget_bytes // avg_row)
        if blk.num_rows <= batch_rows:
            # block's partition spike is already ≤ pool → whole-block, no overhead
            for pid, shard in partition_fn(blk).items():
                yield pid, shard
        else:
            for batch in blk.to_batches(max_chunksize=batch_rows):
                bt = pa.Table.from_batches([batch], schema=blk.schema)
                for pid, shard in partition_fn(bt).items():
                    yield pid, shard

    final_size_on_close = -1
    try:
        with open(tmp_path, "wb") as f:
            def flush(pid: int):
                shards = staging.get(pid)
                if not shards:
                    return
                tbl = (
                    pa.concat_tables(shards) if len(shards) > 1 else shards[0]
                )

                buf = _ipc_buffer(tbl, compression=compression)
                off = f.tell()

                # blocking write = natural backpressure on slow disk
                f.write(memoryview(buf))
                index.setdefault(pid, []).append((off, buf.size))
                staging[pid] = []
                staging_bytes[pid] = 0

            def pool_size() -> int:
                return sum(staging_bytes.values())

            for blk in blocks:
                if transformer is not None:
                    blk = transformer(blk)
                for pid, shard in _partition_units(blk):
                    if not shard.num_rows:
                        continue
                    staging.setdefault(pid, []).append(shard)
                    staging_bytes[pid] = (
                        staging_bytes.get(pid, 0) + shard.nbytes
                    )
                    peak_inflight = max(peak_inflight, pool_size())
                    # Pool overflow → spill the LARGEST bucket(s) until back under
                    # budget. Bounds total staging to pool_budget_bytes,
                    # M-independent.
                    while pool_size() >= pool_budget_bytes:
                        victim = max(staging_bytes, key=staging_bytes.get)
                        if staging_bytes[victim] == 0:
                            break
                        flush(victim)
            for pid in list(staging.keys()):  # final flush of remainders
                flush(pid)

            # ---- end-of-write: flush + (optional) fdatasync + sanity check --
            # Per-flush we do NOT fsync — page cache is fine for steady-state
            # backpressure (the OS already throttles when dirty pages pile up).
            # The single end-of-write sync below is what pairs with the atomic
            # rename below to give "file exists == fully on disk".
            f.flush()
            if fsync_on_close:
                # ``fdatasync`` flushes data pages without bothering to update
                # metadata timestamps — strictly faster than ``fsync`` and
                # sufficient for our "bytes are on disk" guarantee. Not all
                # platforms expose it (it's Linux-only in the Python stdlib),
                # so fall back to ``fsync`` elsewhere (e.g. macOS).
                fdatasync = getattr(os, "fdatasync", None)
                if fdatasync is not None:
                    fdatasync(f.fileno())
                else:
                    os.fsync(f.fileno())

            # Cross-check that ``f.tell()`` (what we actually wrote) matches the
            # max ``offset + length`` we recorded in the index. A mismatch means
            # either the index drifted from the writes (logic bug) or some
            # write silently short-wrote (filesystem error). Either way we
            # refuse to publish — the .tmp gets cleaned up by the except below.
            final_size_on_close = f.tell()
            if index:
                expected_size = max(
                    off + length
                    for ranges in index.values()
                    for off, length in ranges
                )
            else:
                expected_size = 0
            if final_size_on_close != expected_size:
                raise RuntimeError(
                    f"v3_map_task: file size mismatch — wrote "
                    f"{final_size_on_close} bytes, index implies "
                    f"{expected_size}. Refusing to publish corrupt file."
                )

        # ---- atomic publish ------------------------------------------------
        # rename(2) on the same filesystem is atomic: any concurrent reader
        # either sees the old (here: nonexistent) name or the fully written
        # new file. No "half-published" state is observable.
        os.rename(tmp_path, final_path)
    except Exception:
        # Best-effort cleanup of the unpublished .tmp so failed attempts
        # don't leak files in ``out_dir``. (Ray lineage will retry the task
        # and write a fresh .tmp anyway.)
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise

    return {
        "path": os.path.realpath(final_path),
        "index": index,
        # ActorHandle to this node's ShuffleManager. Reducer calls
        # ``ray.get(manager.endpoint.remote())`` at fetch time to get the
        # CURRENT (host, port) — survives actor restart on a new port.
        # Embedding the handle also makes Ray ref-count the actor for us:
        # when the last ShuffleHandle ref is dropped, the actor dies, which
        # is the natural end-of-shuffle signal.
        "manager": manager,
        "token": token,
        "num_partitions": num_partitions,
        "peak_inflight_bytes": peak_inflight,  # debug: output held at once
        # Total bytes written, post-seal. Lets the reducer (or operator)
        # cross-check against the index without needing an os.stat
        "total_bytes": final_size_on_close,
        # Informational: IPC reader auto-detects from per-batch metadata, so
        # reducers do not need this field to decode. Useful for metrics and
        # operator-level decisions (e.g. skip same-node mmap zero-copy path
        # when bytes are compressed and decode will copy anyway).
        "compression": compression,
    }


class ShuffleFetchError(RuntimeError):
    """Raised when a side-channel fetch fails (source gone / file lost). Surfaced
    so the executor/lineage can re-run the producer mapper (§4.10/§11 Q1)."""


# --------------------------------------------------------- prefetch-file framing
# Each prefetched source lands in its own file under ``prefetch_dir`` with:
#     u32 count           # number of IPC shards from this source
#     repeat count times:
#         u32 len_i        # bytes of the next IPC stream
#         <IPC stream_i>   # complete Arrow IPC stream as written by the mapper
# Why file-backed instead of List[bytes] in memory:
#   * bounds reducer peak RSS to roughly one source's bytes during decode
#     (the unbounded user-space accumulation across ALL sources was the
#     biggest gap in the old reduce path under partition skew);
#   * kernel page cache automatically shares pages and lets memory pressure
#     evict cold prefetch data — a soft "spill" for free, with no explicit
#     reducer-side memory accounting;
#   * mmap'd reads give zero-copy ``pa.Buffer`` views into Arrow IPC decode
#     for the uncompressed case (compressed IPC always decompresses into
#     fresh buffers, so the copy is unavoidable there).
def _write_prefetch_file(path: str, bufs: List[bytes]) -> None:
    """Stage one fetched source's IPC shards to ``path`` with framing.

    We deliberately do not ``fsync``: the file is local-only and consumed by
    the very same reduce task within seconds, so kernel page cache is
    sufficient — and is exactly what makes the subsequent ``pa.memory_map``
    decode hit cache rather than re-read from disk.
    """
    with open(path, "wb") as f:
        f.write(struct.pack(">I", len(bufs)))
        for b in bufs:
            f.write(struct.pack(">I", len(b)))
            f.write(b)


_DEFAULT_MAX_BYTES_PER_FETCH = 256 * 1024 * 1024  # 256 MiB per FETCH frame


_PrefetchMember = Tuple[int, str, List[Tuple[int, int]]]


def _prefetch_node_into(
    out_file_obj,
    manager: "ray.actor.ActorHandle",
    token: str,
    members: List[_PrefetchMember],
    max_bytes_per_fetch: int,
) -> None:
    """Open ONE keep-alive connection to ``manager``'s endpoint and stream
    every member's shards into ``out_file_obj`` via 1+ multi-source FETCH
    frames, each bounded by ``max_bytes_per_fetch``.

    Endpoint is resolved at call-time via ``manager.endpoint.remote()`` —
    survives ShuffleManager restart on a new port. If the first connect
    fails with a transient error (typical sign of mid-restart), re-resolve
    once and retry; persistent failure surfaces as ShuffleFetchError so the
    operator layer can decide what to do.
    """
    def _resolve() -> Tuple[str, int]:
        return ray.get(manager.endpoint.remote())

    endpoint = _resolve()
    try:
        try:
            conn_cm = open_shuffle_connection(endpoint, token)
        except (ConnectionRefusedError, ConnectionResetError, OSError):
            # Manager process may have just restarted on a new port —
            # re-resolve once and try again.
            endpoint = _resolve()
            conn_cm = open_shuffle_connection(endpoint, token)
        with conn_cm as conn:
            for batch in _chunk_members_by_bytes(members, max_bytes_per_fetch):
                sources = [
                    (src_path, src_ranges)
                    for _idx, src_path, src_ranges in batch
                ]
                conn.fetch_into(sources, out_file_obj)
    except Exception as e:
        # Wrap any underlying error in the typed shuffle exception so the
        # operator/lineage layer above can retry the source mapper.
        raise ShuffleFetchError(
            f"fetch from {endpoint} (sources={len(members)}) failed: {e}"
        ) from e


def _chunk_members_by_bytes(
    members: List[_PrefetchMember],
    max_bytes: int,
) -> Iterable[List[_PrefetchMember]]:
    """Yield consecutive sub-batches of ``members`` whose total requested
    bytes (sum of ranges' ``length``) stay within ``max_bytes``.

    A single member larger than the budget gets its own singleton batch — we
    never split a source's ranges across FETCHes (would require dedicated
    multi-FETCH stitching). For typical workloads this is fine: the per-FETCH
    budget is much larger than any one source's partition slice.
    """
    cur: List[Tuple[int, str, List[Tuple[int, int]]]] = []
    cur_bytes = 0
    for member in members:
        _idx, _path, src_ranges = member
        size = sum(length for _off, length in src_ranges)
        if cur and cur_bytes + size > max_bytes:
            yield cur
            cur = []
            cur_bytes = 0
        cur.append(member)
        cur_bytes += size
    if cur:
        yield cur


@ray.remote(max_calls=1)
def v3_reduce_task(
    handles: List[ShuffleHandle],
    partition_id: int,
    reduce_fn: ReduceFn,
    prefetch_dir: Optional[str] = None,
    max_bytes_per_fetch: int = _DEFAULT_MAX_BYTES_PER_FETCH,
    target_max_block_size: Optional[int] = None,
    streaming: bool = True,
) -> Generator[Union[Block, bytes], None, None]:
    """Fetch one partition's shards, decode mmap'd prefetch file, stream
    ``reduce_fn`` output as (block, pickled metadata) pairs.

    Phase 1 (single-threaded): per ShuffleManager node one keep-alive TCP
    connection; one or more multi-source FETCHes bounded by
    ``max_bytes_per_fetch``; all responses appended into one ``prefetch.bin``.
    Phase 2: mmap that file and walk shards. Bytes stay out of Plasma (§4.6).

    Streaming-generator protocol matches v2's ``_shuffle_reduce_task``:
        yield Block
        yield pickle(BlockMetadataWithSchema)
    So the operator wraps this in a ``DataOpTask`` and feeds each pair into
    its output queue with proper backpressure.

    ``streaming=True``: flush incrementally — ``reduce_fn`` is invoked each
    time accumulated input crosses ``target_max_block_size``. Bounds peak
    accumulator memory but requires ``reduce_fn`` to produce valid output
    from partial input (concat is fine; global sort/aggregate is NOT).
    ``streaming=False``: accumulate everything then call ``reduce_fn`` once.

    ``target_max_block_size=None`` skips reshape entirely — every block
    ``reduce_fn`` produces is emitted as-is (partition = block contract).

    Args:
        prefetch_dir: optional staging directory. Unset → per-task tempdir.
        max_bytes_per_fetch: cap on requested bytes per FETCH (server
            response buffer); big partitions split across multiple FETCHes
            on the same connection. Default 256 MiB.
        target_max_block_size: output reshape target; also the streaming
            flush threshold.
        streaming: incremental flush vs accumulate-then-reduce.
    """
    start_time_s = time.perf_counter()

    # Collect (manager, token, src_path, ranges) per source for this partition.
    jobs: List[
        Tuple["ray.actor.ActorHandle", str, str, List[Tuple[int, int]]]
    ] = []
    for h in handles:
        if not isinstance(h, dict):
            h = ray.get(h)
        ranges = h["index"].get(partition_id) or []
        if ranges:
            jobs.append((h["manager"], h["token"], h["path"], ranges))

    # streaming yield helper
    def _yield_with_stats(block: Block):
        """Yield (block, pickled metadata) — the executor's DataOpTask
        wrapper sends a StreamingGeneratorStats back between the two yields,
        which we feed into BlockExecStats for accurate ``block_ser_time_s``.
        """
        exec_stats_builder = BlockExecStats.builder()
        exec_stats_builder.finish()
        gen_stats: StreamingGeneratorStats = yield block
        exec_stats = exec_stats_builder.build(
            block_ser_time_s=(
                gen_stats.object_creation_dur_s if gen_stats else None
            ),
        )
        yield pickle.dumps(
            BlockMetadataWithSchema.from_block(
                block,
                block_exec_stats=exec_stats,
                task_exec_stats=TaskExecWorkerStats(
                    task_wall_time_s=time.perf_counter() - start_time_s,
                ),
            )
        )

    # Empty-input shortcut: still call reduce_fn (may produce empty block)
    # and yield via the protocol so the operator gets the metadata.
    if not jobs:
        for block in reduce_fn(partition_id, []):
            yield from _yield_with_stats(block)
        return

    # Decide where the prefetch file lives, and whether we own the cleanup.
    owns_dir = prefetch_dir is None
    if owns_dir:
        prefetch_dir = tempfile.mkdtemp(prefix=f"ray_shuffle_p{partition_id}_")
    else:
        os.makedirs(prefetch_dir, exist_ok=True)
    assert prefetch_dir is not None
    staging_dir: str = prefetch_dir
    prefetch_file = os.path.join(staging_dir, "prefetch.bin")

    # Group by manager actor: one TCP connection per ShuffleManager.
    # Key on the actor's id (binary) so multiple deserialized ActorHandle
    # instances pointing at the same actor collapse to one entry.
    groups: Dict[
        bytes,
        Tuple[
            "ray.actor.ActorHandle",
            str,
            List[Tuple[int, str, List[Tuple[int, int]]]],
        ],
    ] = {}
    for idx, (manager, token, src_path, src_ranges) in enumerate(jobs):
        key = manager._actor_id.binary()
        if key not in groups:
            groups[key] = (manager, token, [])
        groups[key][2].append((idx, src_path, src_ranges))

    try:
        # Phase 1: prefetch all shards into one prefetch.bin
        with open(prefetch_file, "wb") as out_f:
            for manager, token, members in groups.values():
                _prefetch_node_into(
                    out_f, manager, token, members, max_bytes_per_fetch
                )

        # Phase 2: walk prefetch.bin, drive reduce_fn streamingly
        # Reshape buffer (created lazily on first flush) and the running
        # accumulator. Memory peak is bounded by `target_max_block_size`
        # in streaming mode.
        accum_tables: List[pa.Table] = []
        accum_bytes: int = 0
        output_buffer: Optional[BlockOutputBuffer] = None

        def _flush(tables: List[pa.Table]):
            """Call reduce_fn on `tables` and yield reshaped output."""
            nonlocal output_buffer
            if output_buffer is None and target_max_block_size is not None:
                output_buffer = BlockOutputBuffer(
                    OutputBlockSizeOption.of(
                        target_max_block_size=target_max_block_size,
                    )
                )
            # todo: if reduce_fn does join/sort, it might oom
            for block in reduce_fn(partition_id, tables):
                if output_buffer is None:
                    # target_max_block_size=None: emit blocks as-is.
                    yield from _yield_with_stats(block)
                else:
                    output_buffer.add_block(block)
                    while output_buffer.has_next():
                        yield from _yield_with_stats(output_buffer.next())

        mmf = pa.memory_map(prefetch_file, "r")
        try:
            file_size = mmf.size()
            while mmf.tell() < file_size:
                length = struct.unpack(">I", bytes(mmf.read(4)))[0]
                # Zero-copy view into mmap for uncompressed IPC; the decoder
                # copies into fresh buffers for compressed IPC.
                ipc_buf = mmf.read(length)
                table = _read_ipc(ipc_buf)
                accum_tables.append(table)
                accum_bytes += table.nbytes

                if (
                    streaming
                    and target_max_block_size is not None
                    and accum_bytes >= target_max_block_size
                ):
                    # Detach the accumulated batch and run it through
                    # reduce_fn before draining. Yields any blocks the
                    # reshape buffer can emit.
                    tables, accum_tables = accum_tables, []
                    accum_bytes = 0
                    yield from _flush(tables)

            # Drain any remaining accumulated shards
            # In blocking mode this is the ONLY reduce_fn call;
            # in streaming mode it's the tail.
            if accum_tables:
                yield from _flush(accum_tables)
                accum_tables = []

            # Finalize the reshape buffer: emit any partial trailing block.
            if output_buffer is not None:
                output_buffer.finalize()
                while output_buffer.has_next():
                    yield from _yield_with_stats(output_buffer.next())
        finally:
            try:
                mmf.close()
            except Exception:
                pass
    finally:
        # One file, one unlink. Idempotent on partial-failure paths.
        try:
            os.unlink(prefetch_file)
        except OSError:
            pass
        if owns_dir:
            try:
                os.rmdir(prefetch_dir)
            except OSError:
                pass
def concat_reduce(partition_id: int, tables: List[pa.Table]) -> Iterable[pa.Table]:
    if not tables:
        return
    yield pa.concat_tables(tables) if len(tables) > 1 else tables[0]
