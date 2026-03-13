import binascii
import errno
import io
import os
import stat
import threading
import tempfile
import time
from collections import OrderedDict

try:
    from fuse import FUSE, FuseOSError, Operations
except ImportError as exc:  # pragma: no cover - optional dependency
    raise SystemExit(
        "Missing fusepy. Install with `pip install fusepy` and ensure macFUSE is installed."
    ) from exc

from permstream_pro import (
    CodecConfig,
    PSFS_CHUNK_FLAG_CRC,
    PSFS_CHUNK_SIZE,
    PSFS_CODEC_RAW,
    PSFS_FILE_FLAG_DIR,
    PSFS_FILE_FLAG_SYMLINK,
    PSFS_FLAG_HAS_MANIFEST,
    PSFS_MAGIC,
    PSFS_MANIFEST_FMT,
    PSFS_MANIFEST_SIZE,
    PSFS_SUPER_FMT,
    PSFS_SUPER_SIZE,
    PSFS_VERSION,
    PSFS_HASH_SHA256,
    PSFS_CHUNK_FMT,
    PSFS_FILE_FMT,
    PSFS_FILE_SIZE,
    decompress_chunk_payload,
    compress_stream,
    decompress_stream,
)

import hashlib
import struct

PSFJ_MAGIC = b"PSFJ"
PSFJ_VERSION = 1
PSFJ_HEADER_FMT = "<4sHHI"
PSFJ_HEADER_SIZE = struct.calcsize(PSFJ_HEADER_FMT)
PSFJ_RECORD_FMT = "<BBHIIIQQI"
PSFJ_RECORD_SIZE = struct.calcsize(PSFJ_RECORD_FMT)
PSFJ_TYPE_FILE = 1
PSFJ_TYPE_SYMLINK = 2
PSFJ_TYPE_DIR = 3
PSFJ_TYPE_DELETE = 4
PSFJ_TYPE_EXTENT = 5
PSFJ_TYPE_COMMIT = 6
PSFJ_FLAG_COMPRESSED = 1 << 0
PSFJ_FLAG_RECURSIVE = 1 << 1
PSFJ_EXTENT_FMT = "<QQQ"
PSFJ_EXTENT_SIZE = struct.calcsize(PSFJ_EXTENT_FMT)

def derive_blob_path(journal_path):
    root, ext = os.path.splitext(journal_path)
    if ext == ".psfj":
        return root + ".psfb"
    return journal_path + ".psfb"


def normalize_journal_path(path):
    path = os.path.normpath(path)
    if path.startswith("/"):
        path = path[1:]
    if path == ".":
        return ""
    return path

def apply_journal_profile(args):
    base = {
        "journal_sync_every": 0,
        "journal_compress": False,
        "journal_compress_min_size": 4096,
        "journal_compress_min_gain": 128,
        "journal_inline_max": 4 * 1024 * 1024,
        "journal_extent_size": 4 * 1024 * 1024,
    }
    profiles = {
        "business": {
            "journal_sync_every": 64,
            "journal_compress": True,
            "journal_compress_min_size": 4096,
            "journal_compress_min_gain": 128,
            "journal_inline_max": 4 * 1024 * 1024,
            "journal_extent_size": 8 * 1024 * 1024,
        },
        "media": {
            "journal_sync_every": 16,
            "journal_compress": False,
            "journal_compress_min_size": 65536,
            "journal_compress_min_gain": 1024,
            "journal_inline_max": 4 * 1024 * 1024,
            "journal_extent_size": 16 * 1024 * 1024,
        },
    }
    chosen = profiles.get(args.journal_profile, base)
    if args.journal_profile is None:
        chosen = base
    for key, value in chosen.items():
        if getattr(args, key) is None:
            setattr(args, key, value)
    if getattr(args, "journal_no_compress", None):
        args.journal_compress = False


class JournalWriter:
    def __init__(self, path, sync=False, sync_every=0, blob_path=None):
        self.path = path
        self.sync = sync
        self.sync_every = max(int(sync_every), 0)
        self.blob_path = blob_path or derive_blob_path(path)
        self.lock = threading.Lock()
        self.pending_records = 0
        self.fsyncs = 0
        self.blob_dirty = False
        self._ensure_journal()
        self.handle = open(path, "ab")
        self.blob_handle = open(self.blob_path, "ab")
        self.blob_offset = os.fstat(self.blob_handle.fileno()).st_size

    def _ensure_journal(self):
        if os.path.exists(self.path):
            with open(self.path, "rb") as handle:
                header = handle.read(PSFJ_HEADER_SIZE)
            if len(header) < PSFJ_HEADER_SIZE:
                raise ValueError("Invalid PSFJ header")
            magic, version, _, _ = struct.unpack(PSFJ_HEADER_FMT, header)
            if magic != PSFJ_MAGIC:
                raise ValueError("Invalid PSFJ magic")
            if version != PSFJ_VERSION:
                raise ValueError("Unsupported PSFJ version")
            return
        with open(self.path, "wb") as handle:
            handle.write(
                struct.pack(PSFJ_HEADER_FMT, PSFJ_MAGIC, PSFJ_VERSION, PSFJ_HEADER_SIZE, 0)
            )

    def append_record(self, rec_type, flags, path, mode, uid, gid, mtime_ns, size, data):
        path = os.path.normpath(path)
        if path.startswith("/"):
            path = path[1:]
        if path == ".":
            path = ""
        record = self.build_record(rec_type, flags, path, mode, uid, gid, mtime_ns, size, data)
        self.append_records([record])

    def append_records(self, records):
        if not records:
            return
        with self.lock:
            for record in records:
                self.handle.write(record)
            self.handle.flush()
            self.pending_records += len(records)
            if self.sync or (self.sync_every and self.pending_records >= self.sync_every):
                os.fsync(self.handle.fileno())
                if self.blob_dirty:
                    os.fsync(self.blob_handle.fileno())
                    self.blob_dirty = False
                self.pending_records = 0
                self.fsyncs += 1

    def append_blob(self, data):
        if not data:
            return self.blob_offset
        with self.lock:
            offset = self.blob_offset
            self.blob_handle.write(data)
            self.blob_handle.flush()
            self.blob_offset += len(data)
            self.blob_dirty = True
            return offset

    def build_record(self, rec_type, flags, path, mode, uid, gid, mtime_ns, size, data):
        path_bytes = path.encode("utf-8")
        header = struct.pack(
            PSFJ_RECORD_FMT,
            rec_type,
            flags,
            len(path_bytes),
            mode,
            uid,
            gid,
            mtime_ns,
            size,
            len(data),
        )
        return header + path_bytes + (data or b"")

    def close(self):
        if self.handle:
            if self.pending_records and (self.sync or self.sync_every):
                os.fsync(self.handle.fileno())
                self.pending_records = 0
                self.fsyncs += 1
            self.handle.close()
            self.handle = None
        if self.blob_handle:
            if self.blob_dirty and (self.sync or self.sync_every):
                os.fsync(self.blob_handle.fileno())
                self.blob_dirty = False
            self.blob_handle.close()
            self.blob_handle = None

    def stats_snapshot(self):
        return {"fsyncs": self.fsyncs}


class PSFSReader:
    def __init__(
        self,
        path,
        verify=False,
        verify_chunks=False,
        cache_chunks=32,
        readahead=2,
        stats=False,
        journal_path=None,
        journal_blob_path=None,
    ):
        self.path = path
        self.handle = open(path, "rb")
        self.lock = threading.Lock()
        self.cache_lock = threading.Lock()
        self.cache = OrderedDict()
        self.cache_chunks = max(int(cache_chunks), 0)
        self.readahead = max(int(readahead), 0)
        self.verify_chunks = verify_chunks
        self.stats_enabled = stats
        self.cache_hits = 0
        self.cache_misses = 0
        self.decode_errors = 0
        self.journal_records = {}
        self.journal_recursive_deletes = []
        self.journal_pending_extents = {}
        self.journal_blob_path = None
        self.journal_blob_handle = None
        self.journal_blob_lock = threading.Lock()
        self.journal_seq = 0
        self._load(verify)
        if journal_path and os.path.exists(journal_path):
            self.journal_blob_path = journal_blob_path or derive_blob_path(journal_path)
            if self.journal_blob_path and os.path.exists(self.journal_blob_path):
                self.journal_blob_handle = open(self.journal_blob_path, "rb")
            self._load_journal(journal_path)
        self.dir_index = self._build_dir_index()

    def _load(self, verify):
        header = self.handle.read(PSFS_SUPER_SIZE)
        if len(header) < PSFS_SUPER_SIZE:
            raise ValueError("Missing superblock")
        (
            magic,
            version,
            header_size,
            flags,
            chunk_size,
            block_size,
            codec_id,
            hash_id,
            file_count,
            chunk_count,
            index_offset,
            strings_offset,
            chunk_table_offset,
            data_offset,
            manifest_offset,
            codec_flags,
            seed,
            w0,
            w1,
            w2,
            w3,
            transform_id,
            _,
        ) = struct.unpack(PSFS_SUPER_FMT, header)
        if magic != PSFS_MAGIC:
            raise ValueError("Invalid PSFS magic")
        if version != PSFS_VERSION:
            raise ValueError("Unsupported PSFS version")

        self.chunk_size = chunk_size
        predictor_mode = "header" if (codec_flags & (1 << 1)) else "seeded"
        use_rank = bool(codec_flags & (1 << 0))
        self.config = CodecConfig(
            chunk_size=chunk_size,
            block_size=block_size,
            use_rank=use_rank,
            predictor_mode=predictor_mode,
            seed=seed,
            weights=[w0, w1, w2, w3],
        )

        if verify and (flags & PSFS_FLAG_HAS_MANIFEST):
            self.handle.seek(index_offset)
            file_index_bytes = self.handle.read(strings_offset - index_offset)
            self.handle.seek(strings_offset)
            string_table = self.handle.read(chunk_table_offset - strings_offset)
            self.handle.seek(chunk_table_offset)
            chunk_table_bytes = self.handle.read(chunk_count * PSFS_CHUNK_SIZE)
            table_hash = hashlib.sha256(
                file_index_bytes + string_table + chunk_table_bytes
            ).digest()
            self.handle.seek(manifest_offset)
            manifest = self.handle.read(PSFS_MANIFEST_SIZE)
            if len(manifest) < PSFS_MANIFEST_SIZE:
                raise ValueError("Missing manifest")
            hash_id, _, stored_hash = struct.unpack(PSFS_MANIFEST_FMT, manifest)
            if hash_id != PSFS_HASH_SHA256:
                raise ValueError("Unsupported manifest hash")
            if stored_hash != table_hash:
                raise ValueError("Manifest hash mismatch")

        self.handle.seek(index_offset)
        file_entries = []
        for _ in range(file_count):
            entry_data = self.handle.read(PSFS_FILE_SIZE)
            if len(entry_data) < PSFS_FILE_SIZE:
                raise ValueError("Unexpected EOF in file index")
            (
                file_id,
                mode,
                fflags,
                uid,
                gid,
                mtime_ns,
                size,
                chunk_start,
                entry_chunk_count,
                path_offset,
                path_len,
            ) = struct.unpack(PSFS_FILE_FMT, entry_data)
            file_entries.append(
                {
                    "file_id": file_id,
                    "mode": mode,
                    "flags": fflags,
                    "uid": uid,
                    "gid": gid,
                    "mtime_ns": mtime_ns,
                    "size": size,
                    "chunk_start": chunk_start,
                    "chunk_count": entry_chunk_count,
                    "path_offset": path_offset,
                    "path_len": path_len,
                }
            )

        self.handle.seek(strings_offset)
        string_table = self.handle.read(chunk_table_offset - strings_offset)

        self.handle.seek(chunk_table_offset)
        chunk_entries = []
        for _ in range(chunk_count):
            chunk_data = self.handle.read(PSFS_CHUNK_SIZE)
            if len(chunk_data) < PSFS_CHUNK_SIZE:
                raise ValueError("Unexpected EOF in chunk table")
            (
                file_id,
                cflags,
                file_offset,
                raw_size,
                stored_size,
                data_off,
                codec_id,
                transform_id,
                _,
                crc32,
            ) = struct.unpack(PSFS_CHUNK_FMT, chunk_data)
            chunk_entries.append(
                {
                    "file_id": file_id,
                    "flags": cflags,
                    "file_offset": file_offset,
                    "raw_size": raw_size,
                    "stored_size": stored_size,
                    "data_offset": data_off,
                    "codec_id": codec_id,
                    "transform_id": transform_id,
                    "crc32": crc32,
                }
            )

        self.files = {}
        for entry in file_entries:
            path_bytes = string_table[
                entry["path_offset"] : entry["path_offset"] + entry["path_len"]
            ]
            rel_path = path_bytes.decode("utf-8")
            self.files["/" + rel_path] = entry

        self.chunk_entries = chunk_entries

    def _normalize_path(self, path):
        path = os.path.normpath(path)
        if path in ("", "."):
            return "/"
        if not path.startswith("/"):
            path = "/" + path
        return path

    def _load_journal(self, path):
        with open(path, "rb") as handle:
            header = handle.read(PSFJ_HEADER_SIZE)
            if len(header) < PSFJ_HEADER_SIZE:
                raise ValueError("Invalid PSFJ header")
            magic, version, header_size, _ = struct.unpack(PSFJ_HEADER_FMT, header)
            if magic != PSFJ_MAGIC:
                raise ValueError("Invalid PSFJ magic")
            if version != PSFJ_VERSION:
                raise ValueError("Unsupported PSFJ version")
            if header_size > PSFJ_HEADER_SIZE:
                handle.seek(header_size, os.SEEK_SET)
            while True:
                record = handle.read(PSFJ_RECORD_SIZE)
                if not record:
                    break
                if len(record) < PSFJ_RECORD_SIZE:
                    break
                (
                    rec_type,
                    rec_flags,
                    path_len,
                    mode,
                    uid,
                    gid,
                    mtime_ns,
                    size,
                    data_len,
                ) = struct.unpack(PSFJ_RECORD_FMT, record)
                path_bytes = handle.read(path_len)
                if len(path_bytes) < path_len:
                    break
                data = handle.read(data_len)
                if len(data) < data_len:
                    break
                path = self._normalize_path(path_bytes.decode("utf-8"))
                version = (mtime_ns, self.journal_seq)
                self.journal_seq += 1
                if rec_type == PSFJ_TYPE_DELETE:
                    record = {
                        "path": path,
                        "version": version,
                        "recursive": bool(rec_flags & PSFJ_FLAG_RECURSIVE),
                    }
                    self.journal_pending_extents.pop(path, None)
                    if record["recursive"]:
                        self.journal_recursive_deletes.append(record)
                    else:
                        self.journal_records.setdefault(path, []).append(
                            {"type": PSFJ_TYPE_DELETE, "version": version}
                        )
                    continue
                if rec_type == PSFJ_TYPE_EXTENT:
                    if len(data) != PSFJ_EXTENT_SIZE:
                        continue
                    file_offset, blob_offset, stored_len = struct.unpack(
                        PSFJ_EXTENT_FMT, data
                    )
                    self.journal_pending_extents.setdefault(path, []).append(
                        {
                            "file_offset": file_offset,
                            "raw_len": size,
                            "blob_offset": blob_offset,
                            "stored_len": stored_len,
                            "compressed": bool(rec_flags & PSFJ_FLAG_COMPRESSED),
                        }
                    )
                    continue
                if rec_type == PSFJ_TYPE_COMMIT:
                    extents = self.journal_pending_extents.pop(path, [])
                    extents.sort(key=lambda e: e["file_offset"])
                    entry = {
                        "file_id": None,
                        "mode": mode & 0xFFFF,
                        "flags": 0,
                        "uid": uid,
                        "gid": gid,
                        "mtime_ns": mtime_ns,
                        "size": size,
                        "chunk_start": 0,
                        "chunk_count": 0,
                        "journal": True,
                        "extents": extents,
                        "blob_path": self.journal_blob_path,
                    }
                    self.journal_records.setdefault(path, []).append(
                        {"type": PSFJ_TYPE_COMMIT, "version": version, "entry": entry}
                    )
                    continue
                flags = 0
                if rec_type == PSFJ_TYPE_DIR:
                    flags |= PSFS_FILE_FLAG_DIR
                elif rec_type == PSFJ_TYPE_SYMLINK:
                    flags |= PSFS_FILE_FLAG_SYMLINK
                if rec_type in (PSFJ_TYPE_FILE, PSFJ_TYPE_DIR, PSFJ_TYPE_SYMLINK):
                    self.journal_pending_extents.pop(path, None)
                entry = {
                    "file_id": None,
                    "mode": mode & 0xFFFF,
                    "flags": flags,
                    "uid": uid,
                    "gid": gid,
                    "mtime_ns": mtime_ns,
                    "size": size,
                    "chunk_start": 0,
                    "chunk_count": 0,
                    "journal": True,
                    "data": data,
                    "compressed": bool(rec_flags & PSFJ_FLAG_COMPRESSED),
                }
                self.journal_records.setdefault(path, []).append(
                    {"type": rec_type, "version": version, "entry": entry}
                )

    def _latest_record(self, path):
        records = self.journal_records.get(path, [])
        if not records:
            return None
        return max(records, key=lambda r: r["version"])

    def _latest_recursive_delete(self, path):
        best = None
        for record in self.journal_recursive_deletes:
            prefix = record["path"].rstrip("/")
            if prefix == "":
                match = True
            else:
                match = path == prefix or path.startswith(prefix + "/")
            if not match:
                continue
            version = record["version"]
            if best is None or version > best:
                best = version
        return best

    def _is_deleted(self, path, entry_version=None):
        delete_version = self._latest_recursive_delete(path)
        if delete_version is None:
            return False
        if entry_version is None:
            return True
        return delete_version > entry_version

    def overlay_entry(self, path, entry, rec_type, version=None):
        if version is None:
            version = (entry.get("mtime_ns", 0), self.journal_seq)
        self.journal_seq += 1
        self.journal_records.setdefault(path, []).append(
            {"type": rec_type, "version": version, "entry": entry}
        )
        self.dir_index = self._build_dir_index()

    def overlay_delete(self, path, recursive=False, mtime_ns=None):
        if mtime_ns is None:
            mtime_ns = 0
        version = (mtime_ns, self.journal_seq)
        self.journal_seq += 1
        record = {"path": path, "recursive": recursive, "version": version}
        if recursive:
            self.journal_recursive_deletes.append(record)
        else:
            self.journal_records.setdefault(path, []).append(
                {"type": PSFJ_TYPE_DELETE, "version": version}
            )
        self.dir_index = self._build_dir_index()

    def _all_paths(self):
        paths = set(self.files.keys())
        paths.update(self.journal_records.keys())
        return paths

    def _build_dir_index(self):
        tree = {"/": set()}
        paths = self._all_paths()
        for path in paths:
            if path == "/":
                continue
            entry = self.get_entry(path)
            if entry is None:
                continue
            parts = path.strip("/").split("/")
            current = "/"
            for idx, part in enumerate(parts):
                child = os.path.join(current, part)
                if current not in tree:
                    tree[current] = set()
                tree[current].add(part)
                if idx < len(parts) - 1 or (entry["flags"] & PSFS_FILE_FLAG_DIR):
                    if child not in tree:
                        tree[child] = set()
                current = child
        return tree

    def list_dir(self, path):
        return self.dir_index.get(path, set())

    def get_entry(self, path):
        path = self._normalize_path(path)
        record = self._latest_record(path)
        if record is not None:
            if record["type"] == PSFJ_TYPE_DELETE:
                return None
            if self._is_deleted(path, record["version"]):
                return None
            return record["entry"]
        if self._is_deleted(path, None):
            return None
        return self.files.get(path)

    def _journal_data(self, entry):
        if not entry.get("journal"):
            return b""
        if "extents" in entry:
            return self._read_extents_slice(entry, 0, entry.get("size", 0))
        if "decoded" in entry:
            return entry["decoded"]
        data = entry.get("data") or b""
        if entry.get("compressed"):
            out = io.BytesIO()
            decompress_stream(io.BytesIO(data), out)
            data = out.getvalue()
        entry["decoded"] = data
        return data

    def _read_blob_data(self, blob_offset, stored_len):
        if self.journal_blob_handle is None:
            if self.journal_blob_path and os.path.exists(self.journal_blob_path):
                self.journal_blob_handle = open(self.journal_blob_path, "rb")
            else:
                raise ValueError("Missing journal blob file")
        with self.journal_blob_lock:
            self.journal_blob_handle.seek(blob_offset)
            data = self.journal_blob_handle.read(stored_len)
        if len(data) < stored_len:
            raise ValueError("Truncated journal blob data")
        return data

    def _decode_extent(self, extent):
        data = self._read_blob_data(extent["blob_offset"], extent["stored_len"])
        if extent.get("compressed"):
            out = io.BytesIO()
            decompress_stream(io.BytesIO(data), out)
            data = out.getvalue()
        if len(data) != extent["raw_len"]:
            raise ValueError("Journal extent size mismatch")
        return data

    def _read_extents_slice(self, entry, offset, size):
        total_size = entry.get("size", 0)
        if offset >= total_size:
            return b""
        end = min(offset + size, total_size)
        out = bytearray()
        for extent in entry.get("extents", []):
            ext_start = extent["file_offset"]
            ext_end = ext_start + extent["raw_len"]
            if ext_end <= offset:
                continue
            if ext_start >= end:
                break
            slice_start = max(offset, ext_start)
            slice_end = min(end, ext_end)
            if extent.get("compressed"):
                data = self._decode_extent(extent)
                out.extend(data[slice_start - ext_start : slice_end - ext_start])
            else:
                blob_off = extent["blob_offset"] + (slice_start - ext_start)
                blob_len = slice_end - slice_start
                out.extend(self._read_blob_data(blob_off, blob_len))
        return bytes(out)

    def _cache_get(self, key):
        if self.cache_chunks <= 0:
            return None
        with self.cache_lock:
            value = self.cache.get(key)
            if value is None:
                return None
            self.cache.move_to_end(key)
            return value

    def _cache_has(self, key):
        if self.cache_chunks <= 0:
            return False
        with self.cache_lock:
            return key in self.cache

    def _cache_set(self, key, value):
        if self.cache_chunks <= 0:
            return
        with self.cache_lock:
            self.cache[key] = value
            self.cache.move_to_end(key)
            while len(self.cache) > self.cache_chunks:
                self.cache.popitem(last=False)

    def _decode_chunk(self, entry, chunk_index):
        chunk = self.chunk_entries[entry["chunk_start"] + chunk_index]
        with self.lock:
            self.handle.seek(chunk["data_offset"])
            payload = self.handle.read(chunk["stored_size"])
        decoded = decompress_chunk_payload(
            payload,
            chunk["raw_size"],
            self.config,
            entry["file_id"],
            chunk_index,
            chunk["codec_id"],
            chunk["transform_id"],
        )
        if self.verify_chunks and (chunk["flags"] & PSFS_CHUNK_FLAG_CRC):
            check = binascii.crc32(decoded) & 0xFFFFFFFF
            if check != chunk["crc32"]:
                raise ValueError("CRC mismatch while reading chunk")
        return decoded

    def read_chunk(self, entry, chunk_index):
        key = (entry["file_id"], chunk_index)
        cached = self._cache_get(key)
        if cached is not None:
            self.cache_hits += 1
            return cached
        self.cache_misses += 1
        decoded = self._decode_chunk(entry, chunk_index)
        self._cache_set(key, decoded)
        return decoded

    def prefetch(self, entry, start_index, count):
        if self.cache_chunks <= 0 or count <= 0:
            return
        end_index = min(entry["chunk_count"], start_index + count)
        for idx in range(start_index, end_index):
            key = (entry["file_id"], idx)
            if self._cache_has(key):
                continue
            try:
                decoded = self._decode_chunk(entry, idx)
            except ValueError:
                self.decode_errors += 1
                continue
            self._cache_set(key, decoded)

    def read_entry_data(self, entry):
        if entry.get("journal"):
            return self._journal_data(entry)
        out = bytearray()
        chunk_index = 0
        for chunk in self.chunk_entries[
            entry["chunk_start"] : entry["chunk_start"] + entry["chunk_count"]
        ]:
            with self.lock:
                self.handle.seek(chunk["data_offset"])
                payload = self.handle.read(chunk["stored_size"])
            decoded = decompress_chunk_payload(
                payload,
                chunk["raw_size"],
                self.config,
                entry["file_id"],
                chunk_index,
                chunk["codec_id"],
                chunk["transform_id"],
            )
            out.extend(decoded)
            chunk_index += 1
        return bytes(out)

    def stream_entry_data(self, entry, out_handle, chunk_size=1024 * 1024):
        if entry.get("journal"):
            if "extents" in entry:
                for extent in entry.get("extents", []):
                    data = self._decode_extent(extent)
                    out_handle.seek(extent["file_offset"])
                    out_handle.write(data)
                return
            data = self._journal_data(entry)
            out_handle.write(data)
            return
        chunk_index = 0
        for chunk in self.chunk_entries[
            entry["chunk_start"] : entry["chunk_start"] + entry["chunk_count"]
        ]:
            with self.lock:
                self.handle.seek(chunk["data_offset"])
                payload = self.handle.read(chunk["stored_size"])
            decoded = decompress_chunk_payload(
                payload,
                chunk["raw_size"],
                self.config,
                entry["file_id"],
                chunk_index,
                chunk["codec_id"],
                chunk["transform_id"],
            )
            out_handle.write(decoded)
            chunk_index += 1

    def read_journal_slice(self, entry, offset, size):
        if "extents" in entry:
            return self._read_extents_slice(entry, offset, size)
        data = self._journal_data(entry)
        return data[offset : offset + size]

    def close(self):
        if self.handle:
            self.handle.close()
            self.handle = None
        if self.journal_blob_handle:
            self.journal_blob_handle.close()
            self.journal_blob_handle = None

    def stats_snapshot(self):
        return {
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
            "decode_errors": self.decode_errors,
        }


class PSFSFuse(Operations):
    def __init__(self, reader):
        self.reader = reader
        self.write_handles = {}
        self.write_by_path = {}
        self.handle_lock = threading.Lock()
        self.next_fh = 1
        self.writer = None
        self.journal_compress = False
        self.journal_compress_min_size = 4096
        self.journal_compress_min_gain = 128
        self.journal_inline_max = 4 * 1024 * 1024
        self.journal_extent_size = 4 * 1024 * 1024
        self.journal_stats = {
            "records": 0,
            "bytes_raw": 0,
            "bytes_stored": 0,
            "compressed_records": 0,
        }

    def enable_writer(
        self,
        writer,
        compress=False,
        compress_min_size=4096,
        compress_min_gain=128,
        inline_max=4 * 1024 * 1024,
        extent_size=4 * 1024 * 1024,
    ):
        self.writer = writer
        self.journal_compress = compress
        self.journal_compress_min_size = max(int(compress_min_size), 0)
        self.journal_compress_min_gain = max(int(compress_min_gain), 0)
        if inline_max is not None:
            self.journal_inline_max = max(int(inline_max), 0)
        if extent_size is not None:
            self.journal_extent_size = max(int(extent_size), 0)

    def _allocate_handle(self):
        with self.handle_lock:
            fh = self.next_fh
            self.next_fh += 1
        return fh

    def _get_handle(self, fh):
        return self.write_handles.get(fh)

    def _get_handle_by_path(self, path):
        return self.write_by_path.get(path)

    def _now_ns(self):
        return int(time.time() * 1e9)

    def _compress_payload(self, data):
        if not self.journal_compress:
            return data, 0
        if len(data) < self.journal_compress_min_size:
            return data, 0
        encoded = io.BytesIO()
        compress_stream(io.BytesIO(data), encoded, self.reader.config)
        payload = encoded.getvalue()
        if len(payload) + self.journal_compress_min_gain >= len(data):
            return data, 0
        return payload, PSFJ_FLAG_COMPRESSED

    def _new_spool(self):
        fd, path = tempfile.mkstemp(prefix="psfs_spool_")
        os.close(fd)
        handle = open(path, "r+b")
        return path, handle

    def _spill_handle_to_spool(self, handle):
        if handle.get("spool_handle"):
            return
        spool_path, spool_handle = self._new_spool()
        data = handle.get("data") or bytearray()
        if data:
            spool_handle.write(data)
            spool_handle.flush()
        handle["spool_path"] = spool_path
        handle["spool_handle"] = spool_handle
        handle["size"] = len(data)
        handle["data"] = None

    def _read_handle_slice(self, handle, offset, size):
        if handle.get("spool_handle"):
            if offset >= handle["size"]:
                return b""
            end = min(offset + size, handle["size"])
            handle["spool_handle"].seek(offset)
            return handle["spool_handle"].read(end - offset)
        data = handle.get("data") or bytearray()
        return bytes(data[offset : offset + size])

    def _track_journal_write(self, raw_len, stored_len, compressed):
        stats = self.journal_stats
        stats["records"] += 1
        stats["bytes_raw"] += raw_len
        stats["bytes_stored"] += stored_len
        if compressed:
            stats["compressed_records"] += 1

    def _commit_file(self, path, data, mode, uid, gid, mtime_ns):
        payload, flags = self._compress_payload(data)
        self.writer.append_record(
            PSFJ_TYPE_FILE,
            flags,
            path,
            mode & 0xFFFF,
            uid,
            gid,
            mtime_ns,
            len(data),
            payload,
        )
        self._track_journal_write(len(data), len(payload), bool(flags & PSFJ_FLAG_COMPRESSED))
        entry = {
            "file_id": None,
            "mode": mode & 0xFFFF,
            "flags": 0,
            "uid": uid,
            "gid": gid,
            "mtime_ns": mtime_ns,
            "size": len(data),
            "chunk_start": 0,
            "chunk_count": 0,
            "journal": True,
            "data": payload,
            "compressed": bool(flags & PSFJ_FLAG_COMPRESSED),
        }
        self.reader.overlay_entry(
            self.reader._normalize_path(path), entry, PSFJ_TYPE_FILE
        )

    def _commit_file_extents(self, path, handle, size, mode, uid, gid, mtime_ns):
        records = []
        extents = []
        offset = 0
        norm_path = normalize_journal_path(path)
        chunk_size = self.journal_extent_size or size or 0
        handle.seek(0)
        while offset < size:
            chunk = handle.read(min(chunk_size, size - offset))
            if not chunk:
                break
            payload, flags = self._compress_payload(chunk)
            blob_offset = self.writer.append_blob(payload)
            extent_data = struct.pack(
                PSFJ_EXTENT_FMT, offset, blob_offset, len(payload)
            )
            record = self.writer.build_record(
                PSFJ_TYPE_EXTENT,
                flags,
                norm_path,
                0,
                0,
                0,
                mtime_ns,
                len(chunk),
                extent_data,
            )
            records.append(record)
            extents.append(
                {
                    "file_offset": offset,
                    "raw_len": len(chunk),
                    "blob_offset": blob_offset,
                    "stored_len": len(payload),
                    "compressed": bool(flags & PSFJ_FLAG_COMPRESSED),
                }
            )
            self._track_journal_write(
                len(chunk), len(payload), bool(flags & PSFJ_FLAG_COMPRESSED)
            )
            offset += len(chunk)
        commit_record = self.writer.build_record(
            PSFJ_TYPE_COMMIT,
            0,
            norm_path,
            mode & 0xFFFF,
            uid,
            gid,
            mtime_ns,
            size,
            b"",
        )
        records.append(commit_record)
        self._track_journal_write(0, 0, False)
        self.writer.append_records(records)
        entry = {
            "file_id": None,
            "mode": mode & 0xFFFF,
            "flags": 0,
            "uid": uid,
            "gid": gid,
            "mtime_ns": mtime_ns,
            "size": size,
            "chunk_start": 0,
            "chunk_count": 0,
            "journal": True,
            "extents": extents,
            "blob_path": self.writer.blob_path,
        }
        self.reader.overlay_entry(
            self.reader._normalize_path(path), entry, PSFJ_TYPE_COMMIT
        )

    def _commit_handle(self, handle):
        if self.writer is None:
            return
        path = handle["path"]
        mode = handle["mode"]
        uid = handle["uid"]
        gid = handle["gid"]
        mtime_ns = handle["mtime_ns"]
        if handle.get("spool_handle"):
            size = handle["size"]
            handle["spool_handle"].seek(0)
            if size <= self.journal_inline_max:
                data = handle["spool_handle"].read()
                self._commit_file(path, data, mode, uid, gid, mtime_ns)
            else:
                self._commit_file_extents(
                    path, handle["spool_handle"], size, mode, uid, gid, mtime_ns
                )
        else:
            data = bytes(handle.get("data") or b"")
            if len(data) <= self.journal_inline_max:
                self._commit_file(path, data, mode, uid, gid, mtime_ns)
            else:
                buf = io.BytesIO(data)
                self._commit_file_extents(path, buf, len(data), mode, uid, gid, mtime_ns)

    def _commit_entry_copy(self, path, entry):
        if entry["flags"] & PSFS_FILE_FLAG_DIR:
            self._commit_dir(
                path, entry["mode"], entry["uid"], entry["gid"], entry["mtime_ns"]
            )
            return
        if entry["flags"] & PSFS_FILE_FLAG_SYMLINK:
            try:
                target = os.fsdecode(self.reader.read_entry_data(entry))
            except ValueError:
                raise FuseOSError(errno.EIO)
            self._commit_symlink(
                path, target, entry["mode"], entry["uid"], entry["gid"], entry["mtime_ns"]
            )
            return
        try:
            if "extents" in entry or entry["size"] > self.journal_inline_max:
                spool_path, spool_handle = self._new_spool()
                try:
                    self.reader.stream_entry_data(entry, spool_handle)
                    size = entry["size"]
                    spool_handle.seek(0)
                    if size <= self.journal_inline_max:
                        data = spool_handle.read()
                        self._commit_file(
                            path,
                            data,
                            entry["mode"],
                            entry["uid"],
                            entry["gid"],
                            entry["mtime_ns"],
                        )
                    else:
                        self._commit_file_extents(
                            path,
                            spool_handle,
                            size,
                            entry["mode"],
                            entry["uid"],
                            entry["gid"],
                            entry["mtime_ns"],
                        )
                finally:
                    spool_handle.close()
                    try:
                        os.unlink(spool_path)
                    except OSError:
                        pass
            else:
                data = self.reader.read_entry_data(entry)
                self._commit_file(
                    path,
                    data,
                    entry["mode"],
                    entry["uid"],
                    entry["gid"],
                    entry["mtime_ns"],
                )
        except ValueError:
            raise FuseOSError(errno.EIO)

    def _commit_dir(self, path, mode, uid, gid, mtime_ns):
        self.writer.append_record(
            PSFJ_TYPE_DIR,
            0,
            path,
            mode & 0xFFFF,
            uid,
            gid,
            mtime_ns,
            0,
            b"",
        )
        self._track_journal_write(0, 0, False)
        entry = {
            "file_id": None,
            "mode": mode & 0xFFFF,
            "flags": PSFS_FILE_FLAG_DIR,
            "uid": uid,
            "gid": gid,
            "mtime_ns": mtime_ns,
            "size": 0,
            "chunk_start": 0,
            "chunk_count": 0,
            "journal": True,
            "data": b"",
            "compressed": False,
        }
        self.reader.overlay_entry(
            self.reader._normalize_path(path), entry, PSFJ_TYPE_DIR
        )

    def _commit_symlink(self, path, target, mode, uid, gid, mtime_ns):
        data = os.fsencode(target)
        self.writer.append_record(
            PSFJ_TYPE_SYMLINK,
            0,
            path,
            mode & 0xFFFF,
            uid,
            gid,
            mtime_ns,
            len(data),
            data,
        )
        self._track_journal_write(len(data), len(data), False)
        entry = {
            "file_id": None,
            "mode": mode & 0xFFFF,
            "flags": PSFS_FILE_FLAG_SYMLINK,
            "uid": uid,
            "gid": gid,
            "mtime_ns": mtime_ns,
            "size": len(data),
            "chunk_start": 0,
            "chunk_count": 0,
            "journal": True,
            "data": data,
            "compressed": False,
        }
        self.reader.overlay_entry(
            self.reader._normalize_path(path), entry, PSFJ_TYPE_SYMLINK
        )

    def _commit_delete(self, path, recursive=False):
        flags = PSFJ_FLAG_RECURSIVE if recursive else 0
        mtime_ns = self._now_ns()
        self.writer.append_record(
            PSFJ_TYPE_DELETE,
            flags,
            path,
            0,
            0,
            0,
            mtime_ns,
            0,
            b"",
        )
        self._track_journal_write(0, 0, False)
        self.reader.overlay_delete(
            self.reader._normalize_path(path),
            recursive=recursive,
            mtime_ns=mtime_ns,
        )

    def getattr(self, path, fh=None):
        if path == "/":
            return dict(
                st_mode=(stat.S_IFDIR | 0o755),
                st_nlink=2,
                st_size=0,
            )
        entry = self.reader.get_entry(path)
        if entry is None:
            if path in self.reader.dir_index:
                return dict(
                    st_mode=(stat.S_IFDIR | 0o755),
                    st_nlink=2,
                    st_size=0,
                )
            raise FuseOSError(errno.ENOENT)
        if entry["flags"] & PSFS_FILE_FLAG_DIR:
            return dict(
                st_mode=(stat.S_IFDIR | (entry["mode"] & 0o7777)),
                st_nlink=2,
                st_size=0,
                st_uid=entry["uid"],
                st_gid=entry["gid"],
                st_mtime=entry["mtime_ns"] / 1e9,
            )
        if entry["flags"] & PSFS_FILE_FLAG_SYMLINK:
            return dict(
                st_mode=(stat.S_IFLNK | 0o777),
                st_nlink=1,
                st_size=entry["size"],
                st_uid=entry["uid"],
                st_gid=entry["gid"],
                st_mtime=entry["mtime_ns"] / 1e9,
            )
        return dict(
            st_mode=(stat.S_IFREG | (entry["mode"] & 0o7777)),
            st_nlink=1,
            st_size=entry["size"],
            st_uid=entry["uid"],
            st_gid=entry["gid"],
            st_mtime=entry["mtime_ns"] / 1e9,
        )

    def readdir(self, path, fh):
        yield "."
        yield ".."
        for name in self.reader.list_dir(path):
            yield name

    def read(self, path, size, offset, fh):
        entry = self.reader.get_entry(path)
        if entry is None:
            raise FuseOSError(errno.ENOENT)
        if entry["flags"] & PSFS_FILE_FLAG_DIR:
            raise FuseOSError(errno.EISDIR)
        if entry["flags"] & PSFS_FILE_FLAG_SYMLINK:
            raise FuseOSError(errno.EINVAL)
        handle = self._get_handle(fh)
        if handle and handle["path"] == path:
            return self._read_handle_slice(handle, offset, size)
        handle = self._get_handle_by_path(path)
        if handle:
            return self._read_handle_slice(handle, offset, size)
        if entry.get("journal"):
            try:
                return self.reader.read_journal_slice(entry, offset, size)
            except ValueError:
                raise FuseOSError(errno.EIO)
        if offset >= entry["size"]:
            return b""
        end = min(offset + size, entry["size"])
        first_chunk = offset // self.reader.chunk_size
        last_chunk = (end - 1) // self.reader.chunk_size
        out = bytearray()
        try:
            for idx in range(first_chunk, last_chunk + 1):
                decoded = self.reader.read_chunk(entry, idx)
                chunk_start = idx * self.reader.chunk_size
                start = max(offset, chunk_start)
                finish = min(end, chunk_start + len(decoded))
                out.extend(decoded[start - chunk_start : finish - chunk_start])
            if self.reader.readahead > 0:
                self.reader.prefetch(entry, last_chunk + 1, self.reader.readahead)
        except ValueError:
            raise FuseOSError(errno.EIO)
        return bytes(out)

    def create(self, path, mode, fi=None):
        if self.writer is None:
            raise FuseOSError(errno.EROFS)
        if self.reader.get_entry(path) is not None:
            raise FuseOSError(errno.EEXIST)
        fh = self._allocate_handle()
        handle = {
            "path": path,
            "data": bytearray(),
            "spool_path": None,
            "spool_handle": None,
            "size": 0,
            "mode": mode & 0xFFFF,
            "uid": os.getuid(),
            "gid": os.getgid(),
            "mtime_ns": self._now_ns(),
            "dirty": True,
        }
        self.write_handles[fh] = handle
        self.write_by_path[path] = handle
        return fh

    def mknod(self, path, mode, dev):
        if self.writer is None:
            raise FuseOSError(errno.EROFS)
        if self.reader.get_entry(path) is not None:
            raise FuseOSError(errno.EEXIST)
        self._commit_file(path, b"", mode, os.getuid(), os.getgid(), self._now_ns())
        return 0

    def open(self, path, flags):
        entry = self.reader.get_entry(path)
        if entry is None:
            raise FuseOSError(errno.ENOENT)
        if entry["flags"] & PSFS_FILE_FLAG_DIR:
            raise FuseOSError(errno.EISDIR)
        if entry["flags"] & PSFS_FILE_FLAG_SYMLINK:
            raise FuseOSError(errno.EINVAL)
        if flags & (os.O_WRONLY | os.O_RDWR):
            if self.writer is None:
                raise FuseOSError(errno.EROFS)
            fh = self._allocate_handle()
            handle = {
                "path": path,
                "data": bytearray(),
                "spool_path": None,
                "spool_handle": None,
                "size": 0,
                "mode": entry["mode"],
                "uid": entry["uid"],
                "gid": entry["gid"],
                "mtime_ns": entry["mtime_ns"],
                "dirty": False,
            }
            try:
                if "extents" in entry or entry["size"] > self.journal_inline_max:
                    self._spill_handle_to_spool(handle)
                    self.reader.stream_entry_data(entry, handle["spool_handle"])
                    handle["size"] = entry["size"]
                else:
                    data = self.reader.read_entry_data(entry)
                    handle["data"] = bytearray(data)
                    handle["size"] = len(data)
            except ValueError:
                raise FuseOSError(errno.EIO)
            self.write_handles[fh] = handle
            self.write_by_path[path] = handle
            return fh
        return 0

    def readlink(self, path):
        entry = self.reader.get_entry(path)
        if entry is None:
            raise FuseOSError(errno.ENOENT)
        if not (entry["flags"] & PSFS_FILE_FLAG_SYMLINK):
            raise FuseOSError(errno.EINVAL)
        try:
            data = self.reader.read_entry_data(entry)
            return os.fsdecode(data)
        except ValueError:
            raise FuseOSError(errno.EIO)

    def destroy(self, path):
        if self.reader.stats_enabled:
            stats = self.reader.stats_snapshot()
            print(
                "PSFS stats: hits={cache_hits} misses={cache_misses} errors={decode_errors}".format(
                    **stats
                )
            )
            if self.writer:
                wstats = self.journal_stats
                fsyncs = self.writer.stats_snapshot().get("fsyncs", 0)
                print(
                    "PSFS journal: records={records} raw={bytes_raw} stored={bytes_stored} compressed={compressed_records} fsyncs={fsyncs}".format(
                        fsyncs=fsyncs,
                        **wstats,
                    )
                )
        self.reader.close()
        if self.writer:
            self.writer.close()

    def write(self, path, data, offset, fh):
        if self.writer is None:
            raise FuseOSError(errno.EROFS)
        handle = self._get_handle(fh)
        if handle is None:
            handle = self._get_handle_by_path(path)
        if handle is None:
            entry = self.reader.get_entry(path)
            if entry is None:
                raise FuseOSError(errno.ENOENT)
            if entry["flags"] & PSFS_FILE_FLAG_DIR:
                raise FuseOSError(errno.EISDIR)
            try:
                handle = {
                    "path": path,
                    "data": bytearray(),
                    "spool_path": None,
                    "spool_handle": None,
                    "size": 0,
                    "mode": entry["mode"],
                    "uid": entry["uid"],
                    "gid": entry["gid"],
                    "mtime_ns": entry["mtime_ns"],
                    "dirty": False,
                }
                if "extents" in entry or entry["size"] > self.journal_inline_max:
                    self._spill_handle_to_spool(handle)
                    self.reader.stream_entry_data(entry, handle["spool_handle"])
                    handle["size"] = entry["size"]
                else:
                    data = self.reader.read_entry_data(entry)
                    handle["data"] = bytearray(data)
                    handle["size"] = len(data)
            except ValueError:
                raise FuseOSError(errno.EIO)
            self.write_by_path[path] = handle
        end = offset + len(data)
        if handle.get("spool_handle") or self.journal_inline_max == 0 or end > self.journal_inline_max:
            if not handle.get("spool_handle"):
                self._spill_handle_to_spool(handle)
            spool = handle["spool_handle"]
            if offset > handle["size"]:
                spool.seek(handle["size"])
                spool.write(b"\x00" * (offset - handle["size"]))
            spool.seek(offset)
            spool.write(data)
            handle["size"] = max(handle["size"], end)
        else:
            buf = handle["data"]
            if end > len(buf):
                buf.extend(b"\x00" * (end - len(buf)))
            buf[offset:end] = data
            handle["size"] = len(buf)
        handle["dirty"] = True
        handle["mtime_ns"] = self._now_ns()
        return len(data)

    def truncate(self, path, length, fh=None):
        if self.writer is None:
            raise FuseOSError(errno.EROFS)
        handle = None
        if fh is not None:
            handle = self._get_handle(fh)
        if handle is None:
            handle = self._get_handle_by_path(path)
        if handle is None:
            entry = self.reader.get_entry(path)
            if entry is None:
                raise FuseOSError(errno.ENOENT)
            if entry["flags"] & PSFS_FILE_FLAG_DIR:
                raise FuseOSError(errno.EISDIR)
            try:
                handle = {
                    "path": path,
                    "data": bytearray(),
                    "spool_path": None,
                    "spool_handle": None,
                    "size": 0,
                    "mode": entry["mode"],
                    "uid": entry["uid"],
                    "gid": entry["gid"],
                    "mtime_ns": entry["mtime_ns"],
                    "dirty": True,
                }
                if "extents" in entry or entry["size"] > self.journal_inline_max:
                    self._spill_handle_to_spool(handle)
                    self.reader.stream_entry_data(entry, handle["spool_handle"])
                    handle["size"] = entry["size"]
                else:
                    data = self.reader.read_entry_data(entry)
                    handle["data"] = bytearray(data)
                    handle["size"] = len(data)
            except ValueError:
                raise FuseOSError(errno.EIO)
            fh = self._allocate_handle()
            self.write_handles[fh] = handle
            self.write_by_path[path] = handle
        if handle.get("spool_handle") or self.journal_inline_max == 0 or length > self.journal_inline_max:
            if not handle.get("spool_handle"):
                self._spill_handle_to_spool(handle)
            spool = handle["spool_handle"]
            spool.truncate(length)
            handle["size"] = length
        else:
            buf = handle["data"]
            if length < len(buf):
                del buf[length:]
            elif length > len(buf):
                buf.extend(b"\x00" * (length - len(buf)))
            handle["size"] = len(buf)
        handle["dirty"] = True
        handle["mtime_ns"] = self._now_ns()
        return 0

    def flush(self, path, fh):
        handle = self._get_handle(fh)
        if handle is None:
            handle = self._get_handle_by_path(path)
        if handle and handle["dirty"]:
            self._commit_handle(handle)
            handle["dirty"] = False
        return 0

    def release(self, path, fh):
        handle = self._get_handle(fh)
        if handle is None:
            handle = self._get_handle_by_path(path)
        if handle and handle["dirty"]:
            self._commit_handle(handle)
        if fh in self.write_handles:
            del self.write_handles[fh]
        if handle:
            self.write_by_path.pop(handle["path"], None)
            spool_handle = handle.get("spool_handle")
            spool_path = handle.get("spool_path")
            if spool_handle:
                spool_handle.close()
            if spool_path:
                try:
                    os.unlink(spool_path)
                except OSError:
                    pass
        return 0

    def fsync(self, path, datasync, fh):
        return self.flush(path, fh)

    def unlink(self, path):
        if self.writer is None:
            raise FuseOSError(errno.EROFS)
        entry = self.reader.get_entry(path)
        if entry is None:
            raise FuseOSError(errno.ENOENT)
        if entry["flags"] & PSFS_FILE_FLAG_DIR:
            raise FuseOSError(errno.EISDIR)
        self._commit_delete(path, recursive=False)
        return 0

    def mkdir(self, path, mode):
        if self.writer is None:
            raise FuseOSError(errno.EROFS)
        if self.reader.get_entry(path) is not None:
            raise FuseOSError(errno.EEXIST)
        self._commit_dir(path, mode, os.getuid(), os.getgid(), self._now_ns())
        return 0

    def rmdir(self, path):
        if self.writer is None:
            raise FuseOSError(errno.EROFS)
        entry = self.reader.get_entry(path)
        if entry is None:
            raise FuseOSError(errno.ENOENT)
        if not (entry["flags"] & PSFS_FILE_FLAG_DIR):
            raise FuseOSError(errno.ENOTDIR)
        children = self.reader.list_dir(path)
        if children:
            raise FuseOSError(errno.ENOTEMPTY)
        self._commit_delete(path, recursive=False)
        return 0

    def rename(self, old, new):
        if self.writer is None:
            raise FuseOSError(errno.EROFS)
        if new.startswith(old.rstrip("/") + "/"):
            raise FuseOSError(errno.EINVAL)
        entry = self.reader.get_entry(old)
        if entry is None:
            raise FuseOSError(errno.ENOENT)

        def collect_paths(prefix):
            prefix = prefix.rstrip("/")
            paths = self.reader._all_paths()
            out = []
            for path in paths:
                if self.reader._is_deleted(path):
                    continue
                if path == prefix or path.startswith(prefix + "/"):
                    out.append(path)
            return sorted(out, key=lambda p: p.count("/"))

        if entry["flags"] & PSFS_FILE_FLAG_DIR:
            paths = collect_paths(old)
            for path in paths:
                ent = self.reader.get_entry(path)
                if ent is None:
                    continue
                rel = path[len(old) :].lstrip("/")
                new_path = os.path.join(new, rel) if rel else new
                self._commit_entry_copy(new_path, ent)
            self._commit_delete(old, recursive=True)
            return 0

        self._commit_entry_copy(new, entry)
        self._commit_delete(old, recursive=False)
        return 0

    def symlink(self, name, target):
        if self.writer is None:
            raise FuseOSError(errno.EROFS)
        if self.reader.get_entry(name) is not None:
            raise FuseOSError(errno.EEXIST)
        self._commit_symlink(name, target, 0o777, os.getuid(), os.getgid(), self._now_ns())
        return 0

    def chmod(self, path, mode):
        if self.writer is None:
            raise FuseOSError(errno.EROFS)
        return self._update_metadata(path, mode=mode)

    def chown(self, path, uid, gid):
        if self.writer is None:
            raise FuseOSError(errno.EROFS)
        return self._update_metadata(path, uid=uid, gid=gid)

    def utimens(self, path, times=None):
        if self.writer is None:
            raise FuseOSError(errno.EROFS)
        mtime_ns = self._now_ns()
        if times:
            mtime_ns = int(times[1] * 1e9)
        return self._update_metadata(path, mtime_ns=mtime_ns)

    def _update_metadata(self, path, mode=None, uid=None, gid=None, mtime_ns=None):
        entry = self.reader.get_entry(path)
        if entry is None:
            raise FuseOSError(errno.ENOENT)
        if entry["flags"] & PSFS_FILE_FLAG_DIR:
            self._commit_dir(
                path,
                mode if mode is not None else entry["mode"],
                uid if uid is not None else entry["uid"],
                gid if gid is not None else entry["gid"],
                mtime_ns if mtime_ns is not None else entry["mtime_ns"],
            )
            return 0
        if entry["flags"] & PSFS_FILE_FLAG_SYMLINK:
            try:
                target = os.fsdecode(self.reader.read_entry_data(entry))
            except ValueError:
                raise FuseOSError(errno.EIO)
            self._commit_symlink(
                path,
                target,
                mode if mode is not None else entry["mode"],
                uid if uid is not None else entry["uid"],
                gid if gid is not None else entry["gid"],
                mtime_ns if mtime_ns is not None else entry["mtime_ns"],
            )
            return 0
        new_mode = mode if mode is not None else entry["mode"]
        new_uid = uid if uid is not None else entry["uid"]
        new_gid = gid if gid is not None else entry["gid"]
        new_mtime = mtime_ns if mtime_ns is not None else entry["mtime_ns"]
        try:
            if "extents" in entry or entry["size"] > self.journal_inline_max:
                spool_path, spool_handle = self._new_spool()
                try:
                    self.reader.stream_entry_data(entry, spool_handle)
                    size = entry["size"]
                    spool_handle.seek(0)
                    if size <= self.journal_inline_max:
                        data = spool_handle.read()
                        self._commit_file(
                            path, data, new_mode, new_uid, new_gid, new_mtime
                        )
                    else:
                        self._commit_file_extents(
                            path, spool_handle, size, new_mode, new_uid, new_gid, new_mtime
                        )
                finally:
                    spool_handle.close()
                    try:
                        os.unlink(spool_path)
                    except OSError:
                        pass
            else:
                data = self.reader.read_entry_data(entry)
                self._commit_file(
                    path, data, new_mode, new_uid, new_gid, new_mtime
                )
        except ValueError:
            raise FuseOSError(errno.EIO)
        return 0

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Mount PSFS container")
    parser.add_argument("container")
    parser.add_argument("mountpoint")
    parser.add_argument("--verify", action="store_true")
    parser.add_argument(
        "--journal",
        help="Optional PSFJ journal file to overlay on top of the container",
    )
    parser.add_argument(
        "--verify-chunks",
        action="store_true",
        help="Validate chunk CRC on read",
    )
    parser.add_argument(
        "--cache-chunks",
        type=int,
        default=32,
        help="LRU cache size in decoded chunks",
    )
    parser.add_argument(
        "--readahead",
        type=int,
        default=2,
        help="Number of chunks to prefetch after sequential reads",
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Print cache statistics on unmount",
    )
    parser.add_argument(
        "--foreground",
        action="store_true",
        help="Run in foreground (default is daemonized)",
    )
    parser.add_argument(
        "--allow-other",
        action="store_true",
        help="Allow other users to access the mount (may require system config)",
    )
    parser.add_argument("--debug", action="store_true", help="Enable FUSE debug output")
    parser.add_argument(
        "--read-only",
        action="store_true",
        help="Force read-only mount even when a journal is provided",
    )
    parser.add_argument(
        "--journal-profile",
        choices=["business", "media"],
        help="Use tuned defaults for journal durability/compression",
    )
    parser.add_argument("--journal-blob", help="Path to PSFJ blob file")
    parser.add_argument(
        "--journal-sync",
        action="store_true",
        help="fsync journal writes for stronger durability",
    )
    parser.add_argument(
        "--journal-sync-every",
        type=int,
        default=None,
        help="fsync journal every N records (0 disables periodic sync)",
    )
    compress_group = parser.add_mutually_exclusive_group()
    compress_group.add_argument(
        "--journal-compress",
        action="store_true",
        default=None,
        help="Compress journal payloads using permstream",
    )
    compress_group.add_argument(
        "--journal-no-compress",
        action="store_true",
        default=None,
        help="Disable journal payload compression",
    )
    parser.add_argument(
        "--journal-compress-min-size",
        type=int,
        default=None,
        help="Minimum payload size to attempt compression",
    )
    parser.add_argument(
        "--journal-compress-min-gain",
        type=int,
        default=None,
        help="Minimum byte savings to keep compressed payload",
    )
    parser.add_argument(
        "--journal-inline-max",
        type=int,
        default=None,
        help="Inline journal payload max size before using blob extents",
    )
    parser.add_argument(
        "--journal-extent-size",
        type=int,
        default=None,
        help="Extent size to use when writing blob records",
    )
    args = parser.parse_args()

    apply_journal_profile(args)
    reader = PSFSReader(
        args.container,
        verify=args.verify,
        verify_chunks=args.verify_chunks,
        cache_chunks=args.cache_chunks,
        readahead=args.readahead,
        stats=args.stats,
        journal_path=args.journal,
        journal_blob_path=args.journal_blob,
    )
    fuse = PSFSFuse(reader)
    if args.journal and not args.read_only:
        writer = JournalWriter(
            args.journal,
            sync=args.journal_sync,
            sync_every=args.journal_sync_every,
            blob_path=args.journal_blob,
        )
        fuse.enable_writer(
            writer,
            compress=args.journal_compress,
            compress_min_size=args.journal_compress_min_size,
            compress_min_gain=args.journal_compress_min_gain,
            inline_max=args.journal_inline_max,
            extent_size=args.journal_extent_size,
        )

    options = {
        "foreground": args.foreground,
        "ro": args.read_only or args.journal is None,
        "debug": args.debug,
    }
    if args.allow_other:
        options["allow_other"] = True
    FUSE(fuse, args.mountpoint, **options)


if __name__ == "__main__":
    main()
