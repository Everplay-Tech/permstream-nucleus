import argparse
import io
import os
import shutil
import struct
import tempfile

from permstream_pro import (
    CodecConfig,
    PSFS_MAGIC,
    PSFS_SUPER_FMT,
    PSFS_SUPER_SIZE,
    PSFS_VERSION,
    TRANSFORM_NAMES,
    pack_psfs,
    unpack_psfs,
    decompress_stream,
    verify_psfs,
)

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


def normalize_path(path):
    path = os.path.normpath(path)
    if path.startswith("/"):
        path = path[1:]
    if path == ".":
        return ""
    return path


def derive_blob_path(journal_path):
    root, ext = os.path.splitext(journal_path)
    if ext == ".psfj":
        return root + ".psfb"
    return journal_path + ".psfb"


def load_base_config(container, args):
    with open(container, "rb") as handle:
        data = handle.read(PSFS_SUPER_SIZE)
    (
        magic,
        version,
        _,
        _,
        chunk_size,
        block_size,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        codec_flags,
        seed,
        w0,
        w1,
        w2,
        w3,
        transform_id,
        _,
    ) = struct.unpack(PSFS_SUPER_FMT, data)
    if magic != PSFS_MAGIC:
        raise ValueError("Invalid PSFS magic")
    if version != PSFS_VERSION:
        raise ValueError("Unsupported PSFS version")
    predictor_mode = "header" if (codec_flags & (1 << 1)) else "seeded"
    use_rank = bool(codec_flags & (1 << 0))
    transform = TRANSFORM_NAMES.get(transform_id, "none")

    return CodecConfig(
        chunk_size=args.chunk_size or chunk_size,
        block_size=args.block_size or block_size,
        use_rank=not args.no_rank if args.no_rank is not None else use_rank,
        predictor_mode=args.predictor or predictor_mode,
        seed=args.seed if args.seed is not None else seed,
        weights=[w0, w1, w2, w3],
        entropy_skip=args.entropy_skip if args.entropy_skip is not None else 7.5,
        transform=args.transform or transform,
        force_raw=False,
    )


def read_journal_records(journal_path):
    with open(journal_path, "rb") as handle:
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
                raise ValueError("Truncated PSFJ record header")
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
                raise ValueError("Truncated PSFJ record path")
            data = handle.read(data_len)
            if len(data) < data_len:
                raise ValueError("Truncated PSFJ record data")
            path = normalize_path(path_bytes.decode("utf-8"))
            yield {
                "type": rec_type,
                "flags": rec_flags,
                "path": path,
                "mode": mode,
                "uid": uid,
                "gid": gid,
                "mtime_ns": mtime_ns,
                "size": size,
                "data": data,
            }


def decode_payload(record):
    data = record["data"] or b""
    if record["flags"] & PSFJ_FLAG_COMPRESSED:
        out = io.BytesIO()
        decompress_stream(io.BytesIO(data), out)
        data = out.getvalue()
    return data


def apply_metadata(path, record, follow_symlinks=True):
    mode = record["mode"] & 0o7777
    try:
        os.chmod(path, mode, follow_symlinks=follow_symlinks)
    except (OSError, TypeError):
        pass
    try:
        os.utime(path, ns=(record["mtime_ns"], record["mtime_ns"]), follow_symlinks=follow_symlinks)
    except (OSError, TypeError):
        pass


def remove_path(path, recursive):
    if not os.path.lexists(path):
        return
    if os.path.islink(path) or os.path.isfile(path):
        os.unlink(path)
        return
    if os.path.isdir(path):
        if recursive:
            shutil.rmtree(path)
        else:
            os.rmdir(path)


def apply_journal(temp_dir, journal_path, allow_non_empty=False, since_ns=None, blob_path=None):
    records = list(read_journal_records(journal_path))
    for idx, record in enumerate(records):
        record["seq"] = idx
    if since_ns is not None:
        records = [record for record in records if record["mtime_ns"] >= since_ns]
    records.sort(key=lambda r: (r["mtime_ns"], r["seq"]))
    max_mtime = None
    pending_extents = {}
    blob_handle = None
    if blob_path and os.path.exists(blob_path):
        blob_handle = open(blob_path, "rb")
    for record in records:
        max_mtime = record["mtime_ns"] if max_mtime is None else max(max_mtime, record["mtime_ns"])
        rel_path = record["path"]
        if rel_path == "":
            target_path = temp_dir
        else:
            target_path = os.path.join(temp_dir, rel_path)

        if record["type"] == PSFJ_TYPE_DELETE:
            if record["flags"] & PSFJ_FLAG_RECURSIVE:
                prefix = rel_path.rstrip("/")
                for key in list(pending_extents.keys()):
                    if key == prefix or key.startswith(prefix + "/"):
                        pending_extents.pop(key, None)
            else:
                pending_extents.pop(rel_path, None)
            try:
                remove_path(target_path, bool(record["flags"] & PSFJ_FLAG_RECURSIVE))
            except OSError:
                if not allow_non_empty:
                    raise
            continue
        if record["type"] == PSFJ_TYPE_EXTENT:
            if len(record["data"]) != PSFJ_EXTENT_SIZE:
                raise ValueError("Invalid PSFJ extent record")
            file_offset, blob_offset, stored_len = struct.unpack(
                PSFJ_EXTENT_FMT, record["data"]
            )
            pending_extents.setdefault(rel_path, []).append(
                {
                    "file_offset": file_offset,
                    "raw_len": record["size"],
                    "blob_offset": blob_offset,
                    "stored_len": stored_len,
                    "compressed": bool(record["flags"] & PSFJ_FLAG_COMPRESSED),
                }
            )
            continue
        if record["type"] == PSFJ_TYPE_COMMIT:
            extents = pending_extents.pop(rel_path, [])
            parent = os.path.dirname(target_path)
            if parent:
                os.makedirs(parent, exist_ok=True)
            if os.path.lexists(target_path):
                remove_path(target_path, True)
            with open(target_path, "wb") as handle:
                if extents:
                    if blob_handle is None:
                        raise ValueError("Missing PSFJ blob file for extents")
                    extents.sort(key=lambda e: e["file_offset"])
                    for extent in extents:
                        blob_handle.seek(extent["blob_offset"])
                        payload = blob_handle.read(extent["stored_len"])
                        if len(payload) < extent["stored_len"]:
                            raise ValueError("Truncated PSFJ blob data")
                        if extent["compressed"]:
                            out = io.BytesIO()
                            decompress_stream(io.BytesIO(payload), out)
                            data = out.getvalue()
                        else:
                            data = payload
                        if len(data) != extent["raw_len"]:
                            raise ValueError("Extent size mismatch")
                        handle.seek(extent["file_offset"])
                        handle.write(data)
                handle.truncate(record["size"])
            apply_metadata(target_path, record)
            continue

        parent = os.path.dirname(target_path)
        if parent:
            os.makedirs(parent, exist_ok=True)

        if record["type"] == PSFJ_TYPE_DIR:
            pending_extents.pop(rel_path, None)
            if os.path.lexists(target_path) and not os.path.isdir(target_path):
                remove_path(target_path, True)
            os.makedirs(target_path, exist_ok=True)
            apply_metadata(target_path, record)
            continue

        if os.path.lexists(target_path):
            remove_path(target_path, True)

        if record["type"] == PSFJ_TYPE_SYMLINK:
            pending_extents.pop(rel_path, None)
            data = decode_payload(record)
            os.symlink(os.fsdecode(data), target_path)
            apply_metadata(target_path, record, follow_symlinks=False)
            continue

        if record["type"] == PSFJ_TYPE_FILE:
            pending_extents.pop(rel_path, None)
            data = decode_payload(record)
            with open(target_path, "wb") as handle:
                handle.write(data)
            apply_metadata(target_path, record)
            continue

        raise ValueError("Unsupported PSFJ record type")
    if blob_handle:
        blob_handle.close()
    return max_mtime


def parse_args():
    parser = argparse.ArgumentParser(description="Compact PSFS container + journal into new PSFS")
    parser.add_argument("input")
    parser.add_argument("output")
    parser.add_argument("--journal", required=True)
    parser.add_argument("--journal-blob")
    parser.add_argument("--verify", action="store_true")
    parser.add_argument("--keep-temp", action="store_true")
    parser.add_argument("--allow-non-empty", action="store_true")
    parser.add_argument(
        "--since-ns",
        type=int,
        help="Apply only journal records with mtime_ns >= this value",
    )
    parser.add_argument(
        "--truncate-journal",
        action="store_true",
        help="Truncate the journal to its header after successful compaction",
    )
    parser.add_argument(
        "--watermark",
        help="Read/write compaction watermark (mtime_ns) for automatic --since-ns",
    )
    parser.add_argument("--chunk-size", type=int)
    parser.add_argument("--block-size", type=int)
    parser.add_argument("--predictor", choices=["seeded", "header"])
    parser.add_argument("--seed", type=int)
    parser.add_argument("--no-rank", action="store_true")
    parser.add_argument("--entropy-skip", type=float)
    parser.add_argument("--transform", choices=["none", "delta", "xor", "evenodd"])
    return parser.parse_args()


def truncate_journal(path):
    with open(path, "r+b") as handle:
        handle.seek(0)
        handle.write(struct.pack(PSFJ_HEADER_FMT, PSFJ_MAGIC, PSFJ_VERSION, PSFJ_HEADER_SIZE, 0))
        handle.truncate(PSFJ_HEADER_SIZE)


def read_watermark(path):
    if not path or not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as handle:
        data = handle.read().strip()
    if not data:
        return None
    return int(data)


def write_watermark(path, value):
    if not path:
        return
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(str(int(value)))


def run(args):
    config = load_base_config(args.input, args)
    watermark_value = None
    if args.watermark and args.since_ns is None:
        watermark_value = read_watermark(args.watermark)
        if watermark_value is not None:
            args.since_ns = watermark_value
    blob_path = args.journal_blob or derive_blob_path(args.journal)
    if not os.path.exists(blob_path):
        blob_path = None
    temp_dir = tempfile.mkdtemp(prefix="psfs_compact_")
    applied_max = None
    try:
        unpack_psfs(args.input, temp_dir, verify=False)
        applied_max = apply_journal(
            temp_dir,
            args.journal,
            allow_non_empty=args.allow_non_empty,
            since_ns=args.since_ns,
            blob_path=blob_path,
        )
        pack_psfs(temp_dir, args.output, config, verify=args.verify)
    finally:
        if args.keep_temp:
            print(f"Temp dir kept at {temp_dir}")
        else:
            shutil.rmtree(temp_dir, ignore_errors=True)

    if args.verify:
        verify_psfs(args.output)
    if args.truncate_journal:
        truncate_journal(args.journal)
    if args.watermark:
        if applied_max is None:
            applied_max = watermark_value if watermark_value is not None else (args.since_ns or 0)
        write_watermark(args.watermark, applied_max)


def main():
    args = parse_args()
    run(args)


if __name__ == "__main__":
    main()
