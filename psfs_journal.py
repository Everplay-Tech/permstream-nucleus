import argparse
import io
import json
import os
import struct
import time

from permstream_pro import CodecConfig, compress_stream

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


def normalize_path(path):
    path = os.path.normpath(path)
    if path.startswith("/"):
        path = path[1:]
    if path == ".":
        return ""
    return path


def ensure_journal(path):
    if os.path.exists(path):
        with open(path, "rb") as handle:
            header = handle.read(PSFJ_HEADER_SIZE)
        if len(header) < PSFJ_HEADER_SIZE:
            raise ValueError("Invalid PSFJ header")
        magic, version, _, _ = struct.unpack(PSFJ_HEADER_FMT, header)
        if magic != PSFJ_MAGIC:
            raise ValueError("Invalid PSFJ magic")
        if version != PSFJ_VERSION:
            raise ValueError("Unsupported PSFJ version")
        return
    with open(path, "wb") as handle:
        handle.write(struct.pack(PSFJ_HEADER_FMT, PSFJ_MAGIC, PSFJ_VERSION, PSFJ_HEADER_SIZE, 0))


def iter_records(journal_path):
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
            if data_len:
                handle.seek(data_len, os.SEEK_CUR)
            yield {
                "type": rec_type,
                "flags": rec_flags,
                "path": normalize_path(path_bytes.decode("utf-8")),
                "mode": mode,
                "uid": uid,
                "gid": gid,
                "mtime_ns": mtime_ns,
                "size": size,
            }


def append_record(path, rec_type, flags, mode, uid, gid, mtime_ns, size, data):
    path = normalize_path(path)
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
    return header + path_bytes + data


def compress_payload(data, args):
    if not args.compress:
        return data, 0
    config = CodecConfig(
        chunk_size=args.chunk_size,
        block_size=args.block_size,
        use_rank=not args.no_rank,
        predictor_mode=args.predictor,
        seed=args.seed,
        entropy_skip=args.entropy_skip,
        transform=args.transform,
        force_raw=False,
    )
    encoded = io.BytesIO()
    compress_stream(io.BytesIO(data), encoded, config)
    return encoded.getvalue(), PSFJ_FLAG_COMPRESSED


def cmd_add(args):
    ensure_journal(args.journal)
    stat = os.stat(args.source)
    with open(args.source, "rb") as handle:
        data = handle.read()
    payload, flags = compress_payload(data, args)
    record = append_record(
        args.path,
        PSFJ_TYPE_FILE,
        flags,
        stat.st_mode & 0xFFFF,
        stat.st_uid,
        stat.st_gid,
        int(stat.st_mtime_ns),
        len(data),
        payload,
    )
    with open(args.journal, "ab") as handle:
        handle.write(record)


def cmd_mkdir(args):
    ensure_journal(args.journal)
    mode = args.mode
    uid = os.getuid()
    gid = os.getgid()
    mtime_ns = int(time.time() * 1e9)
    record = append_record(
        args.path,
        PSFJ_TYPE_DIR,
        0,
        mode & 0xFFFF,
        uid,
        gid,
        mtime_ns,
        0,
        b"",
    )
    with open(args.journal, "ab") as handle:
        handle.write(record)


def cmd_symlink(args):
    ensure_journal(args.journal)
    mode = args.mode
    uid = os.getuid()
    gid = os.getgid()
    mtime_ns = int(time.time() * 1e9)
    data = args.target.encode("utf-8")
    record = append_record(
        args.path,
        PSFJ_TYPE_SYMLINK,
        0,
        mode & 0xFFFF,
        uid,
        gid,
        mtime_ns,
        len(data),
        data,
    )
    with open(args.journal, "ab") as handle:
        handle.write(record)


def cmd_delete(args):
    ensure_journal(args.journal)
    flags = PSFJ_FLAG_RECURSIVE if args.recursive else 0
    mtime_ns = int(time.time() * 1e9)
    record = append_record(
        args.path,
        PSFJ_TYPE_DELETE,
        flags,
        0,
        0,
        0,
        mtime_ns,
        0,
        b"",
    )
    with open(args.journal, "ab") as handle:
        handle.write(record)


def cmd_max_mtime(args):
    max_mtime = None
    for record in iter_records(args.journal):
        if max_mtime is None or record["mtime_ns"] > max_mtime:
            max_mtime = record["mtime_ns"]
    print(max_mtime or 0)


def cmd_stats(args):
    count = 0
    min_mtime = None
    max_mtime = None
    last_path = ""
    last_mtime = None
    for record in iter_records(args.journal):
        count += 1
        mtime_ns = record["mtime_ns"]
        if min_mtime is None or mtime_ns < min_mtime:
            min_mtime = mtime_ns
        if max_mtime is None or mtime_ns > max_mtime:
            max_mtime = mtime_ns
        if last_mtime is None or mtime_ns > last_mtime or mtime_ns == last_mtime:
            last_mtime = mtime_ns
            last_path = record["path"]
    stats = {
        "count": count,
        "min_mtime_ns": min_mtime or 0,
        "max_mtime_ns": max_mtime or 0,
        "last_path": last_path,
    }
    if args.json:
        print(json.dumps(stats, sort_keys=True))
        return
    print(f"count: {stats['count']}")
    print(f"min_mtime_ns: {stats['min_mtime_ns']}")
    print(f"max_mtime_ns: {stats['max_mtime_ns']}")
    print(f"last_path: {stats['last_path']}")


def parse_args():
    parser = argparse.ArgumentParser(description="PSFS journal utility")
    sub = parser.add_subparsers(dest="cmd", required=True)

    add = sub.add_parser("add", help="Add or replace a file record")
    add.add_argument("journal")
    add.add_argument("path", help="Path inside the container")
    add.add_argument("source", help="Source file on disk")
    add.add_argument("--compress", action="store_true")
    add.add_argument("--chunk-size", type=int, default=512 * 1024)
    add.add_argument("--block-size", type=int, default=64)
    add.add_argument("--predictor", choices=["seeded", "header"], default="seeded")
    add.add_argument("--seed", type=int, default=1337)
    add.add_argument("--no-rank", action="store_true")
    add.add_argument("--entropy-skip", type=float, default=7.5)
    add.add_argument("--transform", choices=["none", "delta", "xor", "evenodd"], default="none")

    mkdir = sub.add_parser("mkdir", help="Add a directory record")
    mkdir.add_argument("journal")
    mkdir.add_argument("path")
    mkdir.add_argument("--mode", type=lambda x: int(x, 8), default=0o755)

    symlink = sub.add_parser("symlink", help="Add a symlink record")
    symlink.add_argument("journal")
    symlink.add_argument("path")
    symlink.add_argument("target")
    symlink.add_argument("--mode", type=lambda x: int(x, 8), default=0o777)

    delete = sub.add_parser("delete", help="Delete a path")
    delete.add_argument("journal")
    delete.add_argument("path")
    delete.add_argument("--recursive", action="store_true")

    max_mtime = sub.add_parser("max-mtime", help="Print max mtime_ns in the journal")
    max_mtime.add_argument("journal")

    stats = sub.add_parser("stats", help="Print record count and mtime range")
    stats.add_argument("journal")
    stats.add_argument("--json", action="store_true")

    return parser.parse_args()


def main():
    args = parse_args()
    if args.cmd == "add":
        cmd_add(args)
    elif args.cmd == "mkdir":
        cmd_mkdir(args)
    elif args.cmd == "symlink":
        cmd_symlink(args)
    elif args.cmd == "delete":
        cmd_delete(args)
    elif args.cmd == "max-mtime":
        cmd_max_mtime(args)
    elif args.cmd == "stats":
        cmd_stats(args)


if __name__ == "__main__":
    main()
