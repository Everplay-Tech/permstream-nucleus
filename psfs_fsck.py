import argparse
import io
import os
import shutil
import struct
import tempfile
import time

from permstream_pro import decompress_stream, verify_psfs

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
TYPE_NAMES = {
    PSFJ_TYPE_FILE: "file",
    PSFJ_TYPE_SYMLINK: "symlink",
    PSFJ_TYPE_DIR: "dir",
    PSFJ_TYPE_DELETE: "delete",
    PSFJ_TYPE_EXTENT: "extent",
    PSFJ_TYPE_COMMIT: "commit",
}


def derive_blob_path(journal_path):
    root, ext = os.path.splitext(journal_path)
    if ext == ".psfj":
        return root + ".psfb"
    return journal_path + ".psfb"


def normalize_path(path):
    path = os.path.normpath(path)
    if path.startswith("/"):
        path = path[1:]
    if path == ".":
        return ""
    return path


def is_path_safe(path):
    if path == "":
        return True
    if path.startswith(".."):
        return False
    if "/../" in path or path.endswith("/.."):
        return False
    return True


def read_journal_header(handle):
    header = handle.read(PSFJ_HEADER_SIZE)
    if len(header) < PSFJ_HEADER_SIZE:
        raise ValueError("Invalid PSFJ header")
    magic, version, header_size, _ = struct.unpack(PSFJ_HEADER_FMT, header)
    if magic != PSFJ_MAGIC:
        raise ValueError("Invalid PSFJ magic")
    if version != PSFJ_VERSION:
        raise ValueError("Unsupported PSFJ version")
    extra = b""
    if header_size > PSFJ_HEADER_SIZE:
        extra = handle.read(header_size - PSFJ_HEADER_SIZE)
        if len(extra) < header_size - PSFJ_HEADER_SIZE:
            raise ValueError("Truncated PSFJ header")
    return header + extra, header_size


def iter_journal_records(handle):
    seq = 0
    while True:
        offset = handle.tell()
        record = handle.read(PSFJ_RECORD_SIZE)
        if not record:
            break
        if len(record) < PSFJ_RECORD_SIZE:
            yield None, None, offset, "Truncated PSFJ record header"
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
            yield None, None, offset, "Truncated PSFJ record path"
            break
        data = handle.read(data_len)
        if len(data) < data_len:
            yield None, None, offset, "Truncated PSFJ record data"
            break
        path = normalize_path(path_bytes.decode("utf-8"))
        raw = record + path_bytes + data
        payload = {
            "type": rec_type,
            "flags": rec_flags,
            "path": path,
            "mode": mode,
            "uid": uid,
            "gid": gid,
            "mtime_ns": mtime_ns,
            "size": size,
            "data": data,
            "seq": seq,
            "offset": offset,
        }
        seq += 1
        yield payload, raw, offset, None


def validate_record(record, deep=False, blob_handle=None, blob_size=None):
    errors = []
    path = record["path"]
    if not is_path_safe(path):
        errors.append(f"unsafe path '{path}'")
    rec_type = record["type"]
    if rec_type == PSFJ_TYPE_DELETE:
        if record["data"]:
            errors.append("delete record has data")
        if record["size"] != 0:
            errors.append("delete record size is nonzero")
        return errors
    if rec_type == PSFJ_TYPE_DIR:
        if record["data"]:
            errors.append("dir record has data")
        if record["size"] != 0:
            errors.append("dir record size is nonzero")
        return errors
    if rec_type == PSFJ_TYPE_COMMIT:
        if record["data"]:
            errors.append("commit record has data")
        return errors
    if rec_type == PSFJ_TYPE_EXTENT:
        if len(record["data"]) != PSFJ_EXTENT_SIZE:
            errors.append("extent record size mismatch")
            return errors
        file_offset, blob_offset, stored_len = struct.unpack(
            PSFJ_EXTENT_FMT, record["data"]
        )
        if record["size"] < 0 or stored_len < 0:
            errors.append("extent has negative length")
        if blob_size is None:
            errors.append("missing blob file for extents")
            return errors
        if blob_size is not None:
            if blob_offset + stored_len > blob_size:
                errors.append("extent exceeds blob size")
        if deep and (record["flags"] & PSFJ_FLAG_COMPRESSED):
            if blob_handle is None:
                errors.append("missing blob for compressed extent")
            else:
                blob_handle.seek(blob_offset)
                payload = blob_handle.read(stored_len)
                if len(payload) < stored_len:
                    errors.append("extent blob truncated")
                else:
                    try:
                        out = io.BytesIO()
                        decompress_stream(io.BytesIO(payload), out)
                        data = out.getvalue()
                        if len(data) != record["size"]:
                            errors.append("extent decompressed size mismatch")
                    except Exception as exc:
                        errors.append(f"extent decompress error: {exc}")
        return errors
    data = record["data"] or b""
    if record["flags"] & PSFJ_FLAG_COMPRESSED:
        if deep:
            try:
                out = io.BytesIO()
                decompress_stream(io.BytesIO(data), out)
                data = out.getvalue()
            except Exception as exc:
                errors.append(f"compressed payload error: {exc}")
    if rec_type == PSFJ_TYPE_SYMLINK:
        if len(data) != record["size"]:
            errors.append("symlink size mismatch")
    elif rec_type == PSFJ_TYPE_FILE:
        if deep and len(data) != record["size"]:
            errors.append("file size mismatch")
    else:
        errors.append(f"unknown record type {rec_type}")
    return errors


def validate_journal(journal_path, deep=False):
    errors = []
    with open(journal_path, "rb") as handle:
        read_journal_header(handle)
        for record, _, offset, err in iter_journal_records(handle):
            if err:
                errors.append(f"offset {offset}: {err}")
                break
            rec_errors = validate_record(record, deep=deep)
            for rec_err in rec_errors:
                errors.append(f"record {record['seq']}: {rec_err}")
    return errors


def collect_journal_records(journal_path, deep=False, blob_path=None):
    records = []
    errors = []
    blob_handle = None
    blob_size = None
    if blob_path and os.path.exists(blob_path):
        blob_handle = open(blob_path, "rb")
        blob_size = os.path.getsize(blob_path)
    try:
        with open(journal_path, "rb") as handle:
            read_journal_header(handle)
            for record, _, offset, err in iter_journal_records(handle):
                if err:
                    errors.append(f"offset {offset}: {err}")
                    break
                rec_errors = validate_record(
                    record, deep=deep, blob_handle=blob_handle, blob_size=blob_size
                )
                for rec_err in rec_errors:
                    errors.append(f"record {record['seq']}: {rec_err}")
                if not rec_errors:
                    records.append(record)
    except Exception as exc:
        errors.append(f"journal verify failed: {exc}")
    finally:
        if blob_handle:
            blob_handle.close()
    return records, errors


def repair_journal(journal_path, output_path, deep=False):
    kept = 0
    dropped = 0
    truncated = None
    with open(journal_path, "rb") as inp, open(output_path, "wb") as out:
        header_bytes, header_size = read_journal_header(inp)
        out.write(header_bytes)
        if header_size > len(header_bytes):
            raise ValueError("PSFJ header size mismatch")
        for record, raw, offset, err in iter_journal_records(inp):
            if err:
                truncated = err
                break
            rec_errors = validate_record(record, deep=deep)
            if rec_errors:
                dropped += 1
                continue
            out.write(raw)
            kept += 1
    return kept, dropped, truncated


def diagnose_conflicts(records, limit):
    by_path = {}
    recursive_deletes = []
    for record in records:
        if record["type"] == PSFJ_TYPE_EXTENT:
            continue
        by_path.setdefault(record["path"], []).append(record)
        if record["type"] == PSFJ_TYPE_DELETE and (record["flags"] & PSFJ_FLAG_RECURSIVE):
            recursive_deletes.append(record)
    conflict_paths = [path for path, recs in by_path.items() if len(recs) > 1]
    masked = 0
    masked_examples = []
    for record in records:
        path = record["path"]
        if record["type"] == PSFJ_TYPE_DELETE:
            continue
        for delete_rec in recursive_deletes:
            del_path = delete_rec["path"]
            if del_path == "":
                match = True
            elif path == del_path or path.startswith(del_path + "/"):
                match = True
            else:
                match = False
            if match and (delete_rec["mtime_ns"], delete_rec["seq"]) >= (
                record["mtime_ns"],
                record["seq"],
            ):
                masked += 1
                if len(masked_examples) < limit:
                    masked_examples.append(path)
                break
    summary = {
        "conflict_paths": conflict_paths,
        "masked": masked,
        "masked_examples": masked_examples,
    }
    return summary


def parse_args():
    parser = argparse.ArgumentParser(description="FSCK for PSFS containers and PSFJ journals")
    parser.add_argument("container")
    parser.add_argument("--journal")
    parser.add_argument("--journal-blob")
    parser.add_argument("--no-base", action="store_true")
    parser.add_argument("--deep", action="store_true")
    parser.add_argument("--conflicts", action="store_true", help="Report journal conflicts")
    parser.add_argument("--conflicts-limit", type=int, default=20)
    parser.add_argument("--repair-journal", help="Write a repaired journal to this path")
    parser.add_argument("--repair", action="store_true", help="Repair journal in-place")
    parser.add_argument("--backup-path", help="Backup path for in-place repair")
    parser.add_argument("--backup-dir", help="Backup directory for in-place repair")
    parser.add_argument("--backup-keep", type=int, help="Keep N newest backups in backup dir")
    parser.add_argument(
        "--backup-suffix",
        help="Backup suffix for in-place repair (default: .bak.<timestamp>)",
    )
    parser.add_argument("--no-backup", action="store_true", help="Skip backup for in-place repair")
    parser.add_argument("--force", action="store_true", help="Overwrite existing backup path")
    return parser.parse_args()


def run(args):
    base_errors = []
    journal_errors = []
    if (args.repair_journal or args.repair or args.conflicts) and not args.journal:
        raise SystemExit("--repair/--repair-journal/--conflicts requires --journal")
    if args.repair and args.repair_journal:
        raise SystemExit("--repair and --repair-journal cannot be used together")
    if args.no_backup and (args.backup_path or args.backup_suffix or args.backup_dir or args.backup_keep):
        raise SystemExit("--no-backup cannot be combined with --backup-path/--backup-suffix")
    if args.backup_keep is not None and args.backup_keep < 0:
        raise SystemExit("--backup-keep must be >= 0")
    if not args.no_base:
        try:
            verify_psfs(args.container)
        except Exception as exc:
            base_errors.append(f"base verify failed: {exc}")
    if args.journal:
        blob_path = args.journal_blob
        if blob_path is None:
            blob_path = derive_blob_path(args.journal)
        if not os.path.exists(blob_path):
            blob_path = None
        records, journal_errors = collect_journal_records(
            args.journal, deep=args.deep, blob_path=blob_path
        )
        if args.repair_journal:
            try:
                kept, dropped, truncated = repair_journal(
                    args.journal, args.repair_journal, deep=args.deep
                )
                print(
                    f"journal repair: kept={kept} dropped={dropped} truncated={truncated or 'no'}"
                )
            except Exception as exc:
                journal_errors.append(f"journal repair failed: {exc}")
        if args.repair:
            temp_path = None
            try:
                fd, temp_path = tempfile.mkstemp(prefix="psfj_repair_", dir=os.path.dirname(args.journal) or ".")
                os.close(fd)
                kept, dropped, truncated = repair_journal(
                    args.journal, temp_path, deep=args.deep
                )
                repaired_records, repaired_errors = collect_journal_records(
                    temp_path, deep=args.deep, blob_path=blob_path
                )
                if repaired_errors:
                    journal_errors.append("repaired journal failed validation")
                    journal_errors.extend(repaired_errors)
                else:
                    backup_path = None
                    if not args.no_backup:
                        backup_dir = args.backup_dir
                        if backup_dir is None and args.backup_keep:
                            backup_dir = os.path.dirname(args.journal) or "."
                        if backup_dir:
                            os.makedirs(backup_dir, exist_ok=True)
                        if args.backup_path:
                            backup_path = args.backup_path
                        elif args.backup_suffix:
                            if backup_dir:
                                backup_path = os.path.join(
                                    backup_dir, os.path.basename(args.journal) + args.backup_suffix
                                )
                            else:
                                backup_path = args.journal + args.backup_suffix
                        else:
                            stamp = time.strftime("%Y%m%d_%H%M%S")
                            name = os.path.basename(args.journal) + f".bak.{stamp}"
                            backup_path = (
                                os.path.join(backup_dir, name) if backup_dir else args.journal + f".bak.{stamp}"
                            )
                        if backup_path == args.journal:
                            raise ValueError("backup path equals journal path")
                        if os.path.exists(backup_path) and not args.force:
                            raise ValueError("backup path exists (use --force to overwrite)")
                        shutil.copy2(args.journal, backup_path)
                        print(f"journal backup: {backup_path}")
                        if args.backup_keep:
                            retention_dir = backup_dir or os.path.dirname(backup_path)
                            prefix = os.path.basename(args.journal) + (
                                args.backup_suffix if args.backup_suffix else ".bak."
                            )
                            try:
                                candidates = [
                                    os.path.join(retention_dir, name)
                                    for name in os.listdir(retention_dir)
                                    if name.startswith(prefix)
                                ]
                                candidates.sort(key=lambda p: os.path.getmtime(p), reverse=True)
                                for stale in candidates[args.backup_keep :]:
                                    os.unlink(stale)
                            except OSError:
                                pass
                    os.replace(temp_path, args.journal)
                    temp_path = None
                    records = repaired_records
                    journal_errors = []
                    print(
                        f"journal in-place repair: kept={kept} dropped={dropped} truncated={truncated or 'no'}"
                    )
            except Exception as exc:
                journal_errors.append(f"journal in-place repair failed: {exc}")
            finally:
                if temp_path and os.path.exists(temp_path):
                    os.unlink(temp_path)
        if args.conflicts and records:
            summary = diagnose_conflicts(records, args.conflicts_limit)
            conflicts = summary["conflict_paths"]
            print(f"conflicts: {len(conflicts)} paths with multiple records")
            for path in conflicts[: args.conflicts_limit]:
                recs = sorted(
                    [r for r in records if r["path"] == path],
                    key=lambda r: (r["mtime_ns"], r["seq"]),
                )
                winner = recs[-1]
                print(
                    f"- {path} ({len(recs)} records) winner={TYPE_NAMES.get(winner['type'], winner['type'])} mtime_ns={winner['mtime_ns']}"
                )
            if summary["masked"]:
                print(
                    f"masked-by-recursive-delete: {summary['masked']} records (examples: {', '.join(summary['masked_examples'])})"
                )
    errors = base_errors + journal_errors
    if errors:
        print("PSFS fsck: FAIL")
        for err in errors:
            print("-", err)
        raise SystemExit(1)
    print("PSFS fsck: OK")


def main():
    args = parse_args()
    run(args)


if __name__ == "__main__":
    main()
