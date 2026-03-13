import argparse
import sys

from permstream_pro import CodecConfig, pack_psfs, unpack_psfs, verify_psfs
from psfs_compact import run as compact_run
from psfs_fsck import run as fsck_run
import psfs_journal

PSFS_CLI_VERSION = "0.1"

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


def load_fuse():
    try:
        from fuse import FUSE
    except Exception as exc:
        raise SystemExit("Missing or broken fusepy. Install requirements.txt to use mount.") from exc
    return FUSE


def parse_args():
    parser = argparse.ArgumentParser(description="PSFS filesystem tools")
    parser.add_argument("--version", action="store_true", help="Show version")
    sub = parser.add_subparsers(dest="cmd", required=False)

    pack = sub.add_parser("pack", help="Pack a directory into a PSFS container")
    pack.add_argument("input_dir")
    pack.add_argument("output")
    pack.add_argument("--chunk-size", type=int, default=512 * 1024)
    pack.add_argument("--block-size", type=int, default=64)
    pack.add_argument("--predictor", choices=["seeded", "header"], default="seeded")
    pack.add_argument("--seed", type=int, default=1337)
    pack.add_argument("--no-rank", action="store_true")
    pack.add_argument("--entropy-skip", type=float, default=7.5)
    pack.add_argument("--transform", choices=["none", "delta", "xor", "evenodd"], default="none")
    pack.add_argument("--verify", action="store_true")

    unpack = sub.add_parser("unpack", help="Unpack a PSFS container to a directory")
    unpack.add_argument("input")
    unpack.add_argument("output_dir")
    unpack.add_argument("--verify", action="store_true")

    verify = sub.add_parser("verify", help="Verify a PSFS container")
    verify.add_argument("input")

    mount = sub.add_parser("mount", help="Mount a PSFS container (read-only)")
    mount.add_argument("container")
    mount.add_argument("mountpoint")
    mount.add_argument("--verify", action="store_true")
    mount.add_argument("--verify-chunks", action="store_true")
    mount.add_argument("--cache-chunks", type=int, default=32)
    mount.add_argument("--readahead", type=int, default=2)
    mount.add_argument("--stats", action="store_true")
    mount.add_argument("--journal")
    mount.add_argument("--read-only", action="store_true")
    mount.add_argument("--journal-profile", choices=["business", "media"])
    mount.add_argument("--journal-blob")
    mount.add_argument("--journal-sync", action="store_true")
    mount.add_argument("--journal-sync-every", type=int, default=None)
    compress_group = mount.add_mutually_exclusive_group()
    compress_group.add_argument("--journal-compress", action="store_true", default=None)
    compress_group.add_argument("--journal-no-compress", action="store_true", default=None)
    mount.add_argument("--journal-compress-min-size", type=int, default=None)
    mount.add_argument("--journal-compress-min-gain", type=int, default=None)
    mount.add_argument("--journal-inline-max", type=int, default=None)
    mount.add_argument("--journal-extent-size", type=int, default=None)
    mount.add_argument("--foreground", action="store_true")
    mount.add_argument("--allow-other", action="store_true")
    mount.add_argument("--debug", action="store_true")

    journal = sub.add_parser("journal", help="Manage PSFJ journal files")
    journal_sub = journal.add_subparsers(dest="journal_cmd", required=True)

    j_add = journal_sub.add_parser("add", help="Add or replace a file record")
    j_add.add_argument("journal")
    j_add.add_argument("path")
    j_add.add_argument("source")
    j_add.add_argument("--compress", action="store_true")
    j_add.add_argument("--chunk-size", type=int, default=512 * 1024)
    j_add.add_argument("--block-size", type=int, default=64)
    j_add.add_argument("--predictor", choices=["seeded", "header"], default="seeded")
    j_add.add_argument("--seed", type=int, default=1337)
    j_add.add_argument("--no-rank", action="store_true")
    j_add.add_argument("--entropy-skip", type=float, default=7.5)
    j_add.add_argument("--transform", choices=["none", "delta", "xor", "evenodd"], default="none")

    j_mkdir = journal_sub.add_parser("mkdir", help="Add a directory record")
    j_mkdir.add_argument("journal")
    j_mkdir.add_argument("path")
    j_mkdir.add_argument("--mode", type=lambda x: int(x, 8), default=0o755)

    j_symlink = journal_sub.add_parser("symlink", help="Add a symlink record")
    j_symlink.add_argument("journal")
    j_symlink.add_argument("path")
    j_symlink.add_argument("target")
    j_symlink.add_argument("--mode", type=lambda x: int(x, 8), default=0o777)

    j_delete = journal_sub.add_parser("delete", help="Delete a path")
    j_delete.add_argument("journal")
    j_delete.add_argument("path")
    j_delete.add_argument("--recursive", action="store_true")

    j_max = journal_sub.add_parser("max-mtime", help="Print max mtime_ns in a journal")
    j_max.add_argument("journal")

    j_stats = journal_sub.add_parser("stats", help="Print journal record stats")
    j_stats.add_argument("journal")
    j_stats.add_argument("--json", action="store_true")

    compact = sub.add_parser("compact", help="Compact base PSFS + journal into a new container")
    compact.add_argument("input")
    compact.add_argument("output")
    compact.add_argument("--journal", required=True)
    compact.add_argument("--journal-blob")
    compact.add_argument("--verify", action="store_true")
    compact.add_argument("--keep-temp", action="store_true")
    compact.add_argument("--allow-non-empty", action="store_true")
    compact.add_argument(
        "--since-ns",
        type=int,
        help="Apply only journal records with mtime_ns >= this value",
    )
    compact.add_argument(
        "--truncate-journal",
        action="store_true",
        help="Truncate the journal after successful compaction",
    )
    compact.add_argument(
        "--watermark",
        help="Read/write compaction watermark (mtime_ns) for automatic --since-ns",
    )
    compact.add_argument("--chunk-size", type=int)
    compact.add_argument("--block-size", type=int)
    compact.add_argument("--predictor", choices=["seeded", "header"])
    compact.add_argument("--seed", type=int)
    compact.add_argument("--no-rank", action="store_true")
    compact.add_argument("--entropy-skip", type=float)
    compact.add_argument("--transform", choices=["none", "delta", "xor", "evenodd"])

    fsck = sub.add_parser("fsck", help="Validate PSFS containers and journals")
    fsck.add_argument("container")
    fsck.add_argument("--journal")
    fsck.add_argument("--journal-blob")
    fsck.add_argument("--no-base", action="store_true")
    fsck.add_argument("--deep", action="store_true")
    fsck.add_argument("--conflicts", action="store_true")
    fsck.add_argument("--conflicts-limit", type=int, default=20)
    fsck.add_argument("--repair-journal")
    fsck.add_argument("--repair", action="store_true")
    fsck.add_argument("--backup-path")
    fsck.add_argument("--backup-dir")
    fsck.add_argument("--backup-keep", type=int)
    fsck.add_argument("--backup-suffix")
    fsck.add_argument("--no-backup", action="store_true")
    fsck.add_argument("--force", action="store_true")

    return parser.parse_args()


def main():
    args = parse_args()
    if args.version:
        print(f"psfs {PSFS_CLI_VERSION}")
        return
    if args.cmd is None:
        print("Use --help for PSFS commands.")
        return
    if args.cmd == "pack":
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
        pack_psfs(args.input_dir, args.output, config, verify=args.verify)
    elif args.cmd == "unpack":
        unpack_psfs(args.input, args.output_dir, verify=args.verify)
    elif args.cmd == "verify":
        verify_psfs(args.input)
    elif args.cmd == "mount":
        FUSE = load_fuse()
        from psfs_fuse import JournalWriter, PSFSFuse, PSFSReader
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
    elif args.cmd == "journal":
        if args.journal_cmd == "add":
            psfs_journal.cmd_add(args)
        elif args.journal_cmd == "mkdir":
            psfs_journal.cmd_mkdir(args)
        elif args.journal_cmd == "symlink":
            psfs_journal.cmd_symlink(args)
        elif args.journal_cmd == "delete":
            psfs_journal.cmd_delete(args)
        elif args.journal_cmd == "max-mtime":
            psfs_journal.cmd_max_mtime(args)
        elif args.journal_cmd == "stats":
            psfs_journal.cmd_stats(args)
    elif args.cmd == "compact":
        compact_run(args)
    elif args.cmd == "fsck":
        fsck_run(args)
    else:
        raise SystemExit(f"Unknown command: {args.cmd}")


if __name__ == "__main__":
    main()
