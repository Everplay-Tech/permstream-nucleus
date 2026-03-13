# PSFS Status

Last updated: 2025-12-27

## Current State
- PSFS container format implemented with pack/unpack/verify.
- Read-only FUSE mount works with caching, readahead, and CRC checks.
- PSFJ journal overlay supports add/replace/delete + symlink/dir records.
- Write-capable FUSE appends PSFJ records (create/write/truncate/unlink/mkdir/rmdir/rename/symlink).
- Journal conflict policy: newest `mtime_ns` wins; ties by append order.
- Compaction merges base + journal into a fresh PSFS.
- Incremental compaction supports `--since-ns` filtering and optional journal truncation.
- FSCK validates base containers and journals (optional deep decode).
- FSCK can report conflicts and write a repaired journal copy.
- FSCK supports in-place journal repair with optional backups.
- FSCK in-place repair supports backup directories with retention.
- Compaction can read/write a watermark file for automatic incremental runs.
- Journal writes support periodic fsync and compression heuristics with stats on unmount.
- Journal benchmark script added for sync/compression tuning.
- Journal mount profiles added for business/media defaults.
- Large file writes use blob extents with commit records in the journal.

## Verified Flows
- Pack/unpack preserves files, empty dirs, and symlinks.
- FUSE mount read path works for files and symlinks.
- Journal overlay overrides base files and removes directories.
- Compaction produces a valid PSFS that matches journal overlay.

## Tooling
- `psfs_fuse.py`: mount read-only with cache/readahead/CRC.
- `psfs_journal.py`: append-only journal writer.
- `psfs_compact.py`: merge base + journal into new PSFS (incremental + watermark + truncate).
- `psfs_fsck.py`: integrity checks + conflict diagnostics + journal repair.
- `bench_journal.py`: benchmark journal compression + fsync cadence.
- `psfs.py`: unified CLI for PSFS tasks.

## Open Decisions
- Journal durability defaults (sync cadence vs buffered).
- FSCK in-place repair workflow vs copy-out defaults.
- Performance instrumentation + cache tuning defaults.

## Next Steps
- Add fsck in-place repair mode and richer conflict triage output.
- Add benchmark script for journal compression heuristics + sync cadence.
- Expand docs + demo script for watermark-driven compaction loops.
