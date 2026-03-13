# Design Choices (PSFS Journal + Durability Profiles)

Last updated: 2025-12-27

## Goals
- Keep the base container immutable while enabling fast, safe writes via an append-only journal.
- Allow different durability/perf tradeoffs without changing the on-disk format.
- Provide operational safety for repair and compaction without data loss.

## Journal Profiles
Profiles are convenience defaults for journal write behavior. Explicit flags always override profile defaults.
Use `--journal-no-compress` to explicitly disable compression when a profile enables it.

### business
Intended for general office/document workloads where data is often compressible and latency matters but can be amortized.
- `journal_sync_every = 64`
- `journal_compress = true`
- `journal_compress_min_size = 4096`
- `journal_compress_min_gain = 128`
- `journal_inline_max = 4 MiB`
- `journal_extent_size = 8 MiB`

Rationale:
- Small/medium files compress well; compression reduces journal growth.
- Sync every 64 records balances durability with throughput.

### media
Intended for audio/video production workloads with large, already-compressed assets.
- `journal_sync_every = 16`
- `journal_compress = false`
- `journal_compress_min_size = 65536`
- `journal_compress_min_gain = 1024`
- `journal_inline_max = 4 MiB`
- `journal_extent_size = 16 MiB`

Rationale:
- Media files rarely compress; skip compression by default to avoid CPU cost.
- Smaller sync interval improves durability for large edits without syncing every record.

## Compression Heuristics
Compression is only kept when:
- Payload size >= `journal_compress_min_size`
- Compressed size + `journal_compress_min_gain` < original size

This prevents wasted CPU and avoids storing near-equal payloads.

## Durability Controls
- `--journal-sync` forces fsync on every record (maximum durability, lowest throughput).
- `--journal-sync-every N` batches fsyncs to amortize cost.

On unmount, stats include journal records written, raw/stored bytes, compressed count, and fsync count.

## Repair Model
FSCK supports:
- Copy-out repair (`--repair-journal`) to write a cleaned journal to a new path.
- In-place repair (`--repair`) with optional backups and retention.

Backups are intended to make repairs safe and reversible.

## Compaction Watermark
`--watermark` stores last-applied `mtime_ns` so incremental compaction can be automated without manual cutoffs.

## Blob Extent Journal (Option C)
Large files are stored as journal extents in a sidecar blob file to avoid
loading multi-GB data into memory. Each extent record points at blob offsets,
and a commit record seals the file version. This keeps the journal append-only
while supporting large sequential writes.
Default blob path replaces `.psfj` with `.psfb` (or appends `.psfb`).

## Benchmarking
`bench_journal.py` measures:
- Write throughput vs fsync cadence
- Compression ratio vs min-gain thresholds

Use it to pick defaults tailored to your real data sizes and latency tolerance.
