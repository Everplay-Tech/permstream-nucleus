# PermStream Container Format (PSFS)

This document defines a maximalist, random-access container format for
PermStream. It is designed for filesystem-style use (pack/unpack + FUSE),
with efficient streaming reads, per-chunk integrity, and forward-compatible
feature flags.

The goals are:
- Random access to any file chunk without scanning the full stream.
- Chunk-level integrity (CRC) and a top-level manifest hash.
- Clear versioning and feature flags for future extensions.
- Efficient I/O with fixed-size metadata tables.

All integers are little-endian unless noted otherwise.

## File Layout (v1)

| Section             | Offset source              | Notes                          |
|--------------------|----------------------------|--------------------------------|
| Superblock         | 0                          | Fixed-size header              |
| File index         | super.index_offset         | Fixed-size file entries        |
| String table       | super.strings_offset       | UTF-8 paths (len + bytes)      |
| Chunk table        | super.chunk_table_offset   | Fixed-size chunk entries       |
| Data region        | super.data_offset          | Chunk data payloads            |
| Manifest (optional)| super.manifest_offset      | Hashes + metadata (optional)   |

The file index, string table, and chunk table allow random access without
scanning the data region. The data region can be written sequentially during
pack and read by offsets during unpack/FUSE.

## Superblock

```
struct Superblock {
  char     magic[4];          // "PSFS"
  uint16   version;           // 1
  uint16   header_size;       // bytes, fixed at 136
  uint32   flags;             // feature flags
  uint32   chunk_size;        // default chunk size (bytes)
  uint16   block_size;        // codec block size (bytes)
  uint8    codec_id;          // 0=permstream, 1=raw
  uint8    hash_id;           // 0=none, 1=crc32
  uint32   file_count;
  uint64   chunk_count;
  uint64   index_offset;
  uint64   strings_offset;
  uint64   chunk_table_offset;
  uint64   data_offset;
  uint64   manifest_offset;   // 0 if unused
  uint32   codec_flags;       // codec behavior flags
  uint32   seed;              // seed for deterministic state
  float    weights[4];        // predictor weights (if header mode)
  uint8    transform_id;      // default transform id
  uint8    reserved[39];      // future use, must be zero
};
```

### Superblock flags (uint32)
- bit 0: has_manifest
- bit 1: has_dedup_index (future)
- bit 2: encrypted (future)
- bit 3: has_dir_index (future)

### Hash ids
- 0: none
- 1: crc32 (per-chunk)
- 2: sha256 (manifest)

### Codec flags (uint32)
- bit 0: use_rank
- bit 1: predictor_header (weights used)

## File Index

Fixed-size entries (one per file) to make random access trivial.

```
struct FileEntry {
  uint32   file_id;
  uint16   mode;           // POSIX mode bits
  uint16   flags;          // file flags (symlink, etc.)
  uint32   uid;
  uint32   gid;
  uint64   mtime_ns;
  uint64   size;           // uncompressed size
  uint64   chunk_start;    // index into ChunkEntry table
  uint64   chunk_count;
  uint64   path_offset;    // offset into string table
  uint32   path_len;
  uint32   reserved;
};
```

### File flags
- bit 0: symlink (payload stores link target bytes; size is target length)
- bit 1: directory (size=0, chunk_count=0; no payload)

## String Table

UTF-8 path bytes with no null terminator. Paths are stored once per file.

```
struct StringTable {
  uint8 data[];
};
```

## Chunk Table

Fixed-size entries, one per chunk. Each entry points to chunk data and
describes how to decode it.

```
struct ChunkEntry {
  uint32   file_id;
  uint32   flags;        // chunk flags
  uint64   file_offset;  // offset in the file
  uint32   raw_size;     // bytes after decode
  uint32   stored_size;  // bytes in data region
  uint64   data_offset;  // absolute offset in container
  uint8    codec_id;     // 0=permstream, 1=raw
  uint8    transform_id; // 0=none, 1=delta, 2=xor, 3=evenodd
  uint16   reserved;
  uint32   crc32;        // of decoded chunk, if enabled
};
```

### Chunk flags
- bit 0: stored raw (no codec)
- bit 1: checksum present
- bit 2: last chunk in file

## Data Region

Chunk payloads stored back-to-back. `data_offset` in ChunkEntry points to each
payload. Payload format is codec-specific, but must be fully self-contained
per chunk to allow random access.

## Codec Semantics

For filesystem use, each chunk must be independently decodable:
- Model resets per chunk, or
- Model snapshots stored per chunk in the payload.

The v1 container assumes model reset per chunk for simplicity.

### Permstream chunk payload (codec_id=0)
When a chunk is stored with the Permstream codec, its payload is:

```
uint16 ent_q         // entropy * 100
u16+bytes ranks[]    // if use_rank: [u16 len][big-endian bytes] per rank
uint32 enc_len       // encoded byte length
uint8  enc_data[]    // arithmetic-coded bytes
```

If `PSFS_CHUNK_FLAG_RAW` is set, the payload is raw bytes with no header.

## Manifest (optional)

If present, contains a hash of the file index + chunk table + string table,
and optional per-file hashes.

```
struct Manifest {
  uint8  hash_id;        // 2 = SHA-256
  uint8  reserved[7];
  uint8  table_hash[32]; // SHA-256 of index+strings+chunk table
  // Optional: per-file hashes follow
};
```

## Journal Sidecar (PSFJ, optional)

Write support is provided via an append-only journal sidecar file. The base
PSFS container remains immutable; the journal overlays new or updated paths
at mount time. This enables crash-safe updates and supports compaction by
repacking the base container plus journal.

### PSFJ Header
```
struct JournalHeader {
  char   magic[4];     // "PSFJ"
  uint16 version;      // 1
  uint16 header_size;  // bytes, fixed at 12
  uint32 flags;        // reserved
};
```

### PSFJ Record
Records are appended sequentially until EOF.
```
struct JournalRecord {
  uint8  type;        // 1=file, 2=symlink, 3=dir, 4=delete, 5=extent, 6=commit
  uint8  flags;       // bit0=compressed, bit1=recursive (delete)
  uint16 path_len;    // UTF-8 length
  uint32 mode;        // POSIX mode bits
  uint32 uid;
  uint32 gid;
  uint64 mtime_ns;
  uint64 size;        // uncompressed size or link target length
  uint32 data_len;    // payload bytes that follow
  uint8  path[path_len];
  uint8  data[data_len];
};
```

Extent payloads use a fixed metadata struct:
```
struct JournalExtent {
  uint64 file_offset;
  uint64 blob_offset;
  uint64 stored_len;
};
```

Notes:
- File records may store raw bytes or a permstream-encoded payload (when
  `flags & 0x01`). The decoded size is `size`.
- Extent records store file data in a separate blob file; `size` is the raw
  length of the extent and `stored_len` is the blob payload length. If
  `flags & 0x01`, the blob payload is permstream-encoded.
- Commit records seal a file version after its extents are written. The
  record header carries the final metadata (`mode`, `uid`, `gid`, `mtime_ns`,
  and `size`). `data_len=0`.
- Symlink records store the link target bytes in `data`.
- Directory records have `size=0` and `data_len=0`.
- Delete records remove a path; if `flags & 0x02`, the delete is recursive.
- Versioned policy: records are resolved by `(mtime_ns, append_order)`,
  so the newest timestamp wins; ties break by append order.
- Deletes hide base entries and older journal entries unless a newer record
  supersedes them.

### PSFJ Blob File
Large file writes can be stored in a sidecar blob file to avoid huge in-memory
buffers. By default the blob path is derived from the journal path:
`updates.psfj` -> `updates.psfb` (fallback: `journal_path + ".psfb"`).

## Random Access Strategy

To read a range:
1) Locate FileEntry by path (string table + index scan or optional hash).
2) Compute chunk indices using file_offset and chunk_size.
3) Seek to chunk data offsets using ChunkEntry table.
4) Decode each chunk independently and copy the requested range.

## Forward Compatibility

Unknown flags must be ignored if safe. Unknown codec_id or transform_id
requires a hard failure. All reserved fields must be zero when written and
ignored when read.

## Versioning

Version increments only for breaking layout changes. Additive changes should
use flags and reserved fields.
