import argparse
import csv
import io
import json
import os
import random
import struct
import sys
import time

from permstream_pro import CodecConfig, compress_stream
from psfs_journal import (
    PSFJ_FLAG_COMPRESSED,
    PSFJ_HEADER_FMT,
    PSFJ_HEADER_SIZE,
    PSFJ_MAGIC,
    PSFJ_TYPE_FILE,
    PSFJ_VERSION,
    append_record,
)


def parse_int_list(value, default):
    if not value:
        return list(default)
    items = []
    for token in value.split(","):
        token = token.strip()
        if token:
            items.append(int(token))
    return items


def init_journal(path, overwrite=False):
    if os.path.exists(path) and not overwrite:
        raise ValueError(f"journal exists: {path} (use --overwrite)")
    with open(path, "wb") as handle:
        handle.write(struct.pack(PSFJ_HEADER_FMT, PSFJ_MAGIC, PSFJ_VERSION, PSFJ_HEADER_SIZE, 0))


def build_payload(data, config, compress, min_size, min_gain):
    if not compress or len(data) < min_size:
        return data, 0, False
    encoded = io.BytesIO()
    compress_stream(io.BytesIO(data), encoded, config)
    payload = encoded.getvalue()
    if len(payload) + min_gain >= len(data):
        return data, 0, False
    return payload, PSFJ_FLAG_COMPRESSED, True


def generate_data(pattern, size, rng, repeat_bytes):
    if size <= 0:
        return b""
    if pattern == "zeros":
        return b"\x00" * size
    if pattern == "repeat":
        return (repeat_bytes * (size // len(repeat_bytes) + 1))[:size]
    return rng.randbytes(size)


def run_variant(
    path,
    record_count,
    size,
    pattern,
    compress,
    min_size,
    min_gain,
    sync_every,
    config,
    overwrite,
):
    init_journal(path, overwrite=overwrite)
    uid = os.getuid()
    gid = os.getgid()
    mode = 0o100644
    rng = random.Random(1337)
    repeat_bytes = b"ABCD"
    bytes_raw = 0
    bytes_stored = 0
    compressed_records = 0
    fsyncs = 0
    pending = 0

    start = time.perf_counter()
    with open(path, "ab") as handle:
        for idx in range(record_count):
            data = generate_data(pattern, size, rng, repeat_bytes)
            payload, flags, compressed = build_payload(
                data, config, compress, min_size, min_gain
            )
            record = append_record(
                f"file_{idx:08d}.bin",
                PSFJ_TYPE_FILE,
                flags,
                mode,
                uid,
                gid,
                time.time_ns(),
                len(data),
                payload,
            )
            handle.write(record)
            handle.flush()
            bytes_raw += len(data)
            bytes_stored += len(payload)
            if compressed:
                compressed_records += 1
            pending += 1
            if sync_every and pending >= sync_every:
                os.fsync(handle.fileno())
                pending = 0
                fsyncs += 1
        if pending and sync_every:
            os.fsync(handle.fileno())
            fsyncs += 1
    elapsed = time.perf_counter() - start
    rate = record_count / elapsed if elapsed else 0
    mbps = (bytes_raw / (1024 * 1024)) / elapsed if elapsed else 0
    ratio = (bytes_stored / bytes_raw) if bytes_raw else 0
    return {
        "records": record_count,
        "size": size,
        "pattern": pattern,
        "compress": compress,
        "min_size": min_size,
        "min_gain": min_gain,
        "sync_every": sync_every,
        "elapsed_s": elapsed,
        "records_per_s": rate,
        "mb_per_s": mbps,
        "bytes_raw": bytes_raw,
        "bytes_stored": bytes_stored,
        "ratio": ratio,
        "compressed_records": compressed_records,
        "fsyncs": fsyncs,
        "path": path,
    }


def format_result(result):
    return (
        f"records={result['records']} size={result['size']} pattern={result['pattern']} "
        f"compress={result['compress']} min_size={result['min_size']} min_gain={result['min_gain']} "
        f"sync_every={result['sync_every']} elapsed_s={result['elapsed_s']:.4f} "
        f"records_s={result['records_per_s']:.1f} mb_s={result['mb_per_s']:.2f} "
        f"raw={result['bytes_raw']} stored={result['bytes_stored']} "
        f"ratio={result['ratio']:.4f} compressed={result['compressed_records']} "
        f"fsyncs={result['fsyncs']}"
    )


def parse_args():
    parser = argparse.ArgumentParser(description="PSFJ journal benchmark")
    parser.add_argument("--output", default="journal_bench.psfj")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--keep", action="store_true")
    parser.add_argument("--records", type=int, default=1000)
    parser.add_argument("--size", type=int, default=4096)
    parser.add_argument("--sizes", help="Comma-separated payload sizes")
    parser.add_argument(
        "--pattern",
        choices=["zeros", "repeat", "random"],
        default="repeat",
    )
    parser.add_argument("--compress", action="store_true")
    parser.add_argument("--compress-min-size", type=int, default=4096)
    parser.add_argument("--compress-min-gain", type=int, default=128)
    parser.add_argument("--compress-min-gain-list", help="Comma-separated gain values")
    parser.add_argument("--sync-every", type=int, default=0)
    parser.add_argument("--sync-every-list", help="Comma-separated fsync cadence values")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--csv", action="store_true")
    parser.add_argument("--csv-path", help="Write CSV output to this path")
    parser.add_argument("--chunk-size", type=int, default=512 * 1024)
    parser.add_argument("--block-size", type=int, default=64)
    parser.add_argument("--predictor", choices=["seeded", "header"], default="seeded")
    parser.add_argument("--seed", type=int, default=1337)
    parser.add_argument("--no-rank", action="store_true")
    parser.add_argument("--entropy-skip", type=float, default=7.5)
    parser.add_argument("--transform", choices=["none", "delta", "xor", "evenodd"], default="none")
    return parser.parse_args()


def main():
    args = parse_args()
    if args.json and (args.csv or args.csv_path):
        raise SystemExit("--json cannot be combined with --csv/--csv-path")
    if args.csv_path:
        args.csv = True
    sizes = parse_int_list(args.sizes, [args.size])
    sync_list = parse_int_list(args.sync_every_list, [args.sync_every])
    gain_list = parse_int_list(args.compress_min_gain_list, [args.compress_min_gain])

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

    variants = []
    for size in sizes:
        for sync_every in sync_list:
            for min_gain in gain_list:
                variants.append((size, sync_every, min_gain))

    multiple = len(variants) > 1
    csv_writer = None
    csv_handle = None
    csv_fields = [
        "records",
        "size",
        "pattern",
        "compress",
        "min_size",
        "min_gain",
        "sync_every",
        "elapsed_s",
        "records_per_s",
        "mb_per_s",
        "bytes_raw",
        "bytes_stored",
        "ratio",
        "compressed_records",
        "fsyncs",
        "path",
    ]
    if args.csv:
        if args.csv_path:
            csv_handle = open(args.csv_path, "w", newline="", encoding="utf-8")
            csv_writer = csv.writer(csv_handle)
        else:
            csv_writer = csv.writer(sys.stdout)
        csv_writer.writerow(csv_fields)
    for size, sync_every, min_gain in variants:
        base, ext = os.path.splitext(args.output)
        out_path = args.output
        if multiple:
            suffix = f".s{size}.sync{sync_every}.gain{min_gain}"
            if not ext:
                ext = ".psfj"
            out_path = base + suffix + ext
        result = run_variant(
            out_path,
            args.records,
            size,
            args.pattern,
            args.compress,
            args.compress_min_size,
            min_gain,
            sync_every,
            config,
            overwrite=args.overwrite,
        )
        if args.json:
            print(json.dumps(result, sort_keys=True))
        elif args.csv:
            csv_writer.writerow([result[field] for field in csv_fields])
        else:
            print(format_result(result))
        if not args.keep:
            try:
                os.unlink(out_path)
            except OSError:
                pass
    if csv_handle:
        csv_handle.close()


if __name__ == "__main__":
    main()
