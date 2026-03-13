import argparse
import io
import os
import time

from permstream_pro import CodecConfig, compress_stream, decompress_stream


def load_inputs(paths):
    if paths:
        samples = []
        for path in paths:
            with open(path, "rb") as handle:
                data = handle.read()
            samples.append((path, data))
        return samples

    samples = []
    readme = "README.md"
    if os.path.exists(readme):
        with open(readme, "rb") as handle:
            samples.append((readme, handle.read()))
    samples.append(("zeros-64k", b"\x00" * 65536))
    samples.append(("pattern-64k", (b"ABCD" * 16384)))
    samples.append(("random-64k", os.urandom(65536)))
    return samples


def run_variant(samples, config):
    results = []
    for name, data in samples:
        comp_stream = io.BytesIO()
        start = time.perf_counter()
        compress_stream(io.BytesIO(data), comp_stream, config)
        comp_time = time.perf_counter() - start
        comp_size = comp_stream.tell()

        comp_stream.seek(0)
        decomp_stream = io.BytesIO()
        start = time.perf_counter()
        decompress_stream(comp_stream, decomp_stream)
        decomp_time = time.perf_counter() - start

        restored = decomp_stream.getvalue()
        ok = restored == data
        ratio = comp_size / len(data) if data else 0
        results.append(
            {
                "name": name,
                "ok": ok,
                "orig": len(data),
                "comp": comp_size,
                "ratio": ratio,
                "comp_time": comp_time,
                "decomp_time": decomp_time,
            }
        )
    return results


def format_results(variant, results):
    lines = [f"== {variant} =="]
    for item in results:
        status = "OK" if item["ok"] else "FAIL"
        lines.append(
            f"{item['name']}: {status} orig={item['orig']} comp={item['comp']} "
            f"ratio={item['ratio']:.3f} comp_s={item['comp_time']:.4f} "
            f"decomp_s={item['decomp_time']:.4f}"
        )
    return "\n".join(lines)


def parse_args():
    parser = argparse.ArgumentParser(description="PermStream Pro variant benchmark")
    parser.add_argument("--inputs", nargs="*", help="Input files to benchmark")
    parser.add_argument("--chunk-size", type=int, default=512 * 1024)
    parser.add_argument("--chunk-sizes", help="Comma-separated chunk sizes for sweep")
    parser.add_argument("--block-size", type=int, default=64)
    parser.add_argument("--entropy-skip", type=float, default=7.5)
    parser.add_argument("--seed", type=int, default=1337)
    parser.add_argument("--predictor", choices=["seeded", "header"], default="seeded")
    parser.add_argument("--force-raw", action="store_true")
    parser.add_argument("--sweep-chunk", action="store_true")
    parser.add_argument(
        "--variant",
        choices=[
            "rank+none",
            "rank+delta",
            "rank+xor",
            "rank+evenodd",
            "norank+none",
            "norank+delta",
            "norank+xor",
            "norank+evenodd",
        ],
        default="rank+none",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    samples = load_inputs(args.inputs)
    variants = {
        "rank+none": (True, "none"),
        "rank+delta": (True, "delta"),
        "rank+xor": (True, "xor"),
        "rank+evenodd": (True, "evenodd"),
        "norank+none": (False, "none"),
        "norank+delta": (False, "delta"),
        "norank+xor": (False, "xor"),
        "norank+evenodd": (False, "evenodd"),
    }

    if args.sweep_chunk:
        sizes = []
        if args.chunk_sizes:
            for token in args.chunk_sizes.split(","):
                token = token.strip()
                if token:
                    sizes.append(int(token))
        if not sizes:
            sizes = [64 * 1024, 128 * 1024, 256 * 1024, 512 * 1024, 1024 * 1024]
        use_rank, transform = variants[args.variant]
        for size in sizes:
            config = CodecConfig(
                chunk_size=size,
                block_size=args.block_size,
                use_rank=use_rank,
                predictor_mode=args.predictor,
                seed=args.seed,
                entropy_skip=args.entropy_skip,
                transform=transform,
                force_raw=args.force_raw,
            )
            results = run_variant(samples, config)
            print(format_results(f"{args.variant} chunk={size}", results))
            print()
        return

    for name, (use_rank, transform) in variants.items():
        config = CodecConfig(
            chunk_size=args.chunk_size,
            block_size=args.block_size,
            use_rank=use_rank,
            predictor_mode=args.predictor,
            seed=args.seed,
            entropy_skip=args.entropy_skip,
            transform=transform,
            force_raw=args.force_raw,
        )
        results = run_variant(samples, config)
        print(format_results(name, results))
        print()


if __name__ == "__main__":
    main()
