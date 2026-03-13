import argparse
import io
import os
import struct
import subprocess
import shutil
import binascii
import hashlib
import numpy as np
from dataclasses import dataclass

try:
    from flask import Flask, request, send_file, jsonify
    FLASK_AVAILABLE = True
except ImportError:
    Flask = None
    request = None
    send_file = None
    jsonify = None
    FLASK_AVAILABLE = False

if FLASK_AVAILABLE:
    app = Flask(__name__)
else:
    app = None

MAGIC = b"PSP1"
VERSION = 4
HEADER_FMT = ">4sBBIHI4f"
HEADER_SIZE = struct.calcsize(HEADER_FMT)

FLAG_USE_RANK = 1 << 0
FLAG_PREDICTOR_HEADER = 1 << 1
FLAG_TRANSFORM_SHIFT = 2
FLAG_TRANSFORM_MASK = 0b00011100
FLAG_FILE_RAW = 1 << 5
CHUNK_FLAG_RAW = 1 << 0

DEFAULT_CHUNK_SIZE = 512 * 1024
DEFAULT_BLOCK_SIZE = 64
DEFAULT_PREDICTOR_MODE = "seeded"
DEFAULT_SEED = 1337
DEFAULT_ENTROPY_SKIP = 7.5
DEFAULT_TRANSFORM = "none"


def entropy(data):
    if not data:
        return 0.0
    freq = np.bincount(np.frombuffer(data, dtype=np.uint8), minlength=256)
    freq = freq[freq > 0]
    p = freq / len(data)
    return float(-np.sum(p * np.log2(p)))


class A3BPredictor:
    def __init__(self, weights=None, learning_rate=0.01):
        if weights is None:
            weights = np.random.random(4) * 0.1
        self.weights = np.array(weights, dtype=np.float64)
        self.learning_rate = learning_rate
        self.history = []

    def predict(self, features):
        return float(np.dot(self.weights, features))

    def update(self, actual_entropy, predicted, features):
        error = actual_entropy - predicted
        self.weights -= self.learning_rate * error * features
        self.history.append(actual_entropy)
        if len(self.history) > 10:
            self.history.pop(0)


def xorshift32(x):
    x &= 0xFFFFFFFF
    x ^= (x << 13) & 0xFFFFFFFF
    x ^= (x >> 17) & 0xFFFFFFFF
    x ^= (x << 5) & 0xFFFFFFFF
    return x & 0xFFFFFFFF


def braid_permutation(n, state, theta):
    theta_int = int(theta * 1_000_000) & 0xFFFFFFFF
    seed = xorshift32((state ^ theta_int ^ n) & 0xFFFFFFFF)
    if seed == 0:
        seed = 1
    perm = np.arange(n, dtype=np.int32)
    x = seed
    for i in range(n - 1, 0, -1):
        x = xorshift32(x)
        j = x % (i + 1)
        perm[i], perm[j] = perm[j], perm[i]
    new_state = xorshift32(state ^ seed ^ n)
    return perm, new_state


def factorials(n):
    facts = [1] * (n + 1)
    for i in range(2, n + 1):
        facts[i] = facts[i - 1] * i
    return facts


def perm_to_rank(perm):
    n = len(perm)
    remaining = list(range(n))
    facts = factorials(n)
    rank = 0
    for i, val in enumerate(perm):
        idx = remaining.index(int(val))
        rank += idx * facts[n - 1 - i]
        remaining.pop(idx)
    return rank


def rank_to_perm(rank, n):
    remaining = list(range(n))
    facts = factorials(n)
    perm = [0] * n
    r = int(rank)
    for k in range(n - 1, -1, -1):
        fact = facts[k]
        idx = r // fact
        r %= fact
        perm[n - 1 - k] = remaining.pop(idx)
    return np.array(perm, dtype=np.int32)


def invert_permutation(perm):
    inv = np.empty_like(perm)
    inv[perm] = np.arange(len(perm), dtype=perm.dtype)
    return inv


def int_to_bytes(value):
    if value == 0:
        return b"\x00"
    length = (value.bit_length() + 7) // 8
    return value.to_bytes(length, "big")


def encode_rank(rank):
    data = int_to_bytes(rank)
    if len(data) > 0xFFFF:
        raise ValueError("Rank is too large to encode")
    return len(data).to_bytes(2, "big") + data


def decode_rank(stream):
    len_bytes = stream.read(2)
    if len(len_bytes) < 2:
        raise EOFError("Unexpected end of stream while reading rank length")
    length = int.from_bytes(len_bytes, "big")
    data = stream.read(length)
    if len(data) < length:
        raise EOFError("Unexpected end of stream while reading rank data")
    return int.from_bytes(data, "big")


def adaptive_model_update(model, symbol, entropy_factor):
    increment = 1 + int(entropy_factor)
    model[symbol] += increment
    if int(model.sum()) > (1 << 20):
        model = (model + 1) // 2
        model[model == 0] = 1
    return model


def arithmetic_encode(chunk, model, entropy_factor):
    low = 0
    high = 0xFFFFFFFF
    pending = 0
    output = []
    for symbol in chunk:
        range_ = high - low + 1
        cum = np.cumsum(model)
        total = int(cum[-1])
        sym_low = cum[symbol - 1] if symbol > 0 else 0
        sym_high = cum[symbol]
        high = low + (range_ * int(sym_high) // total) - 1
        low = low + (range_ * int(sym_low) // total)
        while True:
            if high < 0x80000000:
                output.append(0)
                while pending > 0:
                    output.append(1)
                    pending -= 1
                low <<= 1
                high = (high << 1) | 1
            elif low >= 0x80000000:
                output.append(1)
                while pending > 0:
                    output.append(0)
                    pending -= 1
                low = (low - 0x80000000) << 1
                high = ((high - 0x80000000) << 1) | 1
            elif 0x40000000 <= low < 0x80000000 and high < 0xC0000000:
                pending += 1
                low = (low - 0x40000000) << 1
                high = ((high - 0x40000000) << 1) | 1
            else:
                break
        model = adaptive_model_update(model, int(symbol), entropy_factor)
    output.append(1)
    while pending > 0:
        output.append(0)
        pending -= 1
    byte_output = np.packbits(output).tobytes()
    return byte_output, model


def arithmetic_decode(encoded, model, length, entropy_factor):
    input_bits = np.unpackbits(np.frombuffer(encoded, dtype=np.uint8)).tolist()
    if len(input_bits) < 32:
        input_bits.extend([0] * (32 - len(input_bits)))
    value = 0
    for bit in input_bits[:32]:
        value = (value << 1) | bit
    bit_index = 32
    low = 0
    high = 0xFFFFFFFF
    decoded = []
    for _ in range(length):
        range_ = high - low + 1
        cum = np.cumsum(model)
        total = int(cum[-1])
        scaled = ((value - low + 1) * total - 1) // range_
        symbol = int(np.searchsorted(cum, scaled + 1))
        if symbol >= len(model):
            symbol = len(model) - 1
        model = adaptive_model_update(model, symbol, entropy_factor)
        sym_low = cum[symbol - 1] if symbol > 0 else 0
        sym_high = cum[symbol]
        high = low + (int(sym_high) * range_ // total) - 1
        low = low + (int(sym_low) * range_ // total)
        while True:
            if high < 0x80000000:
                pass
            elif low >= 0x80000000:
                value -= 0x80000000
                low -= 0x80000000
                high -= 0x80000000
            elif 0x40000000 <= low < 0x80000000 and high < 0xC0000000:
                value -= 0x40000000
                low -= 0x40000000
                high -= 0x40000000
            else:
                break
            low = (low << 1) & 0xFFFFFFFF
            high = ((high << 1) | 1) & 0xFFFFFFFF
            if bit_index < len(input_bits):
                bit = input_bits[bit_index]
            else:
                bit = 0
            value = ((value << 1) | bit) & 0xFFFFFFFF
            bit_index += 1
        decoded.append(symbol)
    return bytes(decoded), model


@dataclass
class CodecConfig:
    chunk_size: int = DEFAULT_CHUNK_SIZE
    block_size: int = DEFAULT_BLOCK_SIZE
    use_rank: bool = True
    predictor_mode: str = DEFAULT_PREDICTOR_MODE
    seed: int = DEFAULT_SEED
    weights: np.ndarray = None
    entropy_skip: float = DEFAULT_ENTROPY_SKIP
    transform: str = DEFAULT_TRANSFORM
    force_raw: bool = False


def init_weights(predictor_mode, seed, weights):
    if predictor_mode == "header" and weights is not None:
        return np.array(weights, dtype=np.float64)
    rng = np.random.default_rng(seed)
    return rng.random(4) * 0.1


TRANSFORM_IDS = {
    "none": 0,
    "delta": 1,
    "xor": 2,
    "evenodd": 3,
}
TRANSFORM_NAMES = {v: k for k, v in TRANSFORM_IDS.items()}
VIDEO_EXTENSIONS = {".mp4", ".m4v", ".mov", ".mkv", ".avi"}
PROFILE_PRESETS = {
    "quality": ("slow", 20),
    "balanced": ("medium", 24),
    "small": ("veryfast", 28),
}

PSFS_MAGIC = b"PSFS"
PSFS_VERSION = 1
PSFS_SUPER_FMT = "<4sHHIIHBBIQQQQQQII4fB39s"
PSFS_SUPER_SIZE = struct.calcsize(PSFS_SUPER_FMT)
PSFS_FILE_FMT = "<I H H I I Q Q Q Q I I"
PSFS_FILE_SIZE = struct.calcsize(PSFS_FILE_FMT)
PSFS_CHUNK_FMT = "<I I Q I I Q B B H I"
PSFS_CHUNK_SIZE = struct.calcsize(PSFS_CHUNK_FMT)

PSFS_HASH_NONE = 0
PSFS_HASH_CRC32 = 1
PSFS_HASH_SHA256 = 2
PSFS_CODEC_RAW = 1
PSFS_CODEC_PERMSTREAM = 0

PSFS_CHUNK_FLAG_RAW = 1 << 0
PSFS_CHUNK_FLAG_CRC = 1 << 1
PSFS_FILE_FLAG_SYMLINK = 1 << 0
PSFS_FILE_FLAG_DIR = 1 << 1
PSFS_FLAG_HAS_MANIFEST = 1 << 0
PSFS_MANIFEST_FMT = "<B7s32s"
PSFS_MANIFEST_SIZE = struct.calcsize(PSFS_MANIFEST_FMT)


def apply_transform(arr, transform_id):
    if transform_id == TRANSFORM_IDS["delta"]:
        out = np.empty_like(arr)
        if len(arr) > 0:
            out[0] = arr[0]
            if len(arr) > 1:
                out[1:] = (arr[1:].astype(np.int16) - arr[:-1].astype(np.int16)) & 0xFF
        return out
    if transform_id == TRANSFORM_IDS["xor"]:
        out = np.empty_like(arr)
        if len(arr) > 0:
            out[0] = arr[0]
            if len(arr) > 1:
                out[1:] = np.bitwise_xor(arr[1:], arr[:-1])
        return out
    if transform_id == TRANSFORM_IDS["evenodd"]:
        if len(arr) <= 1:
            return arr.copy()
        return np.concatenate([arr[0::2], arr[1::2]])
    return arr.copy()


def invert_transform(arr, transform_id):
    if transform_id == TRANSFORM_IDS["delta"]:
        if len(arr) == 0:
            return arr.copy()
        out = np.cumsum(arr.astype(np.uint16)) & 0xFF
        return out.astype(np.uint8)
    if transform_id == TRANSFORM_IDS["xor"]:
        if len(arr) == 0:
            return arr.copy()
        out = np.bitwise_xor.accumulate(arr)
        return out.astype(np.uint8)
    if transform_id == TRANSFORM_IDS["evenodd"]:
        n = len(arr)
        if n <= 1:
            return arr.copy()
        even_count = (n + 1) // 2
        evens = arr[:even_count]
        odds = arr[even_count:]
        out = np.empty_like(arr)
        out[0::2] = evens
        out[1::2] = odds
        return out
    return arr.copy()


def is_video_path(path):
    _, ext = os.path.splitext(path.lower())
    return ext in VIDEO_EXTENSIONS


def get_media_duration_seconds(path):
    if shutil.which("ffprobe") is None:
        raise RuntimeError("ffprobe is not installed or not in PATH")
    result = subprocess.run(
        [
            "ffprobe",
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            path,
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return float(result.stdout.strip())


def chunk_state(seed, file_id, chunk_index):
    value = (seed ^ (file_id * 0x9E3779B1) ^ chunk_index) & 0xFFFFFFFF
    return xorshift32(value)


def compress_chunk_payload(data, config, file_id, chunk_index):
    ent = entropy(data)
    ent_q = int(ent * 100)
    if ent_q > 0xFFFF:
        ent_q = 0xFFFF
    ent = ent_q / 100.0
    model = np.ones(256, dtype=np.int64)
    a3b = A3BPredictor(weights=init_weights(config.predictor_mode, config.seed, config.weights))
    prev_rank = 0
    features = np.array([ent, prev_rank % 100, len(data), 1.0], dtype=np.float64)
    theta = a3b.predict(features)
    state = chunk_state(config.seed, file_id, chunk_index)

    if ent >= config.entropy_skip:
        return data, PSFS_CODEC_RAW, PSFS_CHUNK_FLAG_RAW

    chunk_arr = np.frombuffer(data, dtype=np.uint8)
    transform_id = TRANSFORM_IDS.get(config.transform, TRANSFORM_IDS["none"])
    transformed_arr = apply_transform(chunk_arr, transform_id)
    permuted_arr, ranks, _, _ = permute_chunk(
        transformed_arr, config.block_size, state, theta, config.use_rank
    )
    encoded, _ = arithmetic_encode(permuted_arr, model, ent)

    payload = io.BytesIO()
    payload.write(ent_q.to_bytes(2, "big"))
    if config.use_rank:
        for rank in ranks:
            payload.write(encode_rank(rank))
    payload.write(len(encoded).to_bytes(4, "big"))
    payload.write(encoded)
    payload_bytes = payload.getvalue()

    if len(payload_bytes) >= len(data):
        return data, PSFS_CODEC_RAW, PSFS_CHUNK_FLAG_RAW

    return payload_bytes, PSFS_CODEC_PERMSTREAM, 0


def decompress_chunk_payload(payload, raw_size, config, file_id, chunk_index, codec_id, transform_id):
    if codec_id == PSFS_CODEC_RAW:
        return payload

    stream = io.BytesIO(payload)
    ent_bytes = stream.read(2)
    if len(ent_bytes) < 2:
        raise ValueError("Invalid chunk payload (entropy)")
    ent = int.from_bytes(ent_bytes, "big") / 100.0

    model = np.ones(256, dtype=np.int64)
    a3b = A3BPredictor(weights=init_weights(config.predictor_mode, config.seed, config.weights))
    prev_rank = 0
    features = np.array([ent, prev_rank % 100, raw_size, 1.0], dtype=np.float64)
    theta = a3b.predict(features)
    state = chunk_state(config.seed, file_id, chunk_index)

    ranks = []
    if config.use_rank:
        block_count = (raw_size + config.block_size - 1) // config.block_size
        for _ in range(block_count):
            ranks.append(decode_rank(stream))

    enc_len_bytes = stream.read(4)
    if len(enc_len_bytes) < 4:
        raise ValueError("Invalid chunk payload (encoded length)")
    enc_len = int.from_bytes(enc_len_bytes, "big")
    encoded = stream.read(enc_len)
    if len(encoded) < enc_len:
        raise ValueError("Invalid chunk payload (encoded data)")

    decoded, _ = arithmetic_decode(encoded, model, raw_size, ent)
    permuted_arr = np.frombuffer(decoded, dtype=np.uint8)
    restored_arr, _, _ = unpermute_chunk(
        permuted_arr, config.block_size, ranks, state, theta, config.use_rank
    )
    final_arr = invert_transform(restored_arr, transform_id)
    return final_arr.tobytes()


def pack_psfs(input_dir, output_path, config, verify=False):
    if not os.path.isdir(input_dir):
        raise ValueError(f"Input must be a directory: {input_dir}")
    entries = []
    for root, dirs, files in os.walk(input_dir):
        for name in dirs:
            full_path = os.path.join(root, name)
            rel_path = os.path.relpath(full_path, input_dir)
            if os.path.islink(full_path):
                entries.append(("symlink", rel_path, full_path))
            else:
                entries.append(("dir", rel_path, full_path))
        for name in files:
            full_path = os.path.join(root, name)
            rel_path = os.path.relpath(full_path, input_dir)
            if os.path.islink(full_path):
                entries.append(("symlink", rel_path, full_path))
            else:
                entries.append(("file", rel_path, full_path))
    entries.sort(key=lambda item: item[1])

    file_entries = []
    entry_meta = []
    string_table = bytearray()

    chunk_size = config.chunk_size
    block_size = config.block_size
    total_chunks = 0

    for file_id, (kind, rel_path, full_path) in enumerate(entries):
        stat = os.lstat(full_path)
        flags = 0
        target_bytes = None
        size = 0
        if kind == "dir":
            flags |= PSFS_FILE_FLAG_DIR
        elif kind == "symlink":
            flags |= PSFS_FILE_FLAG_SYMLINK
            target_bytes = os.fsencode(os.readlink(full_path))
            size = len(target_bytes)
        else:
            size = stat.st_size
        path_offset = len(string_table)
        path_bytes = rel_path.encode("utf-8")
        string_table.extend(path_bytes)
        chunk_count = (size + chunk_size - 1) // chunk_size if size else 0
        chunk_start = total_chunks
        total_chunks += chunk_count

        file_entries.append(
            {
                "file_id": file_id,
                "mode": stat.st_mode & 0xFFFF,
                "flags": flags,
                "uid": stat.st_uid,
                "gid": stat.st_gid,
                "mtime_ns": int(stat.st_mtime_ns),
                "size": size,
                "chunk_start": chunk_start,
                "chunk_count": chunk_count,
                "path_offset": path_offset,
                "path_len": len(path_bytes),
            }
        )
        entry_meta.append(
            {
                "file_id": file_id,
                "full_path": full_path,
                "flags": flags,
                "size": size,
                "chunk_start": chunk_start,
                "chunk_count": chunk_count,
                "target_bytes": target_bytes,
            }
        )

    index_offset = PSFS_SUPER_SIZE
    strings_offset = index_offset + PSFS_FILE_SIZE * len(file_entries)
    chunk_table_offset = strings_offset + len(string_table)
    data_offset = chunk_table_offset + PSFS_CHUNK_SIZE * total_chunks

    file_index_bytes = bytearray()
    for entry in file_entries:
        file_index_bytes.extend(
            struct.pack(
                PSFS_FILE_FMT,
                entry["file_id"],
                entry["mode"],
                entry["flags"],
                entry["uid"],
                entry["gid"],
                entry["mtime_ns"],
                entry["size"],
                entry["chunk_start"],
                entry["chunk_count"],
                entry["path_offset"],
                entry["path_len"],
            )
        )

    with open(output_path, "wb") as out:
        codec_flags = 0
        if config.use_rank:
            codec_flags |= 1 << 0
        if config.predictor_mode == "header":
            codec_flags |= 1 << 1
        weights = config.weights
        if weights is None:
            weights = np.zeros(4, dtype=np.float32)
        if config.predictor_mode == "header":
            weights = init_weights(config.predictor_mode, config.seed, weights)
        config.weights = np.array(weights, dtype=np.float64)
        transform_id = TRANSFORM_IDS.get(config.transform, TRANSFORM_IDS["none"])
        superblock = struct.pack(
            PSFS_SUPER_FMT,
            PSFS_MAGIC,
            PSFS_VERSION,
            PSFS_SUPER_SIZE,
            0,
            chunk_size,
            block_size,
            PSFS_CODEC_PERMSTREAM,
            PSFS_HASH_CRC32,
            len(file_entries),
            total_chunks,
            index_offset,
            strings_offset,
            chunk_table_offset,
            data_offset,
            0,
            codec_flags,
            config.seed & 0xFFFFFFFF,
            float(weights[0]),
            float(weights[1]),
            float(weights[2]),
            float(weights[3]),
            transform_id,
            bytes(39),
        )
        out.write(superblock)

        out.write(file_index_bytes)
        out.write(string_table)

        chunk_table_pos = out.tell()
        out.write(b"\x00" * (PSFS_CHUNK_SIZE * total_chunks))

        chunk_entries = []
        for entry in entry_meta:
            if entry["flags"] & PSFS_FILE_FLAG_DIR:
                continue
            handle = None
            try:
                if entry["flags"] & PSFS_FILE_FLAG_SYMLINK:
                    handle = io.BytesIO(entry["target_bytes"] or b"")
                else:
                    handle = open(entry["full_path"], "rb")
                chunk_index = 0
                offset = 0
                while True:
                    data = handle.read(chunk_size)
                    if not data:
                        break
                    payload, codec_id, flags = compress_chunk_payload(
                        data, config, entry["file_id"], chunk_index
                    )
                    raw_crc = binascii.crc32(data) & 0xFFFFFFFF
                    stored_size = len(payload)
                    if flags & PSFS_CHUNK_FLAG_RAW:
                        codec_id = PSFS_CODEC_RAW
                    flags |= PSFS_CHUNK_FLAG_CRC
                    data_offset = out.tell()
                    out.write(payload)
                    chunk_entries.append(
                        {
                            "file_id": entry["file_id"],
                            "flags": flags,
                            "file_offset": offset,
                            "raw_size": len(data),
                            "stored_size": stored_size,
                            "data_offset": data_offset,
                            "codec_id": codec_id,
                            "transform_id": transform_id,
                            "crc32": raw_crc,
                        }
                    )
                    offset += len(data)
                    chunk_index += 1
            finally:
                if handle is not None and hasattr(handle, "close"):
                    handle.close()

        chunk_table_bytes = bytearray()
        for entry in chunk_entries:
            chunk_table_bytes.extend(
                struct.pack(
                    PSFS_CHUNK_FMT,
                    entry["file_id"],
                    entry["flags"],
                    entry["file_offset"],
                    entry["raw_size"],
                    entry["stored_size"],
                    entry["data_offset"],
                    entry["codec_id"],
                    entry["transform_id"],
                    0,
                    entry["crc32"],
                )
            )
        out.seek(chunk_table_pos)
        out.write(chunk_table_bytes)

        table_hash = hashlib.sha256(
            file_index_bytes + string_table + chunk_table_bytes
        ).digest()
        manifest_offset = out.seek(0, os.SEEK_END)
        out.write(struct.pack(PSFS_MANIFEST_FMT, PSFS_HASH_SHA256, bytes(7), table_hash))

        superblock = struct.pack(
            PSFS_SUPER_FMT,
            PSFS_MAGIC,
            PSFS_VERSION,
            PSFS_SUPER_SIZE,
            PSFS_FLAG_HAS_MANIFEST,
            chunk_size,
            block_size,
            PSFS_CODEC_PERMSTREAM,
            PSFS_HASH_CRC32,
            len(file_entries),
            total_chunks,
            index_offset,
            strings_offset,
            chunk_table_offset,
            data_offset,
            manifest_offset,
            codec_flags,
            config.seed & 0xFFFFFFFF,
            float(weights[0]),
            float(weights[1]),
            float(weights[2]),
            float(weights[3]),
            transform_id,
            bytes(39),
        )
        out.seek(0)
        out.write(superblock)

    if verify:
        verify_psfs(output_path)


def verify_psfs(container_path):
    with open(container_path, "rb") as handle:
        data = handle.read(PSFS_SUPER_SIZE)
        if len(data) < PSFS_SUPER_SIZE:
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
        ) = struct.unpack(PSFS_SUPER_FMT, data)
        if magic != PSFS_MAGIC:
            raise ValueError("Invalid PSFS magic")
        if version != PSFS_VERSION:
            raise ValueError("Unsupported PSFS version")
        predictor_mode = "header" if (codec_flags & (1 << 1)) else "seeded"
        use_rank = bool(codec_flags & (1 << 0))
        config = CodecConfig(
            chunk_size=chunk_size,
            block_size=block_size,
            use_rank=use_rank,
            predictor_mode=predictor_mode,
            seed=seed,
            weights=np.array([w0, w1, w2, w3], dtype=np.float64),
        )
        if flags & PSFS_FLAG_HAS_MANIFEST:
            handle.seek(index_offset)
            file_index_bytes = handle.read(strings_offset - index_offset)
            handle.seek(strings_offset)
            string_table = handle.read(chunk_table_offset - strings_offset)
            handle.seek(chunk_table_offset)
            chunk_table_bytes = handle.read(chunk_count * PSFS_CHUNK_SIZE)
            table_hash = hashlib.sha256(
                file_index_bytes + string_table + chunk_table_bytes
            ).digest()
            handle.seek(manifest_offset)
            manifest = handle.read(PSFS_MANIFEST_SIZE)
            if len(manifest) < PSFS_MANIFEST_SIZE:
                raise ValueError("Missing manifest")
            hash_id, _, stored_hash = struct.unpack(PSFS_MANIFEST_FMT, manifest)
            if hash_id != PSFS_HASH_SHA256:
                raise ValueError("Unsupported manifest hash")
            if stored_hash != table_hash:
                raise ValueError("Manifest hash mismatch")
        handle.seek(chunk_table_offset)
        chunk_indices = {}
        for _ in range(chunk_count):
            chunk_data = handle.read(PSFS_CHUNK_SIZE)
            if len(chunk_data) < PSFS_CHUNK_SIZE:
                raise ValueError("Unexpected EOF in chunk table")
            (
                file_id,
                chunk_flags,
                file_offset,
                raw_size,
                stored_size,
                data_off,
                entry_codec_id,
                entry_transform_id,
                _,
                crc32,
            ) = struct.unpack(PSFS_CHUNK_FMT, chunk_data)
            if not (chunk_flags & PSFS_CHUNK_FLAG_CRC):
                continue
            handle_pos = handle.tell()
            handle.seek(data_off)
            payload = handle.read(stored_size)
            handle.seek(handle_pos)
            chunk_index = chunk_indices.get(file_id, 0)
            chunk_indices[file_id] = chunk_index + 1
            decoded = decompress_chunk_payload(
                payload,
                raw_size,
                config,
                file_id,
                chunk_index,
                entry_codec_id,
                entry_transform_id,
            )
            if len(decoded) != raw_size:
                raise ValueError("Chunk size mismatch during verify")
            check = binascii.crc32(decoded) & 0xFFFFFFFF
            if check != crc32:
                raise ValueError("CRC mismatch during verify")


def unpack_psfs(container_path, output_dir, verify=False):
    with open(container_path, "rb") as handle:
        data = handle.read(PSFS_SUPER_SIZE)
        if len(data) < PSFS_SUPER_SIZE:
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
        ) = struct.unpack(PSFS_SUPER_FMT, data)
        if magic != PSFS_MAGIC:
            raise ValueError("Invalid PSFS magic")
        if version != PSFS_VERSION:
            raise ValueError("Unsupported PSFS version")
        predictor_mode = "header" if (codec_flags & (1 << 1)) else "seeded"
        use_rank = bool(codec_flags & (1 << 0))
        config = CodecConfig(
            chunk_size=chunk_size,
            block_size=block_size,
            use_rank=use_rank,
            predictor_mode=predictor_mode,
            seed=seed,
            weights=np.array([w0, w1, w2, w3], dtype=np.float64),
        )

        handle.seek(index_offset)
        file_entries = []
        for _ in range(file_count):
            entry_data = handle.read(PSFS_FILE_SIZE)
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
                entry_chunk_start,
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
                    "chunk_start": entry_chunk_start,
                    "chunk_count": entry_chunk_count,
                    "path_offset": path_offset,
                    "path_len": path_len,
                }
            )

        handle.seek(strings_offset)
        string_table = handle.read(chunk_table_offset - strings_offset)

        handle.seek(chunk_table_offset)
        chunk_entries = []
        for _ in range(chunk_count):
            chunk_data = handle.read(PSFS_CHUNK_SIZE)
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
                entry_transform_id,
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
                    "transform_id": entry_transform_id,
                    "crc32": crc32,
                }
            )

        os.makedirs(output_dir, exist_ok=True)

        def decode_entry_data(entry):
            data_parts = []
            chunk_index = 0
            for chunk in chunk_entries[
                entry["chunk_start"] : entry["chunk_start"] + entry["chunk_count"]
            ]:
                handle.seek(chunk["data_offset"])
                payload = handle.read(chunk["stored_size"])
                decoded = decompress_chunk_payload(
                    payload,
                    chunk["raw_size"],
                    config,
                    chunk["file_id"],
                    chunk_index,
                    chunk["codec_id"],
                    chunk["transform_id"],
                )
                if verify and (chunk["flags"] & PSFS_CHUNK_FLAG_CRC):
                    check = binascii.crc32(decoded) & 0xFFFFFFFF
                    if check != chunk["crc32"]:
                        raise ValueError(f"CRC mismatch for {rel_path}")
                data_parts.append(decoded)
                chunk_index += 1
            return b"".join(data_parts)

        def apply_metadata(path, entry, follow_symlinks=True):
            mode = entry["mode"] & 0o7777
            try:
                os.chmod(path, mode, follow_symlinks=follow_symlinks)
            except (OSError, TypeError):
                pass
            try:
                os.utime(
                    path,
                    ns=(entry["mtime_ns"], entry["mtime_ns"]),
                    follow_symlinks=follow_symlinks,
                )
            except (OSError, TypeError):
                pass

        for entry in file_entries:
            path_bytes = string_table[
                entry["path_offset"] : entry["path_offset"] + entry["path_len"]
            ]
            rel_path = path_bytes.decode("utf-8")
            out_path = os.path.join(output_dir, rel_path)
            if entry["flags"] & PSFS_FILE_FLAG_DIR:
                os.makedirs(out_path, exist_ok=True)
                apply_metadata(out_path, entry)
                continue
            os.makedirs(os.path.dirname(out_path), exist_ok=True)
            if entry["flags"] & PSFS_FILE_FLAG_SYMLINK:
                data = decode_entry_data(entry)
                target = os.fsdecode(data)
                os.symlink(target, out_path)
                apply_metadata(out_path, entry, follow_symlinks=False)
                continue
            with open(out_path, "wb") as out:
                chunk_index = 0
                for chunk in chunk_entries[
                    entry["chunk_start"] : entry["chunk_start"] + entry["chunk_count"]
                ]:
                    handle.seek(chunk["data_offset"])
                    payload = handle.read(chunk["stored_size"])
                    decoded = decompress_chunk_payload(
                        payload,
                        chunk["raw_size"],
                        config,
                        chunk["file_id"],
                        chunk_index,
                        chunk["codec_id"],
                        chunk["transform_id"],
                    )
                    if verify and (chunk["flags"] & PSFS_CHUNK_FLAG_CRC):
                        check = binascii.crc32(decoded) & 0xFFFFFFFF
                        if check != chunk["crc32"]:
                            raise ValueError(f"CRC mismatch for {rel_path}")
                    out.write(decoded)
                    chunk_index += 1
            apply_metadata(out_path, entry)


def write_header(output_stream, config):
    flags = 0
    if config.use_rank:
        flags |= FLAG_USE_RANK
    if config.predictor_mode == "header":
        flags |= FLAG_PREDICTOR_HEADER
    transform_id = TRANSFORM_IDS.get(config.transform, TRANSFORM_IDS["none"])
    flags |= (transform_id << FLAG_TRANSFORM_SHIFT) & FLAG_TRANSFORM_MASK
    if config.force_raw:
        flags |= FLAG_FILE_RAW
    seed = config.seed & 0xFFFFFFFF
    weights = config.weights
    if weights is None:
        weights = np.zeros(4, dtype=np.float32)
    output_stream.write(
        struct.pack(
            HEADER_FMT,
            MAGIC,
            VERSION,
            flags,
            int(config.chunk_size),
            int(config.block_size),
            seed,
            float(weights[0]),
            float(weights[1]),
            float(weights[2]),
            float(weights[3]),
        )
    )


def read_header(input_stream):
    data = input_stream.read(HEADER_SIZE)
    if len(data) < HEADER_SIZE:
        raise EOFError("Missing header")
    magic, version, flags, chunk_size, block_size, seed, w0, w1, w2, w3 = struct.unpack(
        HEADER_FMT, data
    )
    if magic != MAGIC:
        raise ValueError("Invalid magic header")
    if version != VERSION:
        raise ValueError("Unsupported version")
    predictor_mode = "header" if (flags & FLAG_PREDICTOR_HEADER) else "seeded"
    use_rank = bool(flags & FLAG_USE_RANK)
    transform_id = (flags & FLAG_TRANSFORM_MASK) >> FLAG_TRANSFORM_SHIFT
    transform = TRANSFORM_NAMES.get(transform_id, "none")
    force_raw = bool(flags & FLAG_FILE_RAW)
    weights = np.array([w0, w1, w2, w3], dtype=np.float64)
    return CodecConfig(
        chunk_size=chunk_size,
        block_size=block_size,
        use_rank=use_rank,
        predictor_mode=predictor_mode,
        seed=seed,
        weights=weights,
        transform=transform,
        force_raw=force_raw,
    )


def permute_chunk(chunk_arr, block_size, state, theta, use_rank):
    chunk_len = len(chunk_arr)
    permuted = np.empty_like(chunk_arr)
    ranks = []
    offset = 0
    prev_rank = 0
    while offset < chunk_len:
        block_len = min(block_size, chunk_len - offset)
        perm, state = braid_permutation(block_len, state, theta)
        rank = perm_to_rank(perm)
        if use_rank:
            ranks.append(rank)
        permuted[offset : offset + block_len] = chunk_arr[offset : offset + block_len][perm]
        prev_rank = rank
        offset += block_len
    return permuted, ranks, state, prev_rank


def unpermute_chunk(permuted_arr, block_size, ranks, state, theta, use_rank):
    chunk_len = len(permuted_arr)
    restored = np.empty_like(permuted_arr)
    offset = 0
    prev_rank = 0
    rank_index = 0
    while offset < chunk_len:
        block_len = min(block_size, chunk_len - offset)
        if use_rank:
            rank = ranks[rank_index]
            rank_index += 1
            perm = rank_to_perm(rank, block_len)
        else:
            perm, state = braid_permutation(block_len, state, theta)
            rank = perm_to_rank(perm)
        inv = invert_permutation(perm)
        restored[offset : offset + block_len] = permuted_arr[offset : offset + block_len][inv]
        prev_rank = rank
        offset += block_len
    return restored, state, prev_rank


def compress_stream(input_stream, output_stream, config):
    config.weights = init_weights(config.predictor_mode, config.seed, config.weights)
    write_header(output_stream, config)
    if config.force_raw:
        total_original = 0
        total_compressed = HEADER_SIZE
        while True:
            chunk = input_stream.read(config.chunk_size)
            if not chunk:
                break
            total_original += len(chunk)
            output_stream.write(chunk)
            total_compressed += len(chunk)
        savings = (1 - total_compressed / total_original) * 100 if total_original > 0 else 0
        print(f"Compression complete. Savings: {savings:.2f}% (raw passthrough)")
        return savings
    chunk_index = 0
    total_original = 0
    total_compressed = HEADER_SIZE
    while True:
        chunk = input_stream.read(config.chunk_size)
        if not chunk:
            break
        model = np.ones(256, dtype=np.int64)
        a3b = A3BPredictor(weights=config.weights.copy())
        prev_rank = 0
        state = chunk_state(config.seed, 0, chunk_index)
        total_original += len(chunk)
        ent = entropy(chunk)
        ent_q = int(ent * 100)
        if ent_q > 0xFFFF:
            ent_q = 0xFFFF
        ent = ent_q / 100.0
        features = np.array([ent, prev_rank % 100, len(chunk), 1.0], dtype=np.float64)
        theta = a3b.predict(features)
        output_stream.write(len(chunk).to_bytes(4, "big"))

        flags = 0
        if ent >= config.entropy_skip:
            flags |= CHUNK_FLAG_RAW

        encoded = None
        ranks = []
        next_prev_rank = prev_rank
        next_model = model
        if not (flags & CHUNK_FLAG_RAW):
            chunk_arr = np.frombuffer(chunk, dtype=np.uint8)
            transform_id = TRANSFORM_IDS.get(config.transform, TRANSFORM_IDS["none"])
            transformed_arr = apply_transform(chunk_arr, transform_id)
            permuted_arr, ranks, _, next_prev_rank = permute_chunk(
                transformed_arr, config.block_size, state, theta, config.use_rank
            )
            encoded, next_model = arithmetic_encode(permuted_arr, next_model, ent)
            raw_size = 1 + 2 + len(chunk)
            rank_size = 0
            if config.use_rank:
                rank_size = sum(2 + len(int_to_bytes(r)) for r in ranks)
            encoded_size = 1 + 2 + rank_size + 4 + len(encoded)
            if encoded_size >= raw_size:
                flags |= CHUNK_FLAG_RAW
                encoded = None

        output_stream.write(flags.to_bytes(1, "big"))
        output_stream.write(ent_q.to_bytes(2, "big"))
        total_compressed += 4 + 1 + 2
        if flags & CHUNK_FLAG_RAW:
            output_stream.write(chunk)
            total_compressed += len(chunk)
        else:
            if config.use_rank:
                for rank in ranks:
                    output_stream.write(encode_rank(rank))
                total_compressed += sum(2 + len(int_to_bytes(r)) for r in ranks)
            output_stream.write(len(encoded).to_bytes(4, "big"))
            output_stream.write(encoded)
            total_compressed += 4 + len(encoded)
        a3b.update(ent, theta, features)
        chunk_index += 1
    savings = (1 - total_compressed / total_original) * 100 if total_original > 0 else 0
    print(f"Compression complete. Savings: {savings:.2f}% (experimental)")
    return savings


def decompress_stream(input_stream, output_stream):
    config = read_header(input_stream)
    if config.force_raw:
        while True:
            chunk = input_stream.read(config.chunk_size)
            if not chunk:
                break
            output_stream.write(chunk)
        print("Decompression complete (raw passthrough).")
        return
    chunk_index = 0
    while True:
        chunk_len_bytes = input_stream.read(4)
        if len(chunk_len_bytes) < 4:
            break
        chunk_len = int.from_bytes(chunk_len_bytes, "big")
        model = np.ones(256, dtype=np.int64)
        weights = init_weights(config.predictor_mode, config.seed, config.weights)
        a3b = A3BPredictor(weights=weights)
        prev_rank = 0
        state = chunk_state(config.seed, 0, chunk_index)
        flag_bytes = input_stream.read(1)
        if len(flag_bytes) < 1:
            raise EOFError("Unexpected end of stream while reading chunk flags")
        flags = int.from_bytes(flag_bytes, "big")
        ent_bytes = input_stream.read(2)
        if len(ent_bytes) < 2:
            raise EOFError("Unexpected end of stream while reading entropy")
        ent = int.from_bytes(ent_bytes, "big") / 100.0

        features = np.array([ent, prev_rank % 100, chunk_len, 1.0], dtype=np.float64)
        theta = a3b.predict(features)

        if flags & CHUNK_FLAG_RAW:
            raw = input_stream.read(chunk_len)
            if len(raw) < chunk_len:
                raise EOFError("Unexpected end of stream while reading raw chunk")
            output_stream.write(raw)
            a3b.update(ent, theta, features)
            continue

        ranks = []
        if config.use_rank:
            block_count = (chunk_len + config.block_size - 1) // config.block_size
            for _ in range(block_count):
                ranks.append(decode_rank(input_stream))
        encoded_len_bytes = input_stream.read(4)
        if len(encoded_len_bytes) < 4:
            raise EOFError("Unexpected end of stream while reading encoded length")
        encoded_len = int.from_bytes(encoded_len_bytes, "big")
        encoded = input_stream.read(encoded_len)
        if len(encoded) < encoded_len:
            raise EOFError("Unexpected end of stream while reading encoded data")
        decoded, model = arithmetic_decode(encoded, model, chunk_len, ent)
        permuted_arr = np.frombuffer(decoded, dtype=np.uint8)
        restored_arr, _, prev_rank = unpermute_chunk(
            permuted_arr, config.block_size, ranks, state, theta, config.use_rank
        )
        transform_id = TRANSFORM_IDS.get(config.transform, TRANSFORM_IDS["none"])
        final_arr = invert_transform(restored_arr, transform_id)
        output_stream.write(final_arr.tobytes())
        a3b.update(ent, theta, features)
        chunk_index += 1
    print("Decompression complete.")


if FLASK_AVAILABLE:
    @app.route("/compress", methods=["POST"])
    def api_compress():
        if "file" not in request.files:
            return jsonify({"error": "No file provided"}), 400
        file = request.files["file"]
        input_stream = io.BytesIO(file.read())
        output_stream = io.BytesIO()
        config = app.config.get("CODEC_CONFIG", CodecConfig())
        if file.filename and file.filename.lower().endswith(".mp4"):
            config = CodecConfig(**config.__dict__)
            config.force_raw = True
        compress_stream(input_stream, output_stream, config)
        output_stream.seek(0)
        return send_file(
            output_stream,
            mimetype="application/octet-stream",
            download_name="compressed.prm",
        )


    @app.route("/decompress", methods=["POST"])
    def api_decompress():
        if "file" not in request.files:
            return jsonify({"error": "No file provided"}), 400
        file = request.files["file"]
        input_stream = io.BytesIO(file.read())
        output_stream = io.BytesIO()
        decompress_stream(input_stream, output_stream)
        output_stream.seek(0)
        return send_file(
            output_stream,
            mimetype="application/octet-stream",
            download_name="decompressed",
        )
else:
    def api_compress():  # pragma: no cover - only used when Flask is missing
        raise RuntimeError("Flask is not installed; install requirements.txt to use the API")

    def api_decompress():  # pragma: no cover - only used when Flask is missing
        raise RuntimeError("Flask is not installed; install requirements.txt to use the API")


def run_api(single_mode=False, single_type=None, config=None, port=5150):
    if not FLASK_AVAILABLE:
        raise RuntimeError("Flask is not installed; install requirements.txt to use the API")
    if config is None:
        config = CodecConfig()
    app.config["CODEC_CONFIG"] = config
    if single_mode:
        from werkzeug.serving import make_server
        server = make_server("0.0.0.0", port, app)
        if single_type == "compress":
            app.add_url_rule("/compress", "api_compress", api_compress, methods=["POST"])
        elif single_type == "decompress":
            app.add_url_rule("/decompress", "api_decompress", api_decompress, methods=["POST"])
        print(
            f"Single-mode server running for one {single_type} request at http://localhost:{port}"
        )
        server.handle_request()
        print("Request handled. Exiting.")
        server.server_close()
    else:
        app.run(host="0.0.0.0", port=port)


def run_selftest(size, config):
    data = os.urandom(size)
    encoded = io.BytesIO()
    compress_stream(io.BytesIO(data), encoded, config)
    encoded.seek(0)
    decoded = io.BytesIO()
    decompress_stream(encoded, decoded)
    result = decoded.getvalue()
    if result == data:
        print(f"Selftest OK ({size} bytes)")
        return True
    print(f"Selftest FAIL ({size} bytes, got {len(result)} bytes)")
    return False


def transcode_video(input_path, output_path, crf, preset, codec, audio, bitrate_kbps=None):
    if shutil.which("ffmpeg") is None:
        raise RuntimeError("ffmpeg is not installed or not in PATH")
    cmd = [
        "ffmpeg",
        "-y",
        "-i",
        input_path,
        "-c:v",
        codec,
    ]
    if bitrate_kbps is not None:
        cmd.extend(
            [
                "-b:v",
                f"{bitrate_kbps}k",
                "-maxrate",
                f"{bitrate_kbps}k",
                "-bufsize",
                f"{bitrate_kbps * 2}k",
            ]
        )
    else:
        cmd.extend(["-preset", preset, "-crf", str(crf)])
    if audio == "copy":
        cmd.extend(["-c:a", "copy"])
    elif audio:
        cmd.extend(["-c:a", audio])
    else:
        cmd.extend(["-an"])
    cmd.append(output_path)
    subprocess.run(cmd, check=True)


def parse_args():
    parser = argparse.ArgumentParser(description="PermStream Pro experimental codec")
    sub = parser.add_subparsers(dest="cmd", required=True)

    compress = sub.add_parser("compress_file", help="Compress a file")
    compress.add_argument("input")
    compress.add_argument("output")
    compress.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE)
    compress.add_argument("--block-size", type=int, default=DEFAULT_BLOCK_SIZE)
    compress.add_argument(
        "--predictor",
        choices=["seeded", "header"],
        default=DEFAULT_PREDICTOR_MODE,
    )
    compress.add_argument("--seed", type=int, default=DEFAULT_SEED)
    compress.add_argument("--no-rank", action="store_true")
    compress.add_argument("--entropy-skip", type=float, default=DEFAULT_ENTROPY_SKIP)
    compress.add_argument(
        "--transform",
        choices=sorted(TRANSFORM_IDS.keys()),
        default=DEFAULT_TRANSFORM,
    )
    compress.add_argument("--force-raw", action="store_true")

    decompress = sub.add_parser("decompress_file", help="Decompress a file")
    decompress.add_argument("input")
    decompress.add_argument("output")

    api = sub.add_parser("api", help="Run the Flask API")
    api.add_argument("--port", type=int, default=5150)
    api.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE)
    api.add_argument("--block-size", type=int, default=DEFAULT_BLOCK_SIZE)
    api.add_argument(
        "--predictor",
        choices=["seeded", "header"],
        default=DEFAULT_PREDICTOR_MODE,
    )
    api.add_argument("--seed", type=int, default=DEFAULT_SEED)
    api.add_argument("--no-rank", action="store_true")
    api.add_argument("--entropy-skip", type=float, default=DEFAULT_ENTROPY_SKIP)
    api.add_argument(
        "--transform",
        choices=sorted(TRANSFORM_IDS.keys()),
        default=DEFAULT_TRANSFORM,
    )
    api.add_argument("--force-raw", action="store_true")

    single = sub.add_parser("single", help="Run single-request API mode")
    single.add_argument("single_type", choices=["compress", "decompress"])
    single.add_argument("--port", type=int, default=5150)
    single.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE)
    single.add_argument("--block-size", type=int, default=DEFAULT_BLOCK_SIZE)
    single.add_argument(
        "--predictor",
        choices=["seeded", "header"],
        default=DEFAULT_PREDICTOR_MODE,
    )
    single.add_argument("--seed", type=int, default=DEFAULT_SEED)
    single.add_argument("--no-rank", action="store_true")
    single.add_argument("--entropy-skip", type=float, default=DEFAULT_ENTROPY_SKIP)
    single.add_argument(
        "--transform",
        choices=sorted(TRANSFORM_IDS.keys()),
        default=DEFAULT_TRANSFORM,
    )
    single.add_argument("--force-raw", action="store_true")

    selftest = sub.add_parser("selftest", help="Run an in-memory round-trip test")
    selftest.add_argument("--size", type=int, default=4096)
    selftest.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE)
    selftest.add_argument("--block-size", type=int, default=DEFAULT_BLOCK_SIZE)
    selftest.add_argument(
        "--predictor",
        choices=["seeded", "header"],
        default=DEFAULT_PREDICTOR_MODE,
    )
    selftest.add_argument("--seed", type=int, default=DEFAULT_SEED)
    selftest.add_argument("--no-rank", action="store_true")
    selftest.add_argument("--entropy-skip", type=float, default=DEFAULT_ENTROPY_SKIP)
    selftest.add_argument(
        "--transform",
        choices=sorted(TRANSFORM_IDS.keys()),
        default=DEFAULT_TRANSFORM,
    )
    selftest.add_argument("--force-raw", action="store_true")

    transcode = sub.add_parser("transcode", help="Lossy re-encode video using ffmpeg")
    transcode.add_argument("input")
    transcode.add_argument("output")
    transcode.add_argument("--codec", default="libx265")
    transcode.add_argument(
        "--profile",
        choices=sorted(PROFILE_PRESETS.keys()),
        default="balanced",
    )
    transcode.add_argument("--preset")
    transcode.add_argument("--crf", type=int)
    transcode.add_argument("--bitrate", type=int, help="Target video bitrate in kbps")
    transcode.add_argument("--target-size-mb", type=float, help="Approx target size in MB")
    transcode.add_argument("--audio", choices=["copy", "aac", "none"], default="copy")

    best = sub.add_parser("best", help="Auto-choose raw vs lossy based on input type")
    best.add_argument("input")
    best.add_argument("output")
    best.add_argument("--codec", default="libx265")
    best.add_argument(
        "--profile",
        choices=sorted(PROFILE_PRESETS.keys()),
        default="balanced",
    )
    best.add_argument("--preset")
    best.add_argument("--crf", type=int)
    best.add_argument("--bitrate", type=int, help="Target video bitrate in kbps")
    best.add_argument("--target-size-mb", type=float, help="Approx target size in MB")
    best.add_argument("--audio", choices=["copy", "aac", "none"], default="copy")
    best.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE)
    best.add_argument("--block-size", type=int, default=DEFAULT_BLOCK_SIZE)
    best.add_argument(
        "--predictor",
        choices=["seeded", "header"],
        default=DEFAULT_PREDICTOR_MODE,
    )
    best.add_argument("--seed", type=int, default=DEFAULT_SEED)
    best.add_argument("--no-rank", action="store_true")
    best.add_argument("--entropy-skip", type=float, default=DEFAULT_ENTROPY_SKIP)
    best.add_argument(
        "--transform",
        choices=sorted(TRANSFORM_IDS.keys()),
        default=DEFAULT_TRANSFORM,
    )

    pack = sub.add_parser("pack", help="Pack a directory into a PSFS container")
    pack.add_argument("input_dir")
    pack.add_argument("output")
    pack.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE)
    pack.add_argument("--block-size", type=int, default=DEFAULT_BLOCK_SIZE)
    pack.add_argument(
        "--predictor",
        choices=["seeded", "header"],
        default=DEFAULT_PREDICTOR_MODE,
    )
    pack.add_argument("--seed", type=int, default=DEFAULT_SEED)
    pack.add_argument("--no-rank", action="store_true")
    pack.add_argument("--entropy-skip", type=float, default=DEFAULT_ENTROPY_SKIP)
    pack.add_argument(
        "--transform",
        choices=sorted(TRANSFORM_IDS.keys()),
        default=DEFAULT_TRANSFORM,
    )
    pack.add_argument("--verify", action="store_true")

    unpack = sub.add_parser("unpack", help="Unpack a PSFS container to a directory")
    unpack.add_argument("input")
    unpack.add_argument("output_dir")
    unpack.add_argument("--verify", action="store_true")

    verify = sub.add_parser("verify", help="Verify a PSFS container")
    verify.add_argument("input")

    return parser.parse_args()


def main():
    args = parse_args()
    if args.cmd == "compress_file":
        force_raw = args.force_raw or args.input.lower().endswith(".mp4")
        config = CodecConfig(
            chunk_size=args.chunk_size,
            block_size=args.block_size,
            use_rank=not args.no_rank,
            predictor_mode=args.predictor,
            seed=args.seed,
            entropy_skip=args.entropy_skip,
            transform=args.transform,
            force_raw=force_raw,
        )
        with open(args.input, "rb") as inp, open(args.output, "wb") as out:
            compress_stream(inp, out, config)
    elif args.cmd == "decompress_file":
        with open(args.input, "rb") as inp, open(args.output, "wb") as out:
            decompress_stream(inp, out)
    elif args.cmd == "api":
        config = CodecConfig(
            chunk_size=args.chunk_size,
            block_size=args.block_size,
            use_rank=not args.no_rank,
            predictor_mode=args.predictor,
            seed=args.seed,
            entropy_skip=args.entropy_skip,
            transform=args.transform,
            force_raw=args.force_raw,
        )
        run_api(single_mode=False, config=config, port=args.port)
    elif args.cmd == "single":
        config = CodecConfig(
            chunk_size=args.chunk_size,
            block_size=args.block_size,
            use_rank=not args.no_rank,
            predictor_mode=args.predictor,
            seed=args.seed,
            entropy_skip=args.entropy_skip,
            transform=args.transform,
            force_raw=args.force_raw,
        )
        run_api(single_mode=True, single_type=args.single_type, config=config, port=args.port)
    elif args.cmd == "selftest":
        config = CodecConfig(
            chunk_size=args.chunk_size,
            block_size=args.block_size,
            use_rank=not args.no_rank,
            predictor_mode=args.predictor,
            seed=args.seed,
            entropy_skip=args.entropy_skip,
            transform=args.transform,
            force_raw=args.force_raw,
        )
        run_selftest(args.size, config)
    elif args.cmd == "transcode":
        audio = None if args.audio == "none" else args.audio
        if args.bitrate and args.target_size_mb:
            raise ValueError("Use either --bitrate or --target-size-mb, not both")
        preset = args.preset
        crf = args.crf
        if preset is None or crf is None:
            profile_preset, profile_crf = PROFILE_PRESETS[args.profile]
            if preset is None:
                preset = profile_preset
            if crf is None:
                crf = profile_crf
        bitrate_kbps = args.bitrate
        if args.target_size_mb:
            duration = get_media_duration_seconds(args.input)
            bitrate_kbps = int((args.target_size_mb * 8192) / max(duration, 0.1))
        transcode_video(
            args.input,
            args.output,
            crf=crf,
            preset=preset,
            codec=args.codec,
            audio=audio,
            bitrate_kbps=bitrate_kbps,
        )
    elif args.cmd == "best":
        audio = None if args.audio == "none" else args.audio
        if is_video_path(args.input):
            if args.bitrate and args.target_size_mb:
                raise ValueError("Use either --bitrate or --target-size-mb, not both")
            preset = args.preset
            crf = args.crf
            if preset is None or crf is None:
                profile_preset, profile_crf = PROFILE_PRESETS[args.profile]
                if preset is None:
                    preset = profile_preset
                if crf is None:
                    crf = profile_crf
            bitrate_kbps = args.bitrate
            if args.target_size_mb:
                duration = get_media_duration_seconds(args.input)
                bitrate_kbps = int((args.target_size_mb * 8192) / max(duration, 0.1))
            transcode_video(
                args.input,
                args.output,
                crf=crf,
                preset=preset,
                codec=args.codec,
                audio=audio,
                bitrate_kbps=bitrate_kbps,
            )
        else:
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
            with open(args.input, "rb") as inp, open(args.output, "wb") as out:
                compress_stream(inp, out, config)
    elif args.cmd == "pack":
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


if __name__ == "__main__":
    main()
