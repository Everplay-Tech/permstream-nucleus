import sys
import math
import os
import io
import numpy as np
from flask import Flask, request, send_file, jsonify
import threading
import multiprocessing as mp
from collections import defaultdict
import time

app = Flask(__name__)

def factorial(n):
    if n <= 1:
        return 1
    res = 1
    for i in range(2, n + 1):
        res *= i
    return res

def multivariate_factorial(dims):
    return math.prod(factorial(d) for d in dims)

def entropy(data):
    if not data:
        return 0
    freq = np.bincount(np.frombuffer(data, dtype=np.uint8), minlength=256)
    freq = freq[freq > 0]
    p = freq / len(data)
    return -np.sum(p * np.log2(p))

class A3BPredictor:
    def __init__(self):
        self.weights = np.random.rand(4) * 0.1
        self.learning_rate = 0.01
        self.history = []

    def predict(self, features):
        return np.dot(self.weights, features)

    def update(self, actual_entropy, predicted, features):
        error = actual_entropy - predicted
        self.weights -= self.learning_rate * error * features
        self.history.append(actual_entropy)
        if len(self.history) > 10:
            self.history.pop(0)

def vectorized_braid_mix(data, state, theta):
    n = len(data)
    braided = np.frombuffer(data, dtype=np.uint8).copy()
    indices = np.arange(n)
    swaps = (indices + (state + braided + int(theta)) % n) % n
    braided = braided[swaps]
    state = (state * 31 + np.sum(braided) % (2**32)) % (2**32)
    return braided.tobytes(), state

def perm_to_rank(perm, dims):
    n = math.prod(dims)
    remaining = np.arange(n)
    flat_perm = np.array(perm).flatten()
    rank = 0
    for i in range(n):
        idx = np.where(remaining == flat_perm[i])[0][0]
        rank += idx * factorial(n - 1 - i)
        remaining = np.delete(remaining, idx)
    return rank

def rank_to_perm(rank, dims):
    n = math.prod(dims)
    remaining = np.arange(n)
    perm = np.zeros(n, dtype=int)
    for k in range(n - 1, -1, -1):
        fact = factorial(k)
        idx = rank // fact
        perm[n - 1 - k] = remaining[idx]
        remaining = np.delete(remaining, idx)
        rank %= fact
    return np.reshape(perm, dims)

def adaptive_model_update(model, symbol, entropy_factor):
    model[symbol] += 1
    total = np.sum(model)
    model = (model / total) * (1 + entropy_factor / 8)
    return model

def arithmetic_encode(chunk, model, entropy_factor, cache):
    chunk_tuple = tuple(chunk)
    if chunk_tuple in cache:
        return cache[chunk_tuple], model
    low = 0
    high = 0xffffffff
    pending = 0
    output = []
    model = np.array([model.get(i, 1) for i in range(256)], dtype=float)
    for symbol in chunk:
        model = adaptive_model_update(model, symbol, entropy_factor)
        range_ = high - low + 1
        cum = np.cumsum(model)
        sym_low = cum[symbol - 1] if symbol > 0 else 0
        sym_high = cum[symbol]
        high = low + int(sym_high * range_ / np.sum(model)) - 1
        low = low + int(sym_low * range_ / np.sum(model))
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
            elif 0x40000000 <= low < 0x80000000 and high < 0xc0000000:
                pending += 1
                low = (low - 0x40000000) << 1
                high = ((high - 0x40000000) << 1) | 1
            else:
                break
    output.append(1)
    while pending > 0:
        output.append(0)
        pending -= 1
    byte_output = np.packbits(output).tobytes()
    cache[chunk_tuple] = byte_output
    return byte_output, dict(enumerate(model))

def arithmetic_decode(encoded, model, length, entropy_factor, cache):
    encoded_tuple = tuple(encoded)
    if encoded_tuple in cache:
        return cache[encoded_tuple], model
    input_bits = np.unpackbits(np.frombuffer(encoded, dtype=np.uint8)).tolist()
    low = 0
    high = 0xffffffff
    value = int(''.join(map(str, input_bits[:32])), 2)
    bit_index = 32
    decoded = []
    model = np.array([model.get(i, 1) for i in range(256)], dtype=float)
    for _ in range(length):
        range_ = high - low + 1
        total = np.sum(model)
        scaled = ((value - low + 1) * total - 1) // range_
        cum = np.cumsum(model)
        symbol = np.searchsorted(cum, scaled + 1)
        model = adaptive_model_update(model, symbol, entropy_factor)
        sym_low = cum[symbol - 1] if symbol > 0 else 0
        sym_high = cum[symbol]
        high = low + (int(sym_high) * range_ // int(total)) - 1
        low = low + (int(sym_low) * range_ // int(total))
        while True:
            if high < 0x80000000:
                pass
            elif low >= 0x80000000:
                value -= 0x80000000
                low -= 0x80000000
                high -= 0x80000000
            elif 0x40000000 <= low < 0x80000000 and high < 0xc0000000:
                value -= 0x40000000
                low -= 0x40000000
                high -= 0x40000000
            else:
                break
            low = (low << 1) & 0xffffffff
            high = ((high << 1) | 1) & 0xffffffff
            value = ((value << 1) | input_bits[bit_index]) & 0xffffffff
            bit_index += 1
        decoded.append(symbol)
    decoded_bytes = bytes(decoded)
    cache[encoded_tuple] = decoded_bytes
    return decoded_bytes, dict(enumerate(model))

def process_chunk(args):
    chunk, state, ent, features, theta, dims, model, cache = args
    theta = A3BPredictor().predict(features)
    braided_chunk, new_state = vectorized_braid_mix(chunk, state, theta)
    rank = perm_to_rank(np.frombuffer(braided_chunk, dtype=np.uint8).reshape(dims), dims)
    encoded, new_model = arithmetic_encode(np.frombuffer(braided_chunk, dtype=np.uint8), model, ent, cache)
    return encoded, rank, ent, new_state, new_model

def compress_stream(input_stream, output_stream, chunk_size=512*1024):
    model = {i: 1 for i in range(256)}
    state = 0
    a3b = A3BPredictor()
    prev_rank = 0
    total_original = 0
    total_compressed = 0
    cache = defaultdict(bytes)
    pool = mp.Pool(8)
    chunks = []
    while True:
        chunk = input_stream.read(chunk_size)
        if not chunk:
            break
        total_original += len(chunk)
        ent = entropy(chunk)
        features = np.array([ent, prev_rank % 100, len(chunk), 1.0])
        dims = [8, 8] if ent > 4 else [16]
        chunks.append((chunk, state, ent, features, a3b.predict(features), dims, model, cache))
    results = pool.map(process_chunk, chunks)
    for encoded, rank, ent, new_state, new_model in results:
        output_stream.write(len(encoded).to_bytes(4, 'big'))
        output_stream.write(encoded)
        output_stream.write(rank.to_bytes(8, 'big'))
        output_stream.write(int(ent * 100).to_bytes(2, 'big'))
        total_compressed += 4 + len(encoded) + 8 + 2
        state = new_state
        model = new_model
        a3b.update(ent, a3b.predict(features), features)
        prev_rank = rank
    pool.close()
    savings = (1 - total_compressed / total_original) * 100 if total_original > 0 else 0
    print(f"Compression complete. Savings: {savings:.2f}% (freed bandwidth for AI)")
    return savings

def process_decode_chunk(args):
    encoded, rank, ent, state, theta, dims, model, cache = args
    decoded = arithmetic_decode(encoded, model, dims[0] * dims[1] if len(dims) == 2 else dims[0], ent, cache)
    unbraided = np.frombuffer(decoded, dtype=np.uint8).copy()
    n = len(unbraided)
    for i in range(n - 1, -1, -1):
        j = (i + int((state + unbraided[i] + theta) % n)) % n
        unbraided[i], unbraided[j] = unbraided[j], unbraided[i]
        state = (state * 31 + unbraided[i]) % (2**32)
    return unbraided.tobytes(), state

def decompress_stream(input_stream, output_stream, chunk_size=512*1024):
    model = {i: 1 for i in range(256)}
    state = 0
    a3b = A3BPredictor()
    prev_rank = 0
    cache = defaultdict(bytes)
    pool = mp.Pool(8)
    chunks = []
    while True:
        enc_len_data = input_stream.read(4)
        if len(enc_len_data) < 4:
            break
        enc_len = int.from_bytes(enc_len_data, 'big')
        encoded = input_stream.read(enc_len)
        rank = int.from_bytes(input_stream.read(8), 'big')
        ent_int = int.from_bytes(input_stream.read(2), 'big')
        ent = ent_int / 100
        features = np.array([ent, prev_rank % 100, chunk_size, 1.0])
        theta = a3b.predict(features)
        dims = [8, 8] if ent > 4 else [16]
        chunks.append((encoded, rank, ent, state, theta, dims, model, cache))
    results = pool.map(process_decode_chunk, chunks)
    for decoded, new_state in results:
        output_stream.write(decoded)
        state = new_state
        a3b.update(ent, theta, features)
        prev_rank = rank
    pool.close()
    print("Decompression complete.")

@app.route('/compress', methods=['POST'])
def api_compress():
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400
    file = request.files['file']
    input_stream = io.BytesIO(file.read())
    output_stream = io.BytesIO()
    compress_stream(input_stream, output_stream)
    output_stream.seek(0)
    return send_file(output_stream, mimetype='application/octet-stream', download_name='compressed.prm')

@app.route('/decompress', methods=['POST'])
def api_decompress():
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400
    file = request.files['file']
    input_stream = io.BytesIO(file.read())
    output_stream = io.BytesIO()
    decompress_stream(input_stream, output_stream)
    output_stream.seek(0)
    return send_file(output_stream, mimetype='application/octet-stream', download_name='decompressed')

def run_api(single_mode=False, single_type=None):
    if single_mode:
        from werkzeug.serving import make_server
        server = make_server('0.0.0.0', 5150, app)
        if single_type == 'compress':
            app.add_url_rule('/compress', 'api_compress', api_compress, methods=['POST'])
        elif single_type == 'decompress':
            app.add_url_rule('/decompress', 'api_decompress', api_decompress, methods=['POST'])
        print(f"Single-mode server running for one {single_type} request at http://localhost:5150")
        server.handle_request()
        print("Request handled. Exiting.")
        server.server_close()
    else:
        app.run(host='0.0.0.0', port=5150)

def main():
    if len(sys.argv) > 1:
        cmd = sys.argv[1]
        if cmd == 'compress_file' and len(sys.argv) == 4:
            with open(sys.argv[2], 'rb') as inp, open(sys.argv[3], 'wb') as out:
                compress_stream(inp, out)
        elif cmd == 'decompress_file' and len(sys.argv) == 4:
            with open(sys.argv[2], 'rb') as inp, open(sys.argv[3], 'wb') as out:
                decompress_stream(inp, out)
        elif cmd == 'api':
            run_api(single_mode=False)
        elif cmd == 'single' and len(sys.argv) == 3:
            single_type = sys.argv[2]
            if single_type in ['compress', 'decompress']:
                run_api(single_mode=True, single_type=single_type)
            else:
                print("Invalid single type: use 'compress' or 'decompress'")
        else:
            print("Usage: python permstream_pro.py [compress_file input output | decompress_file input output | api | single compress | single decompress]")
    else:
        print("Run with command (e.g., 'api' for server, 'single compress' for one-off)")

if __name__ == "__main__":
    main()
