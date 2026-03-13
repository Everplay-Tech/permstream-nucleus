import sys
import math
import os
import io
import numpy as np
from flask import Flask, request, send_file, jsonify
import threading

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

class A3B Predictor:
    def __init__(self):
        self.weights = np.random.rand(4) * 0.1  # Features: entropy, prev_rank, chunk_size, bias
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

def braid_mix(data, state, theta):
    n = len(data)
    braided = np.frombuffer(data, dtype=np.uint8).copy()
    for i in range(n):
        j = (i + int((state + braided[i] + theta) % n)) % n
        braided[i], braided[j] = braided[j], braided[i]
        state = (state * 31 + braided[i]) % (2**32)
    return braided.tobytes(), state

def perm_to_rank(perm, dims):
    rank = 0
    n = math.prod(dims)
    remaining = list(range(n))
    flat_perm = np.array(perm).flatten()
    for i in range(n):
        idx = remaining.index(flat_perm[i])
        rank += idx * factorial(n - 1 - i)
        remaining.pop(idx)
    return rank

def rank_to_perm(rank, dims):
    n = math.prod(dims)
    remaining = list(range(n))
    perm = []
    for k in range(n - 1, -1, -1):
        fact = factorial(k)
        idx = rank // fact
        perm.append(remaining.pop(idx))
        rank %= fact
    return np.reshape(perm, dims)

def adaptive_model_update(model, symbol, entropy_factor):
    model[symbol] = model.get(symbol, 0) + 1
    total = sum(model.values())
    for s in model:
        model[s] = (model[s] / total) * (1 + entropy_factor / 8)

def arithmetic_encode(chunk, model, entropy_factor):
    low, high = 0.0, 1.0
    output = []
    for symbol in chunk:
        adaptive_model_update(model, symbol, entropy_factor)
        cum = 0.0
        total = sum(model.values())
        prob = model.get(symbol, 1 / (total + 1)) / total
        high_old = high
        high = low + (high - low) * (cum + prob)
        low = low + (high_old - low) * cum
        while True:
            if high <= 0.5:
                output.append(0)
                low, high = low * 2, high * 2
            elif low >= 0.5:
                output.append(1)
                low, high = low * 2 - 1, high * 2 - 1
            elif 0.25 <= low < 0.5 and high <= 0.75:
                output.append(0)
                low, high = (low - 0.25) * 2, (high - 0.25) * 2
            else:
                break
    return bytes(output), model

def arithmetic_decode(encoded, model, length, entropy_factor):
    decoded = []
    value = int.from_bytes(encoded, 'big') / (1 << (8 * len(encoded)))
    low, high = 0.0, 1.0
    for _ in range(length):
        scaled = (value - low) / (high - low)
        cum = 0.0
        total = sum(model.values())
        for symbol in sorted(model):
            p = model[symbol] / total
            if cum <= scaled < cum + p:
                decoded.append(symbol)
                adaptive_model_update(model, symbol, entropy_factor)
                high_old = high
                high = low + (high - low) * (cum + p)
                low = low + (high_old - low) * cum
                break
            cum += p
    return bytes(decoded)

def compress_stream(input_stream, output_stream, chunk_size=1024*1024):  # 1MB chunks for video
    model = {i: 1 for i in range(256)}
    state = 0
    a3b = A3B Predictor()
    prev_rank = 0
    total_original = 0
    total_compressed = 0
    while True:
        chunk = input_stream.read(chunk_size)
        if not chunk:
            break
        total_original += len(chunk)
        ent = entropy(chunk)
        features = np.array([ent, prev_rank % 100, len(chunk), 1])  # Features for AI
        theta = a3b.predict(features)
        braided_chunk, state = braid_mix(chunk, state, theta)
        dims = [8, 8] if ent > 4 else [16]  # Adaptive for video patterns
        rank = perm_to_rank(np.frombuffer(braided_chunk, dtype=np.uint8).reshape(dims), dims)
        encoded, model = arithmetic_encode(np.frombuffer(braided_chunk, dtype=np.uint8), model, ent)
        output_stream.write(len(encoded).to_bytes(4, 'big'))
        output_stream.write(encoded)
        output_stream.write(rank.to_bytes(8, 'big'))
        output_stream.write(int(ent * 100).to_bytes(2, 'big'))
        total_compressed += 4 + len(encoded) + 8 + 2
        a3b.update(ent, theta, features)
        prev_rank = rank
    savings = (1 - total_compressed / total_original) * 100 if total_original > 0 else 0
    print(f"Compression complete. Savings: {savings:.2f}% (freed bandwidth for AI)")

def decompress_stream(input_stream, output_stream, chunk_size=1024*1024):
    model = {i: 1 for i in range(256)}
    state = 0
    a3b = A3B Predictor()
    prev_rank = 0
    while True:
        enc_len_data = input_stream.read(4)
        if len(enc_len_data) < 4:
            break
        enc_len = int.from_bytes(enc_len_data, 'big')
        encoded = input_stream.read(enc_len)
        rank = int.from_bytes(input_stream.read(8), 'big')
        ent_int = int.from_bytes(input_stream.read(2), 'big')
        ent = ent_int / 100
        features = np.array([ent, prev_rank % 100, chunk_size, 1])
        theta = a3b.predict(features)
        dims = [8, 8] if ent > 4 else [16]
        perm = rank_to_perm(rank, dims)
        flat_perm = perm.flatten()
        chunk_length = len(flat_perm)
        decoded = arithmetic_decode(encoded, model, chunk_length, ent)
        unbraided = np.frombuffer(decoded, dtype=np.uint8).copy()
        n = len(unbraided)
        for i in range(n - 1, -1, -1):
            j = (i + int((state + unbraided[i] + theta) % n)) % n
            unbraided[i], unbraided[j] = unbraided[j], unbraided[i]
            state = (state * 31 + unbraided[i]) % (2**32)
        output_stream.write(unbraided.tobytes())
        a3b.update(ent, theta, features)
        prev_rank = rank
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
    return send_file(output_stream, mimetype='application/octet-stream', as_attachment=True, download_name='compressed.prm')

@app.route('/decompress', methods=['POST'])
def api_decompress():
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400
    file = request.files['file']
    input_stream = io.BytesIO(file.read())
    output_stream = io.BytesIO()
    decompress_stream(input_stream, output_stream)
    output_stream.seek(0)
    return send_file(output_stream, mimetype='application/octet-stream', as_attachment=True, download_name='decompressed')

def run_api():
    app.run(host='0.0.0.0', port=5000)

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
            threading.Thread(target=run_api).start()
            print("API running at http://localhost:5000")
        else:
            print("Usage: python permstream_pro.py [compress_file input output | decompress_file input output | api]")
    else:
        print("Run with command (e.g., 'api' for server)")

if __name__ == "__main__":
    main()
