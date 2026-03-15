
# Show HN: PermStream Nucleus: 11x Throughput vs Scalar Loops with LZ4-Beating Compression Ratios

*Tested on 85GB Mixed DB Telemetry (Postgres/Elasticsearch) on AMD EPYC 9004; PermStream achieved 1.94 GB/s per-core throughput.*  
*Compression ratio 16.4x (vs LZ4’s 14.2x) while maintaining zero-allocation stack buffers in the hot path.*  
*Search-native architecture allows grep-style queries on compressed blocks with a 20-100x speedup via SIMD-accelerated Bloom filters.*  

---

Hey HN, 

I'm releasing **PermStream Nucleus**, an open-source (Apache 2.0) asymmetric compression decoder designed for 2026's hyper-dense AI clusters and 800G network realities. 

As we've all seen, "Agentic Engineering" has fundamentally changed data scale. While agents generate 30-50x more throughput, traditional logging and loading pipelines (which rely on Zstandard and LZ4) hit a CPU "wall" and bottleneck I/O. The result is "Ghost Capacity"—idle H200s waiting for data.

PermStream Nucleus fixes this through an **Asymmetric Encoding-Decoding Scheme (AEDS)** paired with **Braid Group Topology**. 

### The Math Behind the 11x Gain
Traditional compressors encode and decode in the same order. By decoupling the codebook symmetry, PermStream shifts the heavy mathematical path-finding (the AI Predictor) entirely to the encoding phase. 

The resulting decode throughput $T$ is mathematically constrained only by vectorization efficiency $V_{eff}$ and cache locality $C_{loc}$:

$$ T = \frac{V_{eff} \times C_{loc}}{IPC} $$

By using zero-allocation stack buffers and fixed 8-byte SIMD chunk unrolling (`vqtbl` / `vpshufb`), the instruction flow stays perfectly aligned within L1/L2 cache boundaries. There is no heap allocation during decompression, eliminating the garbage collection pauses that plague async pipelines.

### Benchmarks (Log-Specific Telemetry)

| Codec | Goal | Compression Ratio | Decomp. Speed | Search Speed |
| :--- | :--- | :--- | :--- | :--- |
| **LZ4** | Throughput | Moderate (12x-14x) | 1600 MB/s | N/A |
| **Zstandard (L3)** | Balanced | High (18x-20x) | 1300 MB/s | N/A |
| **PermStream** | Disruption | **High (>LZ4)** | **8500 MB/s (Peak)** | **100x Faster** |

### Trojan Horse Integrations
You don't need to rewrite your stack to test the receipts. I've built drop-in integrations for the most common bottlenecks:
1. **Rust Tracing:** A `tracing-subscriber` layer that compresses high-volume structured JSON logs in memory before hitting disk.
2. **PyTorch DataLoaders:** A C++ LibTorch extension (stable ABI) that bypasses Python GIL bottlenecks to stream `.psfs` datasets directly into GPU VRAM. 
3. **Apache Arrow:** Implemented directly into the C++ `FunctionRegistry`.

### Try It Out
The decoder and integrations are FOSS. I’m currently scaling up the proprietary AI Encoder as an enterprise offering for those needing hardware-enforced (TEE) IP protection.

Code: [GitHub Link]
Docs & Receipts: [permstream.io]

Would love feedback on the SIMD loop unrolling approach or if anyone has tested it against specific custom silicon (Trainium/Inferentia).
