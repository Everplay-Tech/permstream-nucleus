# PermStream Nucleus 🚀 (The Rust AI Data Engine)

[![Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Rust](https://img.shields.io/badge/Rust-1.75%2B-orange)](https://www.rust-lang.org/)
[![WGPU](https://img.shields.io/badge/GPU-WGPU%2FMetal%2FVulkan-green)](https://wgpu.rs/)

## 🌟 **The World's Fastest AI-Driven Data Engine**

PermStream Nucleus is the high-performance Rust evolution of the PermStream Pro prototype. It is designed for multi-gigabyte-per-second streaming, leveraging AI entropy prediction and GPU-accelerated Braid permutations to deliver near-Shannon limit compression at unprecedented speeds.

### 🏆 **Benchmark Breakthroughs (March 2026 Audit)**
| Dataset | Original Time | Optimized Time | Throughput Gain |
|---------|---------------|----------------|-----------------|
| **Dickens (Text)** | 10.0s | **0.90s** | **11x FASTER** |
| **Mozilla (Code)** | 3.8s | **2.22s** | **1.7x FASTER** |
| **XML Data** | 1.5s | **0.23s** | **6.5x FASTER** |

## 🎯 **Core Innovations (Neural Data Engine Paradigm)**
*   **A3B Predictor (Relocatable)**: AI entropy forecasting moved to the encoding phase, allowing the decoder to run at pure memory bandwidth speeds.
*   **O(N) Fenwick Tree Coder**: A Binary Indexed Tree frequency model that reduces cumulative probability lookups from $O(256)$ to $O(8)$ per symbol.
*   **GPU SRAM Tiling**: WGSL shaders that unweave Braid permutations entirely within GPU L1 cache (Shared Memory), eliminating VRAM warp divergence.
*   **Bit-Plane Separation**: ZipNN-inspired transform that isolates structured exponents from noisy mantissas in neural network weights (FP32/BFloat16).

## 🚀 **Quick Start (Rust Daemon)**

```bash
# 1. Build the production binary
cargo build --release

# 2. Pack a directory into a PSFS container
./target/release/psfsd pack ./data ./archive.psfs --transform bitplane

# 3. Mount with FUSE (Experimental)
python psfs.py mount ./archive.psfs /mnt/psfs
```

## 🛡️ **Red-Team Auditing & Future Expansion**

PermStream Nucleus is built for autonomous evolution. To prevent "hallucinated" algorithmic gains and ensure mathematical losslessness, we maintain a strict Red-Team Audit process.

### **How to Audit (For Gemini / Developers)**
If you are an AI agent or developer modifying the core algorithms, you **must** execute the unified audit script to verify core invariants:

```bash
./run_full_audit.sh
```

**This script performs:**
1.  **Structural Integrity Checks**: Verifies Zip-Slip protection, path jailing, and security hardening.
2.  **Mathematical Proofs**: Validates the Fenwick Tree's $O(\log N)$ logic and Bit-Plane inversion.
3.  **Lossless Roundtrip**: Compresses and decompresses random data to ensure zero-bit loss.
4.  **Silesia Regression**: Runs the standard industry benchmark to catch throughput regressions.

For a detailed breakdown of the March 2026 Audit, see [REDTEAM_AUDIT.md](REDTEAM_AUDIT.md).

## 📈 **Roadmap**
*   [x] **Phase 1-2**: 10x Throughput gain via Fenwick Trees & relocatable AI.
*   [x] **GPU Tiling**: Zero-divergence SRAM unpermutation.
*   [ ] **Phase 3**: Thundering Herd (10,000 concurrent AI worker stress test).
*   [ ] **AVX-512/AMX**: Direct-to-register hardware unbraiding.

## 🤝 **License**
[Apache License 2.0](LICENSE) - Free for commercial use, modification, distribution.

---
*Built with ❤️ by Everplay Tech & Magus using AI-driven permutation mathematics*
