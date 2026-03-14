# PermStream Nucleus - Enterprise Encoder (Paid Writer)

Welcome to the `nucleus-writer` directory. This sub-project represents the **"Paid Writer"** component of the PermStream ecosystem, designed specifically for AI-centric data centers and 2026 enterprise infrastructure.

## ⚖️ The Asymmetric Open Source Model

PermStream operates on an **Asymmetric Open Source** strategy:

1. **The Free Reader (`libpermstream` & `psfsd` unpacker):** Fully open-source (Apache 2.0). Anyone can integrate the 1GB/s+ Rust SIMD decoder into their ecosystem (DuckDB, vLLM, Agentic AI) for free. This eliminates the "read-lock" friction of proprietary formats.
2. **The Paid Writer (`nucleus-writer`):** A proprietary, enterprise-grade encoder. It utilizes our heavily optimized, mathematically complex AI-driven predictor to generate the highly compressed `.psfs` format. 

### Why Asymmetric?
As 2026 racks push past 100 kW and networking hits 800G, the bottleneck is CPU-heavy compression. The Asymmetric Encoding-Decoding Scheme (AEDS) paired with topological Braid Groups allows us to offload the heavy compute to the "Writer" phase, so the "Reader" can decode at pure memory bandwidth limits.

## 🛡️ Enterprise Protections

To protect the intellectual property (AI weights and Braid Mathematics) required for ultra-compression, this module includes:
* **Binary Hardening:** Compiled with aggressive symbol stripping, link-time optimization (`lto = true`), and size optimization (`opt-level = "z"`).
* **String Obfuscation:** Critical keys and logic paths are encrypted at compile time.
* **TEE Attestation (Mock):** Designed to interface with Hardware Trusted Execution Environments (Intel TDX, NVIDIA Confidential Computing) before exposing the AI predictor state.

## 📈 Marketing Through Metadata

Every archive packed by the Enterprise Encoder automatically calculates and prints an **Efficiency Badge**. 
```text
[PermStream Efficiency Badge] Compression: 85.2% saved | Energy Efficiency: High | Decoder: Free at permstream.io
```
This metadata allows organizations to easily demonstrate infrastructure ROI and bandwidth savings to their peers, serving as a viral distribution loop for the ecosystem.

## 📄 Licensing

The source code within this `nucleus-writer` directory is licensed under the **Business Source License (BSL 1.1)**. Please see [LICENSE.BSL](./LICENSE.BSL) for details. It will automatically transition to Apache 2.0 in 2030.