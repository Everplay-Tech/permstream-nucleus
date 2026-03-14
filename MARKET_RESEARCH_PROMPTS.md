# PermStream Nucleus: Go-to-Market Research Prompts

These prompts are designed to be fed into Gemini (Advanced/Pro) to generate a realistic, actionable, and highly specific Go-to-Market (GTM) strategy for a solo founder without existing industry connections.

---

### Prompt 1: The "Wedge" Identification (Finding the Desperate Buyer)
**Goal:** Identify the specific sub-niche in 2026 that is losing the most money due to I/O bottlenecks and will adopt a new tool without enterprise sales friction.

> **Context:** I am a solo developer who has built "PermStream Nucleus," a high-throughput data engine. It uses Braid Group topology and AI to achieve 1GB/s+ lossless compression. It specifically beats zstd and lz4 on:
> 1. Streaming neural network weights (via bit-plane separation).
> 2. High-speed structured data/telemetry (via Fenwick Trees).
> 3. Zero-VRAM divergence GPU unpermutation.
> 
> **My Constraint:** I am a solo founder with zero industry connections and no enterprise sales team. I cannot afford 6-month B2B sales cycles.
> 
> **Deep Research Task:**
> Analyze the 2025/2026 data infrastructure market to identify the perfect "wedge" use-case. I need a user profile that is experiencing "hair-on-fire" pain related to I/O bottlenecks right now, where the engineer has the authority to just download my Rust binary and use it. 
> 
> 1. What specific niche (e.g., decentralized LLM inference nodes, algorithmic trading telemetry, edge-device video ingestion) is desperate enough to try a non-standard codec?
> 2. Who is the exact buyer persona (Job Title)? 
> 3. What open-source tools are they currently frustrated with?

---

### Prompt 2: Product-Led Growth (PLG) & The "Asymmetric Open Source" Strategy
**Goal:** Determine how to monetize the project without giving away the proprietary AI math, and how to get users to market it for you.

> **Context:** PermStream Nucleus relies on an "Asymmetric" architecture: the Encoder uses complex AI and topological pruning to find the perfect compression path, while the Decoder is just a "dumb," ultra-fast 1GB/s SIMD unrolling loop in Rust. 
> 
> **My Constraint:** I need to monetize this as a solo founder while maintaining my intellectual property (the AI predictor weights and braid mathematics).
> 
> **Deep Research Task:**
> Research the most successful "Open Core" or "Product-Led Growth" (PLG) models used by infrastructure startups in 2024-2026 (e.g., DuckDB, MotherDuck, Tailscale).
> 
> 1. Evaluate the "Asymmetric Open Source" strategy: If I open-source the ultra-fast Rust *Decoder* (so anyone can read PermStream files for free) but keep the *Encoder* (the AI compressor) proprietary behind a commercial license or API, will this drive viral adoption? 
> 2. Provide 3 specific examples of companies that successfully used a "Free Reader / Paid Writer" model for data formats.
> 3. Formulate a pricing strategy (e.g., usage-based vs. seat-based) that a solo developer can realistically enforce using standard license keys.

---

### Prompt 3: "Zero-Connection" Distribution & Developer Marketing
**Goal:** Create a step-by-step launch plan that relies on engineering credibility and viral benchmarks rather than networking.

> **Context:** I am ready to launch PermStream Nucleus. I have concrete, mathematically verified benchmark receipts showing an 11x throughput gain over standard scalar loops and compression ratios that beat LZ4.
> 
> **My Constraint:** I have no existing audience, no VC backing, and no PR firm.
> 
> **Deep Research Task:**
> Design a "Zero-Connection" distribution strategy. How does a solo engineer get a highly technical infrastructure tool in front of senior systems architects in 2026?
> 
> 1. Identify the top 3 high-leverage platforms for technical "Show and Tell" (e.g., Hacker News, specific Subreddits, specific Discord communities). 
> 2. Outline the exact structure of the "Launch Post." What should the headline be? What benchmark data should be shown in the first 3 lines to instantly hook a senior Rust/C++ engineer?
> 3. Suggest a "Trojan Horse" integration: Is there an existing popular framework (like PyTorch DataLoaders, Apache Arrow, or a specific Rust logging crate) where I should build a drop-in PermStream plugin to steal their users? Provide the exact GitHub repos I should target for these integrations.
