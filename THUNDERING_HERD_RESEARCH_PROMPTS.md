# Thundering Herd & EMSGSIZE Mitigation Research Prompts

These prompts are designed for Gemini (Advanced/Pro) to generate a targeted, kernel-level solution for the `EMSGSIZE` failure encountered during the Phase 3 macOS benchmark.

---

### Prompt 1: The macOS TCP Stack & EMSGSIZE
**Goal:** Understand why macOS Tahoe is throwing `Message too long` under extreme local loopback concurrency and find sysctl or code-level mitigations.

> **System Context:** I am load-testing a Rust-based gRPC data daemon (`psfsd` using Tonic/Tokio). The benchmark spawns 500-2000 Python threads simultaneously requesting 1MB chunks from `localhost:50051`. The server has request coalescing (single-flight) implemented.
> 
> **The Problem:** The Python gRPC client instantly fails with: `E0000 00:00:1773455063.282951  784560 tcp_posix.cc:595] recvmsg encountered uncommon error: Message too long` (which maps to `EMSGSIZE`). This happens exactly when the concurrency spikes, even though the data payload is well within the 64MB gRPC max message size I configured.
> 
> **Deep Research Task:**
> Analyze the macOS network kernel behavior (specifically loopback `lo0` and TCP socket buffers) under high-frequency local traffic.
> 1. Why does macOS throw `EMSGSIZE` on a TCP stream? Is this related to socket buffer exhaustion (`net.inet.tcp.recvspace`), MTU/MSS mismatch on the loopback, or a bug in how `grpc-core` handles `recvmsg` when the kernel drops packets?
> 2. What `sysctl` commands can I run on macOS to expand the relevant buffers, queues, or max slots to survive a 2,000-connection burst?
> 3. Does Explicit Congestion Notification (ECN) on macOS interfere with high-speed local gRPC traffic, and should it be disabled?

---

### Prompt 2: The "Linux Transition" vs. "Local Shared Memory"
**Goal:** Determine the strategic path forward. Do we fight macOS, move to Linux, or bypass the network entirely?

> **System Context:** My Rust data node serves AI tensors at 1GB/s. The core engine is mathematically sound, but local TCP scaling on macOS fails at ~500 concurrent connections.
> 
> **The Decision:** I need to choose the next engineering step to prove the "Thundering Herd" resilience of this architecture to investors and users.
> 
> **Deep Research Task:**
> Evaluate the following three paths for a solo developer building a high-performance data node in 2026:
> 
> **Path A (The Cloud VM):** Deploying the daemon to an AWS/GCP Linux instance and using `io_uring` or standard Linux `epoll`. Will Linux inherently survive 10,000 loopback connections better than macOS, or will I just hit a different `ulimit`/`somaxconn` wall?
> 
> **Path B (Zero-Copy IPC):** Bypassing TCP entirely for local workers. Can I use a Rust crate like `iceoryx2` or `shmem-ipc` to create a lock-free ring buffer in shared memory? How difficult is it to integrate shared memory with a Python PyTorch DataLoader client?
> 
> **Path C (gRPC Flow Control Tuning):** Staying on TCP but tuning Tonic/gRPC. Are there specific HTTP/2 flow control windows (`initial_stream_window_size`, `initial_connection_window_size`) that I must explicitly configure in the Rust `Server::builder()` to prevent the client from overwhelming the macOS socket buffers?
> 
> Provide a recommendation on which path offers the highest ROI for a solo developer trying to prove 10,000-worker concurrency.
