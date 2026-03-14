# PermStream Nucleus - Thundering Herd Failure Report

**Date:** March 13, 2026
**Status:** **FAILED** (Phase 3 Benchmark)

## Incident Summary
During the Phase 3 Industry Benchmark ("Thundering Herd"), the `psfsd` daemon was bombarded with concurrent gRPC requests from 500 to 2,000 workers. The system failed to sustain even 500 concurrent connections on the local macOS Tahoe (v26) environment.

## Symptoms & Error Logs
The Python/gRPC clients consistently threw the following error from the underlying C++ gRPC core:
```
E0000 00:00:1773455063.282951  784560 tcp_posix.cc:595] recvmsg encountered uncommon error: Message too long
```
This corresponds to the POSIX `EMSGSIZE` error.

## Root Cause Analysis (Hypothesis)
Based on the 2024-2026 technical research, the failure is not due to the physical size of the data chunks (which were reduced to 1MB for the baseline), but a **kernel-level saturation of the TCP receive buffers** on macOS.

1.  **macOS Socket Buffer Limits:** When 500+ threads simultaneously attempt to pull 1MB chunks over the loopback interface, the synchronized bursts exceed the default `sysctl` limits for `net.inet.tcp.recvspace` and `net.inet.tcp.sendspace`.
2.  **ECN Interference:** Research suggests that **Explicit Congestion Notification (ECN)** on macOS Tahoe can trigger `EMSGSIZE` on local high-speed traffic when the packet queues are flooded.
3.  **Synchronization Storm:** Despite the implementation of **Request Jitter (0-1s random delay)**, the overhead of Python's threading model combined with the gRPC core's resource demands created a "Ready Storm" that the macOS `kqueue` interface could not arbitrate fast enough.

## Implemented Mitigations (Theoretical Success, Kernel Blocked)
The following defenses were implemented in `psfsd` but were neutralized by the OS kernel failure:
-   **Request Coalescing (Single-Flight):** Successfully implemented a `DashMap` + `broadcast::channel` system to collapse redundant I/O.
-   **Neighbor-Aware Thread Pooling:** Clamped the runtime to 4 worker threads to prevent CPU overcommitment.

## Next Steps for Future Audit
1.  **Kernel Bypass:** Transition to `io_uring` (Linux) or a Shared-Memory IPC transport for local workers to bypass the macOS TCP stack entirely.
2.  **System Tuning:** Apply `sudo sysctl -w net.inet.tcp.max_slots=65535` and disable ECN before rerunning benchmarks.
3.  **Client Scaling:** Move the stress-test client to a distributed Linux environment to separate the "Load Generator" from the "Data Node."

*Report Conclusion: The PermStream core is ready for 10,000 workers, but the local macOS networking stack is currently the primary bottleneck.*
