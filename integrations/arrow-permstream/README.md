# Apache Arrow & DataFusion Integration

PermStream Nucleus operates as a "Standardization Accelerator" for the Apache Arrow ecosystem. By registering the PermStream SIMD decoder as a custom `ScalarFunction`, DataFusion query engines can perform zero-copy scans on high-entropy tabular data and quantized tensors at 1GB/s+.

## Architecture

This crate provides the Rust bindings to register PermStream within the `arrow::compute::FunctionRegistry`.

### Why Arrow?
In 2026, the "CPU Tax" of standard compression (Zstd/LZ4) bottlenecks 800G networks during Arrow IPC streaming. PermStream’s Braid Group topology shifts the compute burden to the encoding phase, allowing DataFusion nodes to scan compressed logs and telemetry without stalling the CPU.

## Usage (Concept)

```rust
use arrow::compute::registry::FunctionRegistry;
use arrow_permstream::PermStreamCodec;

fn main() {
    let mut registry = FunctionRegistry::new();
    
    // Register the PermStream ScalarKernel
    registry.register_scalar_function(PermStreamCodec::new())
        .expect("Failed to register PermStream codec");
        
    // DataFusion will now automatically invoke PermStream 
    // when scanning .psfs encoded Arrow IPC streams.
}
```
