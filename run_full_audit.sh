#!/bin/bash
set -e

echo "==========================================="
echo "PermStream Nucleus - Red Team Audit Script"
echo "==========================================="
echo ""

echo "[1/4] Running core structural tests (Lossless Invariants & Security)..."
cargo test --manifest-path=Cargo.toml
echo "✅ Core structural tests passed."
echo ""

echo "[2/4] Verifying Fenwick Tree (Binary Indexed Tree) math logic..."
cargo test test_fenwick_tree_logic --manifest-path=Cargo.toml
echo "✅ Fenwick Tree mathematical audit passed."
echo ""

echo "[3/4] Verifying ZipNN Bit-Plane Transform logic..."
cargo test test_bitplane_transform --manifest-path=Cargo.toml
echo "✅ Bit-Plane separation audit passed."
echo ""

echo "[4/4] Invoking Silesia Benchmarks (Checking for regressions)..."
echo "Note: The benchmark uses the permstream-test-suite repository."
if [ -d "permstream-test-suite" ]; then
    python3 permstream-test-suite/bench_silesia.py
else
    echo "⚠️ permstream-test-suite directory not found. Skipping benchmark execution."
    echo "To run the benchmark, please ensure the test suite is cloned locally."
fi

echo ""
echo "==========================================="
echo "ALL CLEAR. Audit successfully executed."
echo "==========================================="
