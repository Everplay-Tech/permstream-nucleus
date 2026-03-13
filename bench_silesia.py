import os
import time
import subprocess
import urllib.request
import zipfile
import shutil

SILESIA_URL = "https://sun.aei.polsl.pl//~sdeor/corpus/silesia.zip"
CORPUS_DIR = "silesia_corpus"

def download_silesia():
    if not os.path.exists(CORPUS_DIR):
        print("Downloading Silesia Corpus...")
        os.makedirs(CORPUS_DIR)
        zip_path = "silesia.zip"
        urllib.request.urlretrieve(SILESIA_URL, zip_path)
        print("Extracting...")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(CORPUS_DIR)
        os.remove(zip_path)
    print("Silesia Corpus ready.")

def run_command(cmd, input_file, output_file):
    start = time.perf_counter()
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    end = time.perf_counter()
    return end - start

def benchmark_tools():
    results = {}
    files = [os.path.join(CORPUS_DIR, f) for f in os.listdir(CORPUS_DIR) if os.path.isfile(os.path.join(CORPUS_DIR, f))]
    
    print("Pre-building PermStream (Rust) release binary...")
    subprocess.run(["cargo", "build", "--release", "--manifest-path=psfsd/Cargo.toml"], check=True)
    rust_bin = os.path.join(os.path.dirname(__file__), "target/release/psfsd")
    if not os.path.exists(rust_bin):
        # Handle cases where target is in a different place
        rust_bin = "psfsd/target/release/psfsd"
    
    tools = {
        "gzip": {"compress": ["gzip", "-k", "-f"], "ext": ".gz"},
        "lz4": {"compress": ["lz4", "-f", "-q"], "ext": ".lz4"},
        "zstd": {"compress": ["zstd", "-f", "-q"], "ext": ".zst"},
        "permstream (Rust)": {"compress": [rust_bin, "pack", "--no-rank"], "ext": ".psfs"}
    }

    print("\n--- Silesia Corpus Benchmark ---")
    print(f"{'File':<15} | {'Size (MB)':<10} | {'Tool':<18} | {'Comp. Size (MB)':<15} | {'Ratio (%)':<10} | {'Time (s)':<10}")
    print("-" * 85)

    for f in sorted(files):
        orig_size = os.path.getsize(f)
        orig_mb = orig_size / (1024 * 1024)
        
        for tool, config in tools.items():
            out_file = f + config["ext"]
            
            if tool == "permstream (Rust)":
                # PermStream packs directories, but we can just pack the single file
                # by creating a temp dir
                temp_dir = f + "_temp"
                os.makedirs(temp_dir, exist_ok=True)
                shutil.copy(f, os.path.join(temp_dir, os.path.basename(f)))
                
                cmd = config["compress"] + [temp_dir, out_file]
                try:
                    duration = run_command(cmd, None, None)
                    comp_size = os.path.getsize(out_file)
                    ratio = (comp_size / orig_size) * 100
                    print(f"{os.path.basename(f):<15} | {orig_mb:<10.2f} | {tool:<18} | {comp_size/(1024*1024):<15.2f} | {ratio:<10.2f} | {duration:<10.4f}")
                except subprocess.CalledProcessError:
                    print(f"{os.path.basename(f):<15} | {orig_mb:<10.2f} | {tool:<18} | {'ERROR':<15} | {'ERROR':<10} | {'ERROR':<10}")
                
                if os.path.exists(out_file):
                    os.remove(out_file)
                shutil.rmtree(temp_dir)

            else:
                cmd = config["compress"] + [f]
                try:
                    duration = run_command(cmd, None, None)
                    if os.path.exists(out_file):
                        comp_size = os.path.getsize(out_file)
                        ratio = (comp_size / orig_size) * 100
                        print(f"{os.path.basename(f):<15} | {orig_mb:<10.2f} | {tool:<18} | {comp_size/(1024*1024):<15.2f} | {ratio:<10.2f} | {duration:<10.4f}")
                        os.remove(out_file)
                except FileNotFoundError:
                    print(f"{os.path.basename(f):<15} | {orig_mb:<10.2f} | {tool:<18} | {'NOT INSTALLED':<15} | {'N/A':<10} | {'N/A':<10}")
                except subprocess.CalledProcessError:
                    print(f"{os.path.basename(f):<15} | {orig_mb:<10.2f} | {tool:<18} | {'ERROR':<15} | {'ERROR':<10} | {'ERROR':<10}")

if __name__ == "__main__":
    download_silesia()
    benchmark_tools()
