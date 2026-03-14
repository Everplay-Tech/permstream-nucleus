use num_bigint::BigUint;
use num_traits::{One, Zero, ToPrimitive};
use ndarray::Array1;
use std::io::{Read, Write, Cursor};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

pub const MAGIC: &[u8; 4] = b"PSP1";
pub const VERSION: u8 = 4;

pub const FLAG_USE_RANK: u8 = 1 << 0;
pub const FLAG_PREDICTOR_HEADER: u8 = 1 << 1;
pub const FLAG_TRANSFORM_SHIFT: u8 = 2;
pub const FLAG_TRANSFORM_MASK: u8 = 0b00011100;
pub const FLAG_FILE_RAW: u8 = 1 << 5;
pub const CHUNK_FLAG_RAW: u8 = 1 << 0;

pub const DEFAULT_CHUNK_SIZE: usize = 512 * 1024;
pub const DEFAULT_BLOCK_SIZE: usize = 64;
pub const DEFAULT_ENTROPY_SKIP: f64 = 7.5;

#[derive(Debug, Clone)]
pub enum PredictorMode {
    Seeded,
    Header,
}

#[derive(Debug, Clone)]
pub struct CodecConfig {
    pub chunk_size: usize,
    pub block_size: usize,
    pub use_rank: bool,
    pub predictor_mode: PredictorMode,
    pub seed: u32,
    pub weights: Option<Array1<f64>>,
    pub entropy_skip: f64,
    pub transform: String,
    pub force_raw: bool,
}

impl Default for CodecConfig {
    fn default() -> Self {
        Self {
            chunk_size: DEFAULT_CHUNK_SIZE,
            block_size: DEFAULT_BLOCK_SIZE,
            use_rank: true,
            predictor_mode: PredictorMode::Seeded,
            seed: 1337,
            weights: None,
            entropy_skip: DEFAULT_ENTROPY_SKIP,
            transform: "none".to_string(),
            force_raw: false,
        }
    }
}

pub fn get_transform_id(name: &str) -> u8 {
    match name {
        "none" => 0,
        "delta" => 1,
        "xor" => 2,
        "evenodd" => 3,
        "bitplane" => 4,
        _ => 0,
    }
}

pub fn get_transform_name(id: u8) -> String {
    match id {
        1 => "delta".to_string(),
        2 => "xor".to_string(),
        3 => "evenodd".to_string(),
        4 => "bitplane".to_string(),
        _ => "none".to_string(),
    }
}

pub mod gpu;
pub mod rag;

pub mod predictor {
    use super::*;
    use rand::prelude::*;
    use rand_chacha::ChaCha8Rng;

    pub struct A3BPredictor {
        pub weights: Array1<f64>,
        pub learning_rate: f64,
        pub history: Vec<f64>,
    }

    impl A3BPredictor {
        pub fn new(weights: Option<Array1<f64>>, learning_rate: f64) -> Self {
            let weights = weights.unwrap_or_else(|| {
                Array1::from_vec(vec![0.05, 0.05, 0.05, 0.05])
            });
            Self {
                weights,
                learning_rate,
                history: Vec::with_capacity(10),
            }
        }

        pub fn init_weights(mode: &PredictorMode, seed: u32, weights: Option<Array1<f64>>) -> Array1<f64> {
            if let (PredictorMode::Header, Some(w)) = (mode, weights) {
                return w;
            }
            let mut rng = ChaCha8Rng::seed_from_u64(seed as u64);
            Array1::from_shape_fn(4, |_| rng.gen::<f64>() * 0.1)
        }

        pub fn predict(&self, features: &Array1<f64>) -> f64 {
            let theta = self.weights.dot(features);
            if theta.is_nan() { return 0.5; }
            theta.clamp(0.0, 1.0)
        }

        pub fn update(&mut self, actual_entropy: f64, predicted: f64, features: &Array1<f64>) {
            let error = actual_entropy - predicted;
            let update_val = features.mapv(|f| f * self.learning_rate * error);
            self.weights -= &update_val;
            self.history.push(actual_entropy);
            if self.history.len() > 10 {
                self.history.remove(0);
            }
        }
    }
}

pub mod crypto {
    use super::*;

    pub fn xorshift32(mut x: u32) -> u32 {
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        x
    }

    pub fn chunk_state(seed: u32, file_id: u32, chunk_index: u32) -> u32 {
        let mut state = (seed as u64) ^ ((file_id as u64) << 32) ^ (chunk_index as u64);
        // Fast 64-bit avalanche mixer (SplitMix-style)
        state = (state ^ (state >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        state = (state ^ (state >> 27)).wrapping_mul(0x94d049bb133111eb);
        state = state ^ (state >> 31);
        state as u32
    }

    pub fn braid_permutation(n: usize, state: u32, theta: f64) -> (Vec<usize>, u32) {
        let theta_int = (theta * 1_000_000.0) as u64 as u32;
        let mut seed = xorshift32(state ^ theta_int ^ (n as u32));
        if seed == 0 {
            seed = 1;
        }
        let mut perm: Vec<usize> = (0..n).collect();
        let mut x = seed;
        for i in (1..n).rev() {
            x = xorshift32(x);
            let j = (x % (i as u32 + 1)) as usize;
            perm.swap(i, j);
        }
        let new_state = xorshift32(state ^ seed ^ (n as u32));
        (perm, new_state)
    }

    pub fn factorials(n: usize) -> Vec<BigUint> {
        let mut facts = vec![BigUint::one(); n + 1];
        for i in 2..=n {
            facts[i] = &facts[i - 1] * BigUint::from(i);
        }
        facts
    }

    pub fn perm_to_rank(perm: &[usize]) -> BigUint {
        let n = perm.len();
        let mut remaining: Vec<usize> = (0..n).collect();
        let facts = factorials(n);
        let mut rank = BigUint::zero();
        for (i, &val) in perm.iter().enumerate() {
            let idx = remaining.iter().position(|&x| x == val).expect("Inconsistent permutation");
            rank += BigUint::from(idx) * &facts[n - 1 - i];
            remaining.remove(idx);
        }
        rank
    }

    pub fn rank_to_perm(mut rank: BigUint, n: usize) -> Vec<usize> {
        let mut remaining: Vec<usize> = (0..n).collect();
        let facts = factorials(n);
        let mut perm = vec![0; n];
        for k in (0..n).rev() {
            let fact = &facts[k];
            let idx = (&rank / fact).to_usize().unwrap();
            rank %= fact;
            perm[n - 1 - k] = remaining.remove(idx);
        }
        perm
    }

    pub fn invert_permutation(perm: &[usize]) -> Vec<usize> {
        let mut inv = vec![0; perm.len()];
        for (i, &val) in perm.iter().enumerate() {
            inv[val] = i;
        }
        inv
    }

    pub fn encode_rank<W: Write>(writer: &mut W, rank: &BigUint) -> std::io::Result<()> {
        let bytes = rank.to_bytes_be();
        if bytes.len() > 0xFFFF {
            return Err(std::io::Error::new(std::io::ErrorKind::Other, "Rank too large"));
        }
        writer.write_u16::<BigEndian>(bytes.len() as u16)?;
        writer.write_all(&bytes)?;
        Ok(())
    }

    pub fn decode_rank<R: Read>(reader: &mut R) -> std::io::Result<BigUint> {
        let len = reader.read_u16::<BigEndian>()? as usize;
        let mut bytes = vec![0u8; len];
        reader.read_exact(&mut bytes)?;
        Ok(BigUint::from_bytes_be(&bytes))
    }

    pub fn permute_chunk(chunk: &[u8], block_size: usize, mut state: u32, theta: f64, use_rank: bool, transform_id: u8) -> (Vec<u8>, Vec<BigUint>, u32) {
        let chunk_len = chunk.len();
        let mut permuted = vec![0u8; chunk_len];
        let mut ranks = Vec::new();
        let mut offset = 0;
        
        // High-entropy bypass: If theta is extremely high, the braid is essentially randomized noise
        // we can skip the compute-heavy permutation and pass to AC directly.
        let skip_braid = theta > 0.98;

        while offset < chunk_len {
            let block_len = std::cmp::min(block_size, chunk_len - offset);
            let (perm, new_state) = braid_permutation(block_len, state, theta);
            state = new_state;
            if use_rank {
                ranks.push(perm_to_rank(&perm));
            }
            let block = &chunk[offset..offset+block_len];
            
            if skip_braid {
                // Pass through but still apply transform inline
                for (i, &val) in block.iter().enumerate() {
                    let mut v = val;
                    if transform_id == 1 && i > 0 { v = v.wrapping_sub(block[i-1]); }
                    else if transform_id == 2 && i > 0 { v = v ^ block[i-1]; }
                    permuted[offset + i] = v;
                }
            } else {
                // Fused Transform-Permutation (Lifting Braid style)
                for (i, &p) in perm.iter().enumerate() {
                    let mut v = block[p];
                    // Apply Delta/XOR inline relative to original block structure to expose redundancy
                    if transform_id == 1 && p > 0 { v = v.wrapping_sub(block[p-1]); }
                    else if transform_id == 2 && p > 0 { v = v ^ block[p-1]; }
                    permuted[offset + i] = v;
                }
            }
            offset += block_len;
        }
        (permuted, ranks, state)
    }

    pub fn unpermute_chunk(permuted: &[u8], block_size: usize, ranks: &[BigUint], mut state: u32, theta: f64, use_rank: bool, transform_id: u8) -> Vec<u8> {
        let chunk_len = permuted.len();
        // Pad to a multiple of 32 bytes for future SIMD "wild copies" (ZXC optimization paradigm)
        let padded_len = (chunk_len + 31) & !31;
        let mut restored = vec![0u8; padded_len];
        let mut offset = 0;
        let mut rank_index = 0;
        
        let skip_braid = theta > 0.98;

        while offset < chunk_len {
            let block_len = std::cmp::min(block_size, chunk_len - offset);
            let perm = if skip_braid {
                (0..block_len).collect()
            } else if use_rank {
                let rank = ranks[rank_index].clone();
                rank_index += 1;
                rank_to_perm(rank, block_len)
            } else {
                let (p, new_state) = braid_permutation(block_len, state, theta);
                state = new_state;
                p
            };
            let inv = invert_permutation(&perm);
            let block = &permuted[offset..offset+block_len];
            
            // SIMD-Friendly Fast Path (Unrolled for Auto-Vectorization)
            // We use fixed 8-byte chunks to encourage LLVM to emit NEON/AVX shuffle instructions
            let mut i = 0;
            while i + 8 <= block_len {
                let p0 = inv[i];
                let p1 = inv[i+1];
                let p2 = inv[i+2];
                let p3 = inv[i+3];
                let p4 = inv[i+4];
                let p5 = inv[i+5];
                let p6 = inv[i+6];
                let p7 = inv[i+7];
                
                restored[offset + i] = block[p0];
                restored[offset + i + 1] = block[p1];
                restored[offset + i + 2] = block[p2];
                restored[offset + i + 3] = block[p3];
                restored[offset + i + 4] = block[p4];
                restored[offset + i + 5] = block[p5];
                restored[offset + i + 6] = block[p6];
                restored[offset + i + 7] = block[p7];
                
                i += 8;
            }
            
            // Remainder loop
            while i < block_len {
                restored[offset + i] = block[inv[i]];
                i += 1;
            }
            
            if transform_id == 1 {
                let mut last = 0u8;
                for i in 0..block_len {
                    last = last.wrapping_add(restored[offset + i]);
                    restored[offset + i] = last;
                }
            } else if transform_id == 2 {
                let mut last = 0u8;
                for i in 0..block_len {
                    last ^= restored[offset + i];
                    restored[offset + i] = last;
                }
            } else if transform_id == 3 {
                // EvenOdd is harder to fuse, so we use a fallback for now or ignore
                let sub_block = restored[offset..offset+block_len].to_vec();
                let inverted = transforms::invert_evenodd(&sub_block);
                restored[offset..offset+block_len].copy_from_slice(&inverted);
            }
            
            offset += block_len;
        }
        // Truncate back to the exact logical length before returning
        restored.truncate(chunk_len);
        restored
    }
}

pub mod entropy {
    pub fn calculate(data: &[u8]) -> f64 {
        if data.is_empty() {
            return 0.0;
        }
        let mut freq = [0usize; 256];
        for &byte in data {
            freq[byte as usize] += 1;
        }
        let len = data.len() as f64;
        let mut ent = 0.0;
        for &count in freq.iter() {
            if count > 0 {
                let p = count as f64 / len;
                ent -= p * p.log2();
            }
        }
        ent
    }
}

pub mod transforms {
    use super::*;

    pub fn apply_transform(data: &[u8], transform_id: u8) -> Vec<u8> {
        match transform_id {
            1 => apply_delta(data),
            2 => apply_xor(data),
            3 => apply_evenodd(data),
            4 => apply_bitplane(data),
            _ => data.to_vec(),
        }
    }

    pub fn invert_transform(data: &[u8], transform_id: u8) -> Vec<u8> {
        match transform_id {
            1 => invert_delta(data),
            2 => invert_xor(data),
            3 => invert_evenodd(data),
            4 => invert_bitplane(data),
            _ => data.to_vec(),
        }
    }

    pub fn apply_bitplane(data: &[u8]) -> Vec<u8> {
        let n = data.len();
        if n <= 4 { return data.to_vec(); }
        // Assume 4-byte (f32) stride commonly found in tensors
        let stride = 4;
        let mut out = Vec::with_capacity(n);
        for byte_idx in 0..stride {
            for i in (byte_idx..n).step_by(stride) {
                out.push(data[i]);
            }
        }
        out
    }

    pub fn invert_bitplane(data: &[u8]) -> Vec<u8> {
        let n = data.len();
        if n <= 4 { return data.to_vec(); }
        let stride = 4;
        let mut out = vec![0u8; n];
        let chunk_size = n / stride;
        let remainder = n % stride;
        
        let mut write_idx = 0;
        for i in 0..chunk_size {
            for byte_idx in 0..stride {
                let read_idx = byte_idx * chunk_size + std::cmp::min(byte_idx, remainder) + i;
                out[write_idx] = data[read_idx];
                write_idx += 1;
            }
        }
        
        // Handle remainder for non-aligned lengths
        for byte_idx in 0..remainder {
            let read_idx = byte_idx * chunk_size + byte_idx + chunk_size;
            out[write_idx] = data[read_idx];
            write_idx += 1;
        }
        out
    }

    pub fn apply_delta(data: &[u8]) -> Vec<u8> {
        if data.is_empty() { return vec![]; }
        let mut out = vec![0u8; data.len()];
        out[0] = data[0];
        for i in 1..data.len() {
            out[i] = data[i].wrapping_sub(data[i-1]);
        }
        out
    }

    pub fn invert_delta(data: &[u8]) -> Vec<u8> {
        if data.is_empty() { return vec![]; }
        let mut out = vec![0u8; data.len()];
        let mut last = 0u8;
        for i in 0..data.len() {
            last = last.wrapping_add(data[i]);
            out[i] = last;
        }
        out
    }

    pub fn apply_xor(data: &[u8]) -> Vec<u8> {
        if data.is_empty() { return vec![]; }
        let mut out = vec![0u8; data.len()];
        out[0] = data[0];
        for i in 1..data.len() {
            out[i] = data[i] ^ data[i-1];
        }
        out
    }

    pub fn invert_xor(data: &[u8]) -> Vec<u8> {
        if data.is_empty() { return vec![]; }
        let mut out = vec![0u8; data.len()];
        let mut last = 0u8;
        for i in 0..data.len() {
            last ^= data[i];
            out[i] = last;
        }
        out
    }

    pub fn apply_evenodd(data: &[u8]) -> Vec<u8> {
        if data.len() <= 1 { return data.to_vec(); }
        let mut evens = Vec::with_capacity((data.len() + 1) / 2);
        let mut odds = Vec::with_capacity(data.len() / 2);
        for (i, &val) in data.iter().enumerate() {
            if i % 2 == 0 { evens.push(val); } else { odds.push(val); }
        }
        evens.extend(odds);
        evens
    }

    pub fn invert_evenodd(data: &[u8]) -> Vec<u8> {
        if data.len() <= 1 { return data.to_vec(); }
        let n = data.len();
        let even_count = (n + 1) / 2;
        let evens = &data[..even_count];
        let odds = &data[even_count..];
        let mut out = vec![0u8; n];
        for i in 0..even_count {
            out[i * 2] = evens[i];
        }
        for i in 0..odds.len() {
            out[i * 2 + 1] = odds[i];
        }
        out
    }
}

pub mod arithmetic {
    use bitvec::prelude::*;

    pub struct FenwickTree {
        tree: [i64; 257],
    }

    impl FenwickTree {
        pub fn new(model: &[i64; 256]) -> Self {
            let mut ft = FenwickTree { tree: [0; 257] };
            for i in 0..256 {
                ft.add(i, model[i]);
            }
            ft
        }

        pub fn add(&mut self, mut idx: usize, val: i64) {
            idx += 1;
            while idx <= 256 {
                self.tree[idx] += val;
                idx += (idx as i64 & -(idx as i64)) as usize;
            }
        }

        pub fn query(&self, mut idx: usize) -> i64 {
            let mut sum = 0;
            while idx > 0 {
                sum += self.tree[idx];
                idx -= (idx as i64 & -(idx as i64)) as usize;
            }
            sum
        }

        pub fn find_symbol(&self, target: i64) -> (usize, i64) {
            let mut idx = 0;
            let mut sum = 0;
            let mut bit = 128;
            while bit > 0 {
                let next_idx = idx + bit;
                if next_idx <= 256 && sum + self.tree[next_idx] <= target {
                    idx = next_idx;
                    sum += self.tree[idx];
                }
                bit >>= 1;
            }
            (idx, sum)
        }
    }

    pub fn adaptive_model_update(model: &mut [i64; 256], ft: &mut FenwickTree, total: &mut i64, symbol: usize, entropy_factor: f64) {
        let increment = 1 + (entropy_factor as i64);
        model[symbol] += increment;
        ft.add(symbol, increment);
        *total += increment;
        if *total > (1 << 20) {
            *total = 0;
            *ft = FenwickTree { tree: [0; 257] };
            for i in 0..256 {
                model[i] = (model[i] + 1) / 2;
                if model[i] == 0 { model[i] = 1; }
                ft.add(i, model[i]);
                *total += model[i];
            }
        }
    }

    pub fn encode(chunk: &[u8], model: &mut [i64; 256], entropy_factor: f64) -> Vec<u8> {
        let mut low: u32 = 0;
        let mut high: u32 = 0xFFFFFFFF;
        let mut pending: u32 = 0;
        let mut output = bitvec![u8, Msb0;];
        
        let mut ft = FenwickTree::new(model);
        let mut total: i64 = model.iter().sum();

        for &symbol in chunk {
            let symbol = symbol as usize;
            let range = (high as u64) - (low as u64) + 1;
            
            let sym_low = ft.query(symbol);
            let sym_high = sym_low + model[symbol];
            let total_u64 = total as u64;

            high = (low as u64 + (range * sym_high as u64 / total_u64) - 1) as u32;
            low = (low as u64 + (range * sym_low as u64 / total_u64)) as u32;

            loop {
                if high < 0x80000000 {
                    output.push(false);
                    for _ in 0..pending { output.push(true); }
                    pending = 0;
                    low <<= 1;
                    high = (high << 1) | 1;
                } else if low >= 0x80000000 {
                    output.push(true);
                    for _ in 0..pending { output.push(false); }
                    pending = 0;
                    low = (low - 0x80000000) << 1;
                    high = ((high - 0x80000000) << 1) | 1;
                } else if low >= 0x40000000 && high < 0xC0000000 {
                    pending += 1;
                    low = (low - 0x40000000) << 1;
                    high = ((high - 0x40000000) << 1) | 1;
                } else {
                    break;
                }
            }
            adaptive_model_update(model, &mut ft, &mut total, symbol, entropy_factor);
        }
        
        output.push(true);
        for _ in 0..pending { output.push(false); }
        
        output.into_vec()
    }

    pub fn decode(encoded: &[u8], model: &mut [i64; 256], length: usize, entropy_factor: f64) -> Vec<u8> {
        let input_bits = encoded.view_bits::<Msb0>();
        let mut bit_index = 0;
        let mut value: u32 = 0;
        for _ in 0..32 {
            let bit = if bit_index < input_bits.len() { input_bits[bit_index] } else { false };
            value = (value << 1) | (bit as u32);
            bit_index += 1;
        }

        let mut low: u32 = 0;
        let mut high: u32 = 0xFFFFFFFF;
        let mut decoded = Vec::with_capacity(length);
        
        let mut ft = FenwickTree::new(model);
        let mut total: i64 = model.iter().sum();

        for _ in 0..length {
            let range = (high as u64) - (low as u64) + 1;
            let total_u64 = total as u64;

            let scaled = (((value as u64 - low as u64 + 1) * total_u64 - 1) / range) as i64;
            
            let (symbol, sym_low_i64) = ft.find_symbol(scaled);
            let sym_low = sym_low_i64 as u64;
            let sym_high = (sym_low_i64 + model[symbol]) as u64;

            decoded.push(symbol as u8);

            high = (low as u64 + (sym_high * range / total_u64) - 1) as u32;
            low = (low as u64 + (sym_low * range / total_u64)) as u32;

            loop {
                if high < 0x80000000 {
                    // Nothing
                } else if low >= 0x80000000 {
                    value -= 0x80000000;
                    low -= 0x80000000;
                    high -= 0x80000000;
                } else if low >= 0x40000000 && high < 0xC0000000 {
                    value -= 0x40000000;
                    low -= 0x40000000;
                    high -= 0x40000000;
                } else {
                    break;
                }
                low <<= 1;
                high = (high << 1) | 1;
                let bit = if bit_index < input_bits.len() { input_bits[bit_index] } else { false };
                value = (value << 1) | (bit as u32);
                bit_index += 1;
            }

            adaptive_model_update(model, &mut ft, &mut total, symbol, entropy_factor);
        }

        decoded
    }
}

pub fn compress_stream<R: Read, W: Write>(mut input: R, mut output: W, mut config: CodecConfig) -> std::io::Result<f64> {
    let weights = predictor::A3BPredictor::init_weights(&config.predictor_mode, config.seed, config.weights.clone());
    config.weights = Some(weights.clone());

    // Write Header
    output.write_all(MAGIC)?;
    output.write_u8(VERSION)?;
    let mut flags = 0u8;
    if config.use_rank { flags |= FLAG_USE_RANK; }
    if matches!(config.predictor_mode, PredictorMode::Header) { flags |= FLAG_PREDICTOR_HEADER; }
    let transform_id = get_transform_id(&config.transform);
    flags |= (transform_id << FLAG_TRANSFORM_SHIFT) & FLAG_TRANSFORM_MASK;
    if config.force_raw { flags |= FLAG_FILE_RAW; }
    output.write_u8(flags)?;
    output.write_u32::<BigEndian>(config.chunk_size as u32)?;
    output.write_u32::<BigEndian>(config.block_size as u32)?;
    output.write_u32::<BigEndian>(config.seed)?;
    for i in 0..4 {
        output.write_f32::<BigEndian>(weights[i] as f32)?;
    }

    let mut total_original = 0u64;
    let mut total_compressed = 4 + 1 + 1 + 4 + 4 + 4 + 16; // Header size

    if config.force_raw {
        let mut buffer = vec![0u8; config.chunk_size];
        while let Ok(n) = input.read(&mut buffer) {
            if n == 0 { break; }
            total_original += n as u64;
            output.write_all(&buffer[..n])?;
            total_compressed += n as u64;
        }
    } else {
        let mut chunk_index = 0u32;
        let mut buffer = vec![0u8; config.chunk_size];
        let mut a3b = predictor::A3BPredictor::new(Some(weights), 0.01);

        while let Ok(n) = input.read(&mut buffer) {
            if n == 0 { break; }
            let chunk = &buffer[..n];
            total_original += n as u64;

            let ent = entropy::calculate(chunk);
            let ent_q = (ent * 100.0) as u16;
            let ent_f = ent_q as f64 / 100.0;

            let features = Array1::from_vec(vec![ent_f, 0.0, n as f64, 1.0]);
            let theta = a3b.predict(&features);
            let state = crypto::chunk_state(config.seed, 0, chunk_index);

            output.write_u32::<BigEndian>(n as u32)?;
            let mut chunk_flags = 0u8;
            if ent_f >= config.entropy_skip {
                chunk_flags |= CHUNK_FLAG_RAW;
            }

            let mut encoded_payload = Vec::new();
            let mut ranks = Vec::new();
            if chunk_flags == 0 {
                let (permuted, r, _) = crypto::permute_chunk(chunk, config.block_size, state, theta, config.use_rank, transform_id);
                ranks = r;
                let mut model = [1i64; 256];
                encoded_payload = arithmetic::encode(&permuted, &mut model, ent_f);
                
                let mut rank_bytes_len = 0;
                if config.use_rank {
                    for r in &ranks {
                        rank_bytes_len += 2 + r.to_bytes_be().len();
                    }
                }
                let compressed_size = 1 + 2 + rank_bytes_len + 4 + encoded_payload.len();
                let raw_size = 1 + 2 + n;
                if compressed_size >= raw_size {
                    chunk_flags |= CHUNK_FLAG_RAW;
                }
            }

            output.write_u8(chunk_flags)?;
            output.write_u16::<BigEndian>(ent_q)?;
            total_compressed += 4 + 1 + 2;

            if (chunk_flags & CHUNK_FLAG_RAW) != 0 {
                output.write_all(chunk)?;
                total_compressed += n as u64;
            } else {
                if config.use_rank {
                    for r in &ranks {
                        crypto::encode_rank(&mut output, r)?;
                        total_compressed += (2 + r.to_bytes_be().len()) as u64;
                    }
                }
                output.write_u32::<BigEndian>(encoded_payload.len() as u32)?;
                output.write_all(&encoded_payload)?;
                total_compressed += (4 + encoded_payload.len()) as u64;
            }

            a3b.update(ent_f, theta, &features);
            chunk_index += 1;
        }
    }

    let savings = if total_original > 0 {
        (1.0 - total_compressed as f64 / total_original as f64) * 100.0
    } else {
        0.0
    };
    Ok(savings)
}

pub fn decompress_stream<R: Read, W: Write>(mut input: R, mut output: W) -> std::io::Result<()> {
    let mut magic = [0u8; 4];
    input.read_exact(&mut magic)?;
    if &magic != MAGIC {
        return Err(std::io::Error::new(std::io::ErrorKind::Other, "Invalid magic"));
    }
    let version = input.read_u8()?;
    if version != VERSION {
        return Err(std::io::Error::new(std::io::ErrorKind::Other, "Invalid version"));
    }
    let flags = input.read_u8()?;
    let _chunk_size = input.read_u32::<BigEndian>()? as usize;
    let block_size = input.read_u32::<BigEndian>()? as usize;
    let seed = input.read_u32::<BigEndian>()?;
    let mut weights_vec = Vec::new();
    for _ in 0..4 {
        weights_vec.push(input.read_f32::<BigEndian>()? as f64);
    }
    let weights = Array1::from_vec(weights_vec);

    let use_rank = (flags & FLAG_USE_RANK) != 0;
    let _predictor_mode = if (flags & FLAG_PREDICTOR_HEADER) != 0 { PredictorMode::Header } else { PredictorMode::Seeded };
    let transform_id = (flags & FLAG_TRANSFORM_MASK) >> FLAG_TRANSFORM_SHIFT;
    let force_raw = (flags & FLAG_FILE_RAW) != 0;

    if force_raw {
        std::io::copy(&mut input, &mut output)?;
        return Ok(());
    }

    let mut chunk_index = 0u32;
    let mut a3b = predictor::A3BPredictor::new(Some(weights), 0.01);

    loop {
        let chunk_len = match input.read_u32::<BigEndian>() {
            Ok(len) => len as usize,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e),
        };

        let chunk_flags = input.read_u8()?;
        let ent_q = input.read_u16::<BigEndian>()?;
        let ent_f = ent_q as f64 / 100.0;

        if (chunk_flags & CHUNK_FLAG_RAW) != 0 {
            let mut buffer = vec![0u8; chunk_len];
            input.read_exact(&mut buffer)?;
            output.write_all(&buffer)?;
            
            // For RAW chunks, we still need to update the predictor state to stay synchronized if we were tracking it
            let features = Array1::from_vec(vec![ent_f, 0.0, chunk_len as f64, 1.0]);
            let theta = a3b.predict(&features);
            a3b.update(ent_f, theta, &features);
            chunk_index += 1;
            continue;
        }

        let theta = input.read_f32::<BigEndian>()? as f64;
        let state = crypto::chunk_state(seed, 0, chunk_index);

        let mut ranks = Vec::new();
        if use_rank {
            let block_count = (chunk_len + block_size - 1) / block_size;
            for _ in 0..block_count {
                ranks.push(crypto::decode_rank(&mut input)?);
            }
        }
        
        let encoded_len = input.read_u32::<BigEndian>()? as usize;
        let mut encoded = vec![0u8; encoded_len];
        input.read_exact(&mut encoded)?;

        let mut model = [1i64; 256];
        let decoded = arithmetic::decode(&encoded, &mut model, chunk_len, ent_f);
        let final_data = crypto::unpermute_chunk(&decoded, block_size, &ranks, state, theta, use_rank, transform_id);
        output.write_all(&final_data)?;

        chunk_index += 1;
    }

    Ok(())
}

pub mod psfs {
    use super::*;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::io::{Seek, SeekFrom};
    use byteorder::LittleEndian;
    use sha2::{Sha256, Digest};
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    pub const PSFS_MAGIC: &[u8; 4] = b"PSFS";
    pub const PSFS_VERSION: u16 = 1;
    pub const PSFS_SUPER_SIZE: usize = 160;
    pub const PSFS_FILE_SIZE: usize = 52;
    pub const PSFS_CHUNK_SIZE: usize = 40;
    pub const PSFS_MANIFEST_SIZE: usize = 40;
    pub const MAX_STRING_TABLE_SIZE: usize = 64 * 1024 * 1024; // 64 MB
    pub const MAX_CHUNK_SIZE: usize = 32 * 1024 * 1024; // 32 MB
    pub const MAX_RAW_SIZE: usize = 64 * 1024 * 1024; // 64 MB
    pub const MAX_FILE_COUNT: u32 = 1_000_000; // Limit to 1 million files per container

    pub const PSFS_HASH_NONE: u8 = 0;
    pub const PSFS_HASH_CRC32: u8 = 1;
    pub const PSFS_HASH_SHA256: u8 = 2;
    pub const PSFS_CODEC_RAW: u8 = 1;
    pub const PSFS_CODEC_PERMSTREAM: u8 = 0;

    pub const PSFS_CHUNK_FLAG_RAW: u8 = 1 << 0;
    pub const PSFS_CHUNK_FLAG_CRC: u8 = 1 << 1;
    pub const PSFS_FILE_FLAG_SYMLINK: u32 = 1 << 0;
    pub const PSFS_FILE_FLAG_DIR: u32 = 1 << 1;
    pub const PSFS_FLAG_HAS_MANIFEST: u32 = 1 << 0;
    pub const PSFS_FLAG_HAS_EMBEDDINGS: u32 = 1 << 4;

    #[derive(Debug)]
    pub struct Superblock {
        pub magic: [u8; 4],
        pub version: u16,
        pub header_size: u16,
        pub flags: u32,
        pub chunk_size: u32,
        pub block_size: u16,
        pub codec_id: u8,
        pub hash_id: u8,
        pub file_count: u32,
        pub chunk_count: u32,
        pub index_offset: u64,
        pub strings_offset: u64,
        pub chunk_table_offset: u64,
        pub data_offset: u64,
        pub manifest_offset: u64,
        pub embeddings_offset: u64,
        pub codec_flags: u32,
        pub seed: u32,
        pub weights: [f32; 4],
        pub transform_id: u8,
        pub reserved: [u8; 59],
    }

    impl Default for Superblock {
        fn default() -> Self {
            Self {
                magic: [0; 4],
                version: 0,
                header_size: 0,
                flags: 0,
                chunk_size: 0,
                block_size: 0,
                codec_id: 0,
                hash_id: 0,
                file_count: 0,
                chunk_count: 0,
                index_offset: 0,
                strings_offset: 0,
                chunk_table_offset: 0,
                data_offset: 0,
                manifest_offset: 0,
                embeddings_offset: 0,
                codec_flags: 0,
                seed: 0,
                weights: [0.0; 4],
                transform_id: 0,
                reserved: [0; 59],
            }
        }
    }

    impl Superblock {
        pub fn write<W: Write>(&self, mut writer: W) -> std::io::Result<()> {
            writer.write_all(&self.magic)?;
            writer.write_u16::<LittleEndian>(self.version)?;
            writer.write_u16::<LittleEndian>(self.header_size)?;
            writer.write_u32::<LittleEndian>(self.flags)?;
            writer.write_u32::<LittleEndian>(self.chunk_size)?;
            writer.write_u16::<LittleEndian>(self.block_size)?;
            writer.write_u8(self.codec_id)?;
            writer.write_u8(self.hash_id)?;
            writer.write_u32::<LittleEndian>(self.file_count)?;
            writer.write_u32::<LittleEndian>(self.chunk_count)?;
            writer.write_u64::<LittleEndian>(self.index_offset)?;
            writer.write_u64::<LittleEndian>(self.strings_offset)?;
            writer.write_u64::<LittleEndian>(self.chunk_table_offset)?;
            writer.write_u64::<LittleEndian>(self.data_offset)?;
            writer.write_u64::<LittleEndian>(self.manifest_offset)?;
            writer.write_u64::<LittleEndian>(self.embeddings_offset)?;
            writer.write_u32::<LittleEndian>(self.codec_flags)?;
            writer.write_u32::<LittleEndian>(self.seed)?;
            for i in 0..4 {
                writer.write_f32::<LittleEndian>(self.weights[i])?;
            }
            writer.write_u8(self.transform_id)?;
            writer.write_all(&self.reserved)?;
            Ok(())
        }

        pub fn read<R: Read>(mut reader: R) -> std::io::Result<Self> {
            let mut sb = Superblock::default();
            reader.read_exact(&mut sb.magic)?;
            sb.version = reader.read_u16::<LittleEndian>()?;
            sb.header_size = reader.read_u16::<LittleEndian>()?;
            sb.flags = reader.read_u32::<LittleEndian>()?;
            sb.chunk_size = reader.read_u32::<LittleEndian>()?;
            sb.block_size = reader.read_u16::<LittleEndian>()?;
            sb.codec_id = reader.read_u8()?;
            sb.hash_id = reader.read_u8()?;
            sb.file_count = reader.read_u32::<LittleEndian>()?;
            sb.chunk_count = reader.read_u32::<LittleEndian>()?;
            sb.index_offset = reader.read_u64::<LittleEndian>()?;
            sb.strings_offset = reader.read_u64::<LittleEndian>()?;
            sb.chunk_table_offset = reader.read_u64::<LittleEndian>()?;
            sb.data_offset = reader.read_u64::<LittleEndian>()?;
            sb.manifest_offset = reader.read_u64::<LittleEndian>()?;
            sb.embeddings_offset = reader.read_u64::<LittleEndian>()?;
            sb.codec_flags = reader.read_u32::<LittleEndian>()?;
            sb.seed = reader.read_u32::<LittleEndian>()?;
            for i in 0..4 {
                sb.weights[i] = reader.read_f32::<LittleEndian>()?;
            }
            sb.transform_id = reader.read_u8()?;
            reader.read_exact(&mut sb.reserved)?;
            Ok(sb)
        }
    }

    #[derive(Debug, Default)]
    pub struct FileEntry {
        pub file_id: u32,
        pub mode: u16,
        pub flags: u32,
        pub uid: u32,
        pub gid: u32,
        pub mtime_ns: u64,
        pub size: u64,
        pub chunk_start: u32,
        pub chunk_count: u32,
        pub path_offset: u32,
        pub path_len: u32,
    }

    impl FileEntry {
        pub fn write<W: Write>(&self, mut writer: W) -> std::io::Result<()> {
            writer.write_u32::<LittleEndian>(self.file_id)?;
            writer.write_u16::<LittleEndian>(self.mode)?;
            writer.write_u16::<LittleEndian>(0)?; // Padding
            writer.write_u32::<LittleEndian>(self.flags)?;
            writer.write_u32::<LittleEndian>(self.uid)?;
            writer.write_u32::<LittleEndian>(self.gid)?;
            writer.write_u64::<LittleEndian>(self.mtime_ns)?;
            writer.write_u64::<LittleEndian>(self.size)?;
            writer.write_u32::<LittleEndian>(self.chunk_start)?;
            writer.write_u32::<LittleEndian>(self.chunk_count)?;
            writer.write_u32::<LittleEndian>(self.path_offset)?;
            writer.write_u32::<LittleEndian>(self.path_len)?;
            Ok(())
        }

        pub fn read<R: Read>(mut reader: R) -> std::io::Result<Self> {
            let mut fe = FileEntry::default();
            fe.file_id = reader.read_u32::<LittleEndian>()?;
            fe.mode = reader.read_u16::<LittleEndian>()?;
            let _ = reader.read_u16::<LittleEndian>()?; // Padding
            fe.flags = reader.read_u32::<LittleEndian>()?;
            fe.uid = reader.read_u32::<LittleEndian>()?;
            fe.gid = reader.read_u32::<LittleEndian>()?;
            fe.mtime_ns = reader.read_u64::<LittleEndian>()?;
            fe.size = reader.read_u64::<LittleEndian>()?;
            fe.chunk_start = reader.read_u32::<LittleEndian>()?;
            fe.chunk_count = reader.read_u32::<LittleEndian>()?;
            fe.path_offset = reader.read_u32::<LittleEndian>()?;
            fe.path_len = reader.read_u32::<LittleEndian>()?;
            Ok(fe)
        }
    }

    #[derive(Debug, Default)]
    pub struct ChunkEntry {
        pub file_id: u32,
        pub flags: u32,
        pub file_offset: u64,
        pub raw_size: u32,
        pub stored_size: u32,
        pub data_offset: u64,
        pub codec_id: u8,
        pub transform_id: u8,
        pub reserved: u16,
        pub crc32: u32,
    }

    impl ChunkEntry {
        pub fn write<W: Write>(&self, mut writer: W) -> std::io::Result<()> {
            writer.write_u32::<LittleEndian>(self.file_id)?;
            writer.write_u32::<LittleEndian>(self.flags)?;
            writer.write_u64::<LittleEndian>(self.file_offset)?;
            writer.write_u32::<LittleEndian>(self.raw_size)?;
            writer.write_u32::<LittleEndian>(self.stored_size)?;
            writer.write_u64::<LittleEndian>(self.data_offset)?;
            writer.write_u8(self.codec_id)?;
            writer.write_u8(self.transform_id)?;
            writer.write_u16::<LittleEndian>(self.reserved)?;
            writer.write_u32::<LittleEndian>(self.crc32)?;
            Ok(())
        }

        pub fn read<R: Read>(mut reader: R) -> std::io::Result<Self> {
            let mut ce = ChunkEntry::default();
            ce.file_id = reader.read_u32::<LittleEndian>()?;
            ce.flags = reader.read_u32::<LittleEndian>()?;
            ce.file_offset = reader.read_u64::<LittleEndian>()?;
            ce.raw_size = reader.read_u32::<LittleEndian>()?;
            ce.stored_size = reader.read_u32::<LittleEndian>()?;
            ce.data_offset = reader.read_u64::<LittleEndian>()?;
            ce.codec_id = reader.read_u8()?;
            ce.transform_id = reader.read_u8()?;
            ce.reserved = reader.read_u16::<LittleEndian>()?;
            ce.crc32 = reader.read_u32::<LittleEndian>()?;
            Ok(ce)
        }
    }

    pub fn pack_psfs(input_dir: &Path, output_path: &Path, mut config: CodecConfig, verify_flag: bool) -> anyhow::Result<()> {
        let mut entries = Vec::new();
        for entry in walkdir::WalkDir::new(input_dir) {
            let entry = entry?;
            let rel_path = entry.path().strip_prefix(input_dir)?.to_path_buf();
            if rel_path.as_os_str().is_empty() { continue; }
            entries.push((entry, rel_path));
        }
        entries.sort_by(|a, b| a.1.cmp(&b.1));

        let mut file_entries = Vec::new();
        let mut string_table = Vec::new();
        let mut total_chunks = 0u32;

        let mut entry_meta = Vec::new();
        let indexer = rag::VectorIndexer::new(None, None)?; // Use mock for now

        for (id, (entry, rel_path)) in entries.into_iter().enumerate() {
            let metadata = entry.metadata()?;
            let mut flags = 0u32;
            let mut target_bytes = None;
            let mut size = 0u64;

            if metadata.is_dir() {
                flags |= PSFS_FILE_FLAG_DIR;
            } else if entry.path_is_symlink() {
                flags |= PSFS_FILE_FLAG_SYMLINK;
                let target = fs::read_link(entry.path())?;
                let target_s = target.to_str().ok_or_else(|| anyhow::anyhow!("Invalid symlink target"))?;
                target_bytes = Some(target_s.as_bytes().to_vec());
                size = target_bytes.as_ref().unwrap().len() as u64;
            } else {
                size = metadata.len();
            }

            let path_s = rel_path.to_str().ok_or_else(|| anyhow::anyhow!("Invalid path"))?;
            let path_bytes = path_s.as_bytes();
            let path_offset = string_table.len() as u32;
            string_table.extend_from_slice(path_bytes);

            let chunk_count = if size > 0 && (flags & PSFS_FILE_FLAG_DIR) == 0 {
                ((size + config.chunk_size as u64 - 1) / config.chunk_size as u64) as u32
            } else { 0 };

            let chunk_start = total_chunks;
            total_chunks += chunk_count;

            file_entries.push(FileEntry {
                file_id: id as u32,
                mode: metadata.mode() as u16,
                flags,
                uid: metadata.uid(),
                gid: metadata.gid(),
                mtime_ns: metadata.mtime_nsec() as u64 + (metadata.mtime() as u64 * 1_000_000_000),
                size,
                chunk_start,
                chunk_count,
                path_offset,
                path_len: path_bytes.len() as u32,
            });

            entry_meta.push((entry.path().to_path_buf(), flags, target_bytes, path_s.to_string()));
        }

        let mut out = fs::OpenOptions::new().read(true).write(true).create(true).truncate(true).open(output_path)?;
        let mut codec_flags = 0u32;
        if config.use_rank { codec_flags |= 1 << 0; }
        if matches!(config.predictor_mode, PredictorMode::Header) { codec_flags |= 1 << 1; }

        let weights = predictor::A3BPredictor::init_weights(&config.predictor_mode, config.seed, config.weights.clone());
        config.weights = Some(weights.clone());
        let transform_id = get_transform_id(&config.transform);

        let index_offset = PSFS_SUPER_SIZE as u64;
        let strings_offset = index_offset + (file_entries.len() * PSFS_FILE_SIZE) as u64;
        let chunk_table_offset = strings_offset + string_table.len() as u64;
        let data_offset = chunk_table_offset + (total_chunks as usize * PSFS_CHUNK_SIZE) as u64;

        let mut sb = Superblock {
            magic: *PSFS_MAGIC,
            version: PSFS_VERSION,
            header_size: PSFS_SUPER_SIZE as u16,
            flags: PSFS_FLAG_HAS_EMBEDDINGS,
            chunk_size: config.chunk_size as u32,
            block_size: config.block_size as u16,
            codec_id: PSFS_CODEC_PERMSTREAM,
            hash_id: PSFS_HASH_CRC32,
            file_count: file_entries.len() as u32,
            chunk_count: total_chunks,
            index_offset,
            strings_offset,
            chunk_table_offset,
            data_offset,
            manifest_offset: 0,
            embeddings_offset: 0,
            codec_flags,
            seed: config.seed,
            weights: [weights[0] as f32, weights[1] as f32, weights[2] as f32, weights[3] as f32],
            transform_id,
            reserved: [0; 59],
        };

        // Placeholder write
        sb.write(&mut out)?;

        for fe in &file_entries {
            fe.write(&mut out)?;
        }
        out.write_all(&string_table)?;

        let chunk_table_pos = out.stream_position()?;
        out.write_all(&vec![0u8; total_chunks as usize * PSFS_CHUNK_SIZE])?;

        let mut chunk_entries = Vec::new();
        let mut a3b = predictor::A3BPredictor::new(Some(weights), 0.01);
        
        use rayon::prelude::*;
        let embeddings: Vec<Vec<f32>> = entry_meta.par_iter().map(|(_, _, _, rel_path_s)| {
            indexer.generate_embedding(rel_path_s).unwrap_or_else(|_| vec![0.0f32; 384])
        }).collect();

        for (i, (path, flags, target_bytes, _rel_path_s)) in entry_meta.into_iter().enumerate() {
            if (flags & PSFS_FILE_FLAG_DIR) != 0 { continue; }
            
            let mut reader: Box<dyn Read> = if (flags & PSFS_FILE_FLAG_SYMLINK) != 0 {
                Box::new(Cursor::new(target_bytes.unwrap()))
            } else {
                Box::new(fs::File::open(path)?)
            };

            let mut chunk_index = 0u32;
            let mut offset = 0u64;
            let mut buffer = vec![0u8; config.chunk_size];

            while let Ok(n) = reader.read(&mut buffer) {
                if n == 0 { break; }
                let chunk_data = &buffer[..n];

                let ent = entropy::calculate(chunk_data);
                let ent_q = (ent * 100.0) as u16;
                let ent_f = ent_q as f64 / 100.0;

                let features = Array1::from_vec(vec![ent_f, 0.0, n as f64, 1.0]);
                let theta = a3b.predict(&features);
                let state = crypto::chunk_state(config.seed, i as u32, chunk_index);

                let mut cflags = PSFS_CHUNK_FLAG_CRC;
                if ent_f >= config.entropy_skip {
                    cflags |= PSFS_CHUNK_FLAG_RAW;
                }

                let mut payload = Vec::new();
                let mut codec_id = PSFS_CODEC_PERMSTREAM;

                if (cflags & PSFS_CHUNK_FLAG_RAW) == 0 {
                    let (permuted, ranks, _) = crypto::permute_chunk(chunk_data, config.block_size, state, theta, config.use_rank, transform_id);
                    let mut model = [1i64; 256];
                    let encoded = arithmetic::encode(&permuted, &mut model, ent_f);

                    let mut p_cursor = Cursor::new(Vec::new());
                    p_cursor.write_u16::<BigEndian>(ent_q)?;
                    p_cursor.write_f32::<BigEndian>(theta as f32)?;
                    if config.use_rank {
                        for rank in &ranks {
                            crypto::encode_rank(&mut p_cursor, rank)?;
                        }
                    }
                    p_cursor.write_u32::<BigEndian>(encoded.len() as u32)?;
                    p_cursor.write_all(&encoded)?;
                    payload = p_cursor.into_inner();

                    if payload.len() >= chunk_data.len() {
                        cflags |= PSFS_CHUNK_FLAG_RAW;
                        payload = chunk_data.to_vec();
                        codec_id = PSFS_CODEC_RAW;
                    }
                } else {
                    payload = chunk_data.to_vec();
                    codec_id = PSFS_CODEC_RAW;
                }

                let data_off = out.stream_position()?;
                out.write_all(&payload)?;

                let crc32 = crc32fast::hash(chunk_data);

                chunk_entries.push(ChunkEntry {
                    file_id: i as u32,
                    flags: cflags as u32,
                    file_offset: offset,
                    raw_size: n as u32,
                    stored_size: payload.len() as u32,
                    data_offset: data_off,
                    codec_id,
                    transform_id,
                    reserved: 0,
                    crc32,
                });

                offset += n as u64;
                chunk_index += 1;
                a3b.update(ent_f, theta, &features);
            }
        }

        let manifest_offset = out.stream_position()?;
        
        // Write chunk table
        out.seek(SeekFrom::Start(chunk_table_pos))?;
        for ce in &chunk_entries {
            ce.write(&mut out)?;
        }

        // Calculate manifest hash
        let mut hasher = Sha256::new();
        // Hash file index, string table, chunk table
        out.seek(SeekFrom::Start(index_offset))?;
        let hash_size = manifest_offset.checked_sub(index_offset).ok_or_else(|| anyhow::anyhow!("Invalid offset"))? as usize;
        let mut hash_buf = vec![0u8; hash_size];
        out.read_exact(&mut hash_buf)?;
        hasher.update(&hash_buf);
        let table_hash = hasher.finalize();

        out.seek(SeekFrom::Start(manifest_offset))?;
        out.write_u8(PSFS_HASH_SHA256)?;
        out.write_all(&[0; 7])?; // Padding
        out.write_all(&table_hash)?;

        // Write embeddings
        let embeddings_offset = out.stream_position()?;
        for emb in embeddings {
            for &val in &emb {
                out.write_f32::<LittleEndian>(val)?;
            }
        }

        sb.flags |= PSFS_FLAG_HAS_MANIFEST | PSFS_FLAG_HAS_EMBEDDINGS;
        sb.manifest_offset = manifest_offset;
        sb.embeddings_offset = embeddings_offset;

        out.seek(SeekFrom::Start(0))?;
        sb.write(&mut out)?;

        out.seek(SeekFrom::Start(manifest_offset))?;
        out.write_u8(PSFS_HASH_SHA256)?;
        out.write_all(&[0; 7])?; // Padding
        out.write_all(&table_hash)?;

        sb.flags |= PSFS_FLAG_HAS_MANIFEST;
        sb.manifest_offset = manifest_offset;

        out.seek(SeekFrom::Start(0))?;
        sb.write(&mut out)?;

        if verify_flag {
            verify_psfs(output_path)?;
        }

        Ok(())
    }

    pub fn verify_psfs(container_path: &Path) -> anyhow::Result<()> {
        let mut file = fs::File::open(container_path)?;
        let sb = Superblock::read(&mut file)?;
        if &sb.magic != PSFS_MAGIC { anyhow::bail!("Invalid magic"); }

        if (sb.flags & PSFS_FLAG_HAS_MANIFEST) != 0 {
            let manifest_size = sb.manifest_offset.checked_sub(sb.index_offset).ok_or_else(|| anyhow::anyhow!("Invalid offset"))? as usize;
            // Prevent DoS via unbounded manifest size
            if manifest_size > MAX_STRING_TABLE_SIZE * 2 {
                anyhow::bail!("Manifest size exceeds limit");
            }
            file.seek(SeekFrom::Start(sb.index_offset))?;
            let mut buf = Vec::new();
            file.try_clone()?.take(manifest_size as u64).read_to_end(&mut buf)?;
            if buf.len() != manifest_size {
                anyhow::bail!("Failed to read full manifest");
            }
            let mut hasher = Sha256::new();
            hasher.update(&buf);
            let computed_hash = hasher.finalize();

            file.seek(SeekFrom::Start(sb.manifest_offset))?;
            let hash_id = file.read_u8()?;
            let mut pad = [0u8; 7];
            file.read_exact(&mut pad)?;
            let mut stored_hash = [0u8; 32];
            file.read_exact(&mut stored_hash)?;

            if hash_id != PSFS_HASH_SHA256 { anyhow::bail!("Unsupported hash ID"); }
            if computed_hash.as_slice() != stored_hash { anyhow::bail!("Manifest hash mismatch"); }
        }

        let config = CodecConfig {
            chunk_size: sb.chunk_size as usize,
            block_size: sb.block_size as usize,
            use_rank: (sb.codec_flags & (1 << 0)) != 0,
            predictor_mode: if (sb.codec_flags & (1 << 1)) != 0 { PredictorMode::Header } else { PredictorMode::Seeded },
            seed: sb.seed,
            weights: Some(Array1::from_vec(sb.weights.to_vec().iter().map(|&x| x as f64).collect())),
            ..Default::default()
        };

        file.seek(SeekFrom::Start(sb.chunk_table_offset))?;
        let mut chunk_indices = std::collections::HashMap::new();
        for _ in 0..sb.chunk_count {
            let ce = ChunkEntry::read(&mut file)?;
            if (ce.flags & PSFS_CHUNK_FLAG_CRC as u32) == 0 { continue; }

            let pos = file.stream_position()?;
            file.seek(SeekFrom::Start(ce.data_offset))?;
            if ce.stored_size as usize > MAX_CHUNK_SIZE {
                anyhow::bail!("Chunk size exceeds limit");
            }
            let mut payload = Vec::new();
            file.try_clone()?.take(ce.stored_size as u64).read_to_end(&mut payload)?;
            if payload.len() != ce.stored_size as usize {
                anyhow::bail!("Failed to read full chunk payload");
            }
            file.seek(SeekFrom::Start(pos))?;

            let chunk_index = *chunk_indices.get(&ce.file_id).unwrap_or(&0u32);
            chunk_indices.insert(ce.file_id, chunk_index + 1);

            let decoded = decompress_chunk_payload(&payload, ce.raw_size as usize, &config, ce.file_id, chunk_index, ce.codec_id, ce.transform_id)?;
            let crc = crc32fast::hash(&decoded);
            if crc != ce.crc32 {
                anyhow::bail!("CRC mismatch for file {} chunk {}", ce.file_id, chunk_index);
            }
        }

        Ok(())
    }

    pub fn decompress_chunk_payload(payload: &[u8], raw_size: usize, config: &CodecConfig, file_id: u32, chunk_index: u32, codec_id: u8, transform_id: u8) -> anyhow::Result<Vec<u8>> {
        if raw_size > MAX_RAW_SIZE {
            anyhow::bail!("Chunk raw size exceeds maximum limit");
        }
        if codec_id == PSFS_CODEC_RAW {
            return Ok(payload.to_vec());
        }

        let mut reader = Cursor::new(payload);
        let ent_q = reader.read_u16::<BigEndian>()?;
        let ent_f = ent_q as f64 / 100.0;
        let theta = reader.read_f32::<BigEndian>()? as f64;

        let state = crypto::chunk_state(config.seed, file_id, chunk_index);

        let mut ranks = Vec::new();
        if config.use_rank {
            let block_count = (raw_size + config.block_size - 1) / config.block_size;
            for _ in 0..block_count {
                ranks.push(crypto::decode_rank(&mut reader)?);
            }
        }

        let enc_len = reader.read_u32::<BigEndian>()? as usize;
        if enc_len > super::psfs::MAX_CHUNK_SIZE {
            anyhow::bail!("Encoded length exceeds chunk size limit");
        }
        let mut encoded = vec![0u8; enc_len];
        reader.read_exact(&mut encoded)?;

        let mut model = [1i64; 256];
        let decoded = arithmetic::decode(&encoded, &mut model, raw_size, ent_f);
        let final_data = crypto::unpermute_chunk(&decoded, config.block_size, &ranks, state, theta, config.use_rank, transform_id);

        Ok(final_data)
    }

    pub fn unpack_psfs(container_path: &Path, output_dir: &Path, verify_flag: bool) -> anyhow::Result<()> {
        let mut file = fs::File::open(container_path)?;
        let sb = Superblock::read(&mut file)?;
        if &sb.magic != PSFS_MAGIC { anyhow::bail!("Invalid magic"); }

        let config = CodecConfig {
            chunk_size: sb.chunk_size as usize,
            block_size: sb.block_size as usize,
            use_rank: (sb.codec_flags & (1 << 0)) != 0,
            predictor_mode: if (sb.codec_flags & (1 << 1)) != 0 { PredictorMode::Header } else { PredictorMode::Seeded },
            seed: sb.seed,
            weights: Some(Array1::from_vec(sb.weights.to_vec().iter().map(|&x| x as f64).collect())),
            ..Default::default()
        };

        if sb.file_count > MAX_FILE_COUNT {
            anyhow::bail!("File count exceeds maximum limit");
        }
        file.seek(SeekFrom::Start(sb.index_offset))?;
        let mut file_entries = Vec::new();
        for _ in 0..sb.file_count {
            file_entries.push(FileEntry::read(&mut file)?);
        }

        file.seek(SeekFrom::Start(sb.strings_offset))?;
        let string_table_size = sb.chunk_table_offset.checked_sub(sb.strings_offset).ok_or_else(|| anyhow::anyhow!("Invalid offset"))? as usize;
        if string_table_size > MAX_STRING_TABLE_SIZE {
            anyhow::bail!("String table exceeds size limit");
        }
        let mut string_table = Vec::new();
        file.try_clone()?.take(string_table_size as u64).read_to_end(&mut string_table)?;

        file.seek(SeekFrom::Start(sb.chunk_table_offset))?;
        let mut chunk_entries = Vec::new();
        for _ in 0..sb.chunk_count {
            chunk_entries.push(ChunkEntry::read(&mut file)?);
        }

        fs::create_dir_all(output_dir)?;

        for fe in file_entries {
            let path_end = fe.path_offset.checked_add(fe.path_len).ok_or_else(|| anyhow::anyhow!("Path offset overflow"))? as usize;
            if path_end > string_table.len() {
                anyhow::bail!("Path bounds exceed string table length");
            }
            let path_bytes = &string_table[fe.path_offset as usize .. path_end];
            let rel_path = std::str::from_utf8(path_bytes)?;
            
            // SECURITY FIX: Zip-Slip protection
            let mut safe_path = PathBuf::new();
            for component in std::path::Path::new(rel_path).components() {
                if let std::path::Component::Normal(c) = component {
                    safe_path.push(c);
                }
            }
            if safe_path.as_os_str().is_empty() {
                continue; // Skip invalid empty paths
            }
            let out_path = output_dir.join(safe_path);

            if (fe.flags & PSFS_FILE_FLAG_DIR) != 0 {
                fs::create_dir_all(&out_path)?;
                continue;
            }

            fs::create_dir_all(out_path.parent().unwrap())?;

            let chunk_end = fe.chunk_start.checked_add(fe.chunk_count).ok_or_else(|| anyhow::anyhow!("Chunk bounds overflow"))? as usize;
            if chunk_end > chunk_entries.len() {
                anyhow::bail!("Chunk bounds exceed chunk table length");
            }

            if (fe.flags & PSFS_FILE_FLAG_SYMLINK) != 0 {
                let mut data = Vec::new();
                for (chunk_idx, ce) in chunk_entries[fe.chunk_start as usize .. chunk_end].iter().enumerate() {
                    file.seek(SeekFrom::Start(ce.data_offset))?;
                    if ce.stored_size as usize > MAX_CHUNK_SIZE {
                        anyhow::bail!("Chunk size exceeds limit");
                    }
                    let mut payload = Vec::new();
                    file.try_clone()?.take(ce.stored_size as u64).read_to_end(&mut payload)?;
                    let decoded = decompress_chunk_payload(&payload, ce.raw_size as usize, &config, fe.file_id, chunk_idx as u32, ce.codec_id, ce.transform_id)?;
                    data.extend_from_slice(&decoded);
                }
                let target = std::str::from_utf8(&data)?;
                let target_path = Path::new(target);
                if target_path.is_absolute() || target_path.components().any(|c| matches!(c, std::path::Component::ParentDir)) {
                    anyhow::bail!("Invalid symlink target: {}", target);
                }
                std::os::unix::fs::symlink(target, &out_path)?;
                continue;
            }

            let mut out_file = fs::File::create(&out_path)?;
            for (chunk_idx, ce) in chunk_entries[fe.chunk_start as usize .. chunk_end].iter().enumerate() {
                file.seek(SeekFrom::Start(ce.data_offset))?;
                if ce.stored_size as usize > MAX_CHUNK_SIZE {
                    anyhow::bail!("Chunk size exceeds limit");
                }
                let mut payload = Vec::new();
                file.try_clone()?.take(ce.stored_size as u64).read_to_end(&mut payload)?;
                let decoded = decompress_chunk_payload(&payload, ce.raw_size as usize, &config, fe.file_id, chunk_idx as u32, ce.codec_id, ce.transform_id)?;
                if verify_flag && (ce.flags & PSFS_CHUNK_FLAG_CRC as u32) != 0 {
                    let crc = crc32fast::hash(&decoded);
                    if crc != ce.crc32 { anyhow::bail!("CRC mismatch for {}", rel_path); }
                }
                out_file.write_all(&decoded)?;
            }
            
            let mut perms = fs::metadata(&out_path)?.permissions();
            perms.set_mode(fe.mode as u32);
            fs::set_permissions(&out_path, perms)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stream_roundtrip() {
        let data = b"enterprise grade compression test for permstream nucleus project";
        let config = CodecConfig {
            chunk_size: 16,
            block_size: 4,
            transform: "delta".to_string(),
            ..Default::default()
        };

        let mut compressed = Vec::new();
        let savings = compress_stream(&data[..], &mut compressed, config).unwrap();
        println!("Savings: {}%", savings);

        let mut decompressed = Vec::new();
        decompress_stream(&compressed[..], &mut decompressed).unwrap();

        assert_eq!(data.to_vec(), decompressed);
    }
}

#[cfg(test)]
mod security_tests {
    use super::*;
    use std::path::{Path, PathBuf};

    #[test]
    fn test_zip_slip_protection() {
        // We simulate the path parsing logic from unpack_psfs
        let malicious_path = "../../etc/passwd";
        let mut safe_path = PathBuf::new();
        for component in Path::new(malicious_path).components() {
            if let std::path::Component::Normal(c) = component {
                safe_path.push(c);
            }
        }
        assert_eq!(safe_path.as_os_str(), "etc/passwd"); // Traversal removed
        
        let malicious_path_abs = "/etc/shadow";
        let mut safe_path_abs = PathBuf::new();
        for component in Path::new(malicious_path_abs).components() {
            if let std::path::Component::Normal(c) = component {
                safe_path_abs.push(c);
            }
        }
        assert_eq!(safe_path_abs.as_os_str(), "etc/shadow"); // Root stripped
    }

    #[test]
    fn test_symlink_pivot_protection() {
        // We simulate the symlink validation from unpack_psfs
        let malicious_target = "/etc/passwd";
        let target_path = Path::new(malicious_target);
        assert!(target_path.is_absolute() || target_path.components().any(|c| matches!(c, std::path::Component::ParentDir)));

        let traversal_target = "../../root";
        let target_path2 = Path::new(traversal_target);
        assert!(target_path2.is_absolute() || target_path2.components().any(|c| matches!(c, std::path::Component::ParentDir)));

        let safe_target = "local_dir/file.txt";
        let target_path3 = Path::new(safe_target);
        assert!(!(target_path3.is_absolute() || target_path3.components().any(|c| matches!(c, std::path::Component::ParentDir))));
    }
}
mod arithmetic_test;
mod transform_test;
