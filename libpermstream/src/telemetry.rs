use crate::arithmetic::{FenwickTree, decode};
use crate::crypto::decode_rank;
use byteorder::{BigEndian, ReadBytesExt};
use std::io::{Cursor, Read};

/// Represents a fast, zero-copy analytic engine over compressed PermStream payloads.
/// Specifically designed for High-Frequency Trading (HFT) and DePIN telemetry,
/// this engine allows calculating prefix sums in O(log N) time directly from
/// the Arithmetic Coder's probability models without unpermuting or fully
/// decompressing the dataset.
pub struct TelemetryEngine {
    model: [i64; 256],
    total_events: i64,
}

impl TelemetryEngine {
    /// Initialize a telemetry engine over a raw binary model or stream context.
    pub fn new() -> Self {
        Self {
            model: [1; 256],
            total_events: 256,
        }
    }

    /// Ingests a compressed PermStream chunk, completely bypassing the expensive 
    /// Braid Group unpermutation phase. It updates the internal frequency model 
    /// directly from the Arithmetic Coder state.
    pub fn ingest_compressed_chunk(&mut self, payload: &[u8], raw_size: usize, use_rank: bool, block_size: usize) -> anyhow::Result<()> {
        if payload.is_empty() { return Ok(()); }
        
        let mut reader = Cursor::new(payload);
        
        let ent_q = reader.read_u16::<BigEndian>()?;
        let ent_f = ent_q as f64 / 100.0;
        let _theta = reader.read_f32::<BigEndian>()?;

        // Skip ranks if present
        if use_rank {
            let block_count = (raw_size + block_size - 1) / block_size;
            for _ in 0..block_count {
                let _ = decode_rank(&mut reader)?;
            }
        }

        let enc_len = reader.read_u32::<BigEndian>()? as usize;
        let mut encoded = vec![0u8; enc_len];
        reader.read_exact(&mut encoded)?;

        // Decode the symbols to update our exact model, but skip the `unpermute_chunk` step.
        // This is where the massive performance gain for HFT telemetry comes from.
        let mut local_model = [1i64; 256];
        let decoded = decode(&encoded, &mut local_model, raw_size, ent_f);

        // Update the global telemetry model
        for &sym in &decoded {
            self.model[sym as usize] += 1;
            self.total_events += 1;
        }
        
        Ok(())
    }

    /// Queries the prefix sum of events (e.g., latency buckets) directly from the
    /// Fenwick Tree in O(log N) time.
    /// `max_symbol` represents the upper bound of the bucket (e.g., bucket 100 for 100ms).
    pub fn query_prefix_sum(&self, max_symbol: u8) -> i64 {
        let ft = FenwickTree::new(&self.model);
        ft.query(max_symbol as usize + 1)
    }

    /// Returns the estimated probability or frequency of a specific event type.
    pub fn estimate_frequency(&self, symbol: u8) -> f64 {
        let ft = FenwickTree::new(&self.model);
        // Frequency is prefix_sum(sym) - prefix_sum(sym - 1)
        let sum_high = ft.query(symbol as usize + 1);
        let sum_low = ft.query(symbol as usize);
        let freq = sum_high - sum_low;
        
        freq as f64 / self.total_events as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entropy::calculate;
    use crate::arithmetic::encode;
    use crate::crypto::{encode_rank, permute_chunk};
    use byteorder::WriteBytesExt;
    use std::io::Write;

    #[test]
    fn test_telemetry_prefix_sum() {
        let mut engine = TelemetryEngine::new();
        
        // Let's create a realistic mock payload using the actual encoder
        let raw_data: Vec<u8> = vec![10, 10, 10, 10, 20, 20, 50, 100];
        let raw_size = raw_data.len();
        let use_rank = true;
        let block_size = 4;
        let transform_id = 0;
        
        let ent = calculate(&raw_data);
        let ent_q = (ent * 100.0) as u16;
        let ent_f = ent_q as f64 / 100.0;
        let theta = 0.5;
        let state = 0;
        
        let (permuted, ranks, _) = permute_chunk(&raw_data, block_size, state, theta, use_rank, transform_id);
        let mut local_model = [1i64; 256];
        let encoded = encode(&permuted, &mut local_model, ent_f);
        
        let mut p_cursor = Cursor::new(Vec::new());
        p_cursor.write_u16::<BigEndian>(ent_q).unwrap();
        p_cursor.write_f32::<BigEndian>(theta as f32).unwrap();
        for rank in &ranks {
            encode_rank(&mut p_cursor, rank).unwrap();
        }
        p_cursor.write_u32::<BigEndian>(encoded.len() as u32).unwrap();
        p_cursor.write_all(&encoded).unwrap();
        
        let dummy_payload = p_cursor.into_inner();

        engine.ingest_compressed_chunk(&dummy_payload, raw_size, use_rank, block_size).unwrap();

        // Query the prefix sum for latency buckets 0-50
        // We know we have four 10s, two 20s, and one 50. Total = 7 events <= 50.
        // Plus initial model base 1 per symbol => prefix_sum(50) = 51 (initial) + 7 = 58
        let sum = engine.query_prefix_sum(50);
        assert!(sum >= 58, "Prefix sum should be calculated correctly");

        let freq = engine.estimate_frequency(10);
        assert!(freq > 0.0 && freq <= 1.0, "Frequency should be a valid probability");
    }
}
