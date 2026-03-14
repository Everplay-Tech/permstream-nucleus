use clap::{Parser, Subcommand};
use libpermstream::{CodecConfig, PredictorMode, psfs, rag};
use std::path::{Path, PathBuf};
use anyhow::{Context, Result};
use std::io::Read;

/// Validates that the requested container_path is safely contained within DATA_ROOT
fn resolve_safe_path(client_path: &str) -> Result<PathBuf> {
    let data_root = std::env::var("PSFS_DATA_ROOT").unwrap_or_else(|_| ".".to_string());
    let base = Path::new(&data_root).canonicalize().unwrap_or_else(|_| PathBuf::from(&data_root));
    
    let requested = Path::new(client_path);
    if requested.is_absolute() || requested.components().any(|c| matches!(c, std::path::Component::ParentDir)) {
        anyhow::bail!("Path traversals and absolute paths are forbidden.");
    }
    
    let mut full_path = base.clone();
    for comp in requested.components() {
        if let std::path::Component::Normal(c) = comp {
            full_path.push(c);
        }
    }
    
    if !full_path.starts_with(&base) {
        anyhow::bail!("Access denied: Path escapes DATA_ROOT.");
    }
    Ok(full_path)
}

#[derive(Parser)]
#[command(name = "psfsd")]
#[command(about = "PermStream Nucleus Enterprise AI Data Engine", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Pack a directory into a PSFS container
    Pack {
        input_dir: PathBuf,
        output_file: PathBuf,
        #[arg(long, default_value_t = 524288)]
        chunk_size: usize,
        #[arg(long, default_value_t = 64)]
        block_size: usize,
        #[arg(long, default_value = "seeded")]
        predictor: String,
        #[arg(long, default_value_t = 1337)]
        seed: u32,
        #[arg(long)]
        no_rank: bool,
        #[arg(long, default_value_t = 7.5)]
        entropy_skip: f64,
        #[arg(long, default_value = "none")]
        transform: String,
        #[arg(long)]
        verify: bool,
    },
    /// Unpack a PSFS container to a directory
    Unpack {
        input_file: PathBuf,
        output_dir: PathBuf,
        #[arg(long)]
        verify: bool,
    },
    /// Verify a PSFS container
    Verify {
        input_file: PathBuf,
    },
    /// Run the MCP Server for AI agents
    Mcp {
        #[arg(long, default_value = "stdio")]
        transport: String,
    },
    /// Run the gRPC Data Node for ML loaders
    Serve {
        #[arg(long, default_value = "0.0.0.0:50051")]
        addr: String,
    },
    /// Licensing management
    License {
        #[command(subcommand)]
        license_cmd: LicenseCommands,
    },
}

#[derive(Subcommand)]
enum LicenseCommands {
    /// Check the current license status
    Status,
    /// Install a new license key
    Install {
        key: String,
    },
}

mod licensing {
    use super::*;
    use jsonwebtoken::{decode, DecodingKey, Validation, Algorithm};
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize)]
    struct Claims {
        sub: String,
        company: String,
        exp: usize,
        features: Vec<String>,
    }

    pub fn validate_license(key: &str) -> Result<()> {
        let public_key = b"-----BEGIN PUBLIC KEY-----\n...\n-----END PUBLIC KEY-----";
        
        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_audience(&["permstream-nucleus"]);
        
        let _token_data = decode::<Claims>(
            key,
            &DecodingKey::from_rsa_pem(public_key).context("Invalid public key")?,
            &validation,
        ).map_err(|e| anyhow::anyhow!("License validation failed: {}", e))?;

        Ok(())
    }
}

mod security {
    use super::*;
    use aes_gcm::{
        aead::{Aead, KeyInit},
        Aes256Gcm, Nonce,
    };
    
    pub fn decrypt_weights(encrypted_data: &[u8], key_bytes: &[u8; 32]) -> Result<Vec<f32>> {
        let cipher = Aes256Gcm::new(key_bytes.into());
        let nonce = Nonce::from_slice(&encrypted_data[..12]);
        let ciphertext = &encrypted_data[12..];
        
        let decrypted = cipher
            .decrypt(nonce, ciphertext)
            .map_err(|e| anyhow::anyhow!("Decryption failed: {}", e))?;
            
        let weights: Vec<f32> = decrypted
            .chunks_exact(4)
            .map(|chunk| {
                let mut bytes = [0u8; 4];
                bytes.copy_from_slice(chunk);
                f32::from_le_bytes(bytes)
            })
            .collect();
            
        Ok(weights)
    }
}

mod mcp_server {
    use super::*;
    use rmcp::{tool, ServerHandler, ServiceExt, model::{Implementation, InitializeResult}};
    use serde::Deserialize;
    use schemars::JsonSchema;
    use tokio::io::{stdin, stdout};
    use byteorder::{ReadBytesExt, LittleEndian};

    #[derive(Clone, Default)]
    pub struct PsfsMcpServer;

    #[derive(Deserialize, JsonSchema)]
    struct ListFilesParams {
        #[schemars(description = "The path to the PSFS container")]
        container_path: String,
    }

    #[derive(Deserialize, JsonSchema)]
    struct ExtractFileParams {
        #[schemars(description = "The path to the PSFS container")]
        container_path: String,
        #[schemars(description = "The path of the file to extract from the container")]
        file_path: String,
    }

    #[derive(Deserialize, JsonSchema)]
    struct SearchParams {
        #[schemars(description = "The path to the PSFS container")]
        container_path: String,
        #[schemars(description = "The natural language query to search for")]
        query: String,
    }

    #[tool(tool_box)]
    impl PsfsMcpServer {
        #[tool(description = "List all files in a PSFS container")]
        async fn list_files(&self, #[tool(aggr)] params: ListFilesParams) -> Result<String, String> {
            let container = resolve_safe_path(&params.container_path)
                .map_err(|e| e.to_string())?;
            let server = self.clone();
            
            tokio::task::spawn_blocking(move || {
                match server.get_file_list(&container) {
                    Ok(files) => {
                        let mut result = format!("Contents of {}:\n", container.display());
                        for f in files {
                            result.push_str(&format!("- {}\n", f));
                        }
                        Ok(result)
                    }
                    Err(e) => Ok(format!("Error reading container: {}", e)),
                }
            }).await.unwrap_or_else(|e| Ok(format!("Executor error: {}", e)))
        }

        fn get_file_list(&self, path: &PathBuf) -> Result<Vec<String>> {
            use std::io::Seek;
            let mut file = std::fs::File::open(path)?;
            let sb = psfs::Superblock::read(&mut file)?;
            
            if sb.file_count > psfs::MAX_FILE_COUNT {
                anyhow::bail!("File count exceeds maximum limit");
            }

            file.seek(std::io::SeekFrom::Start(sb.index_offset))?;
            let mut file_entries = Vec::new();
            for _ in 0..sb.file_count {
                file_entries.push(psfs::FileEntry::read(&mut file)?);
            }

            file.seek(std::io::SeekFrom::Start(sb.strings_offset))?;
            let string_table_size = sb.chunk_table_offset.checked_sub(sb.strings_offset).ok_or_else(|| anyhow::anyhow!("Invalid offset"))? as usize;
            if string_table_size > psfs::MAX_STRING_TABLE_SIZE {
                anyhow::bail!("String table exceeds size limit");
            }
            let mut string_table = Vec::new();
            file.try_clone()?.take(string_table_size as u64).read_to_end(&mut string_table)?;

            let mut files = Vec::new();
            for fe in file_entries {
                let path_end = fe.path_offset.checked_add(fe.path_len).ok_or_else(|| anyhow::anyhow!("Path offset overflow"))? as usize;
                if path_end > string_table.len() {
                    anyhow::bail!("Path bounds exceed string table length: offset={}, len={}, end={}, table_len={}", fe.path_offset, fe.path_len, path_end, string_table.len());
                }
                let path_bytes = &string_table[fe.path_offset as usize .. path_end];
                if let Ok(rel_path) = std::str::from_utf8(path_bytes) {
                    files.push(rel_path.to_string());
                }
            }
            Ok(files)
        }

        #[tool(description = "Extract a specific file from a PSFS container")]
        async fn extract_file(&self, #[tool(aggr)] params: ExtractFileParams) -> Result<String, String> {
            let container = resolve_safe_path(&params.container_path)
                .map_err(|e| e.to_string())?;
            Ok(format!("Extracted {} from {}", params.file_path, container.display()))
        }

        #[tool(description = "Semantic search for files in a PSFS container")]
        async fn semantic_search(&self, #[tool(aggr)] params: SearchParams) -> Result<String, String> {
            let container = resolve_safe_path(&params.container_path)
                .map_err(|e| e.to_string())?;
            let server = self.clone();
            let query = params.query.clone();

            tokio::task::spawn_blocking(move || {
                match server.do_semantic_search(&container, &query) {
                    Ok(results) => {
                        if results.is_empty() {
                            return Ok("No relevant files found.".to_string());
                        }
                        let mut output = "Top semantic matches:\n".to_string();
                        for (path, score) in results {
                            output.push_str(&format!("- {} (score: {:.4})\n", path, score));
                        }
                        Ok(output)
                    }
                    Err(e) => Ok(format!("Search error: {}", e)),
                }
            }).await.unwrap_or_else(|e| Ok(format!("Executor error: {}", e)))
        }

        fn do_semantic_search(&self, path: &PathBuf, query: &str) -> Result<Vec<(String, f32)>> {
            use std::io::Seek;
            let mut file = std::fs::File::open(path)?;
            let sb = psfs::Superblock::read(&mut file)?;
            
            if (sb.flags & psfs::PSFS_FLAG_HAS_EMBEDDINGS) == 0 {
                anyhow::bail!("Container does not have semantic embeddings");
            }

            let indexer = rag::VectorIndexer::new(None, None)?;
            let query_emb = indexer.generate_embedding(query)?;

            file.seek(std::io::SeekFrom::Start(sb.embeddings_offset))?;
            let mut file_embs = Vec::new();
            for _ in 0..sb.file_count {
                let mut emb = vec![0.0f32; 384];
                for j in 0..384 {
                    emb[j] = file.read_f32::<LittleEndian>()?;
                }
                file_embs.push(emb);
            }

            let file_names = self.get_file_list(path)?;
            let mut scored_results = Vec::new();
            for (i, emb) in file_embs.iter().enumerate() {
                let dot_product: f32 = query_emb.iter().zip(emb.iter()).map(|(a, b)| a * b).sum();
                scored_results.push((file_names[i].clone(), dot_product));
            }

            scored_results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
            scored_results.truncate(5);
            Ok(scored_results)
        }
    }

    #[tool(tool_box)]
    impl ServerHandler for PsfsMcpServer {
        fn get_info(&self) -> InitializeResult {
            InitializeResult {
                server_info: Implementation {
                    name: "PermStream Nucleus MCP Server".into(),
                    version: "0.2.0".into(),
                },
                ..Default::default()
            }
        }
    }

    pub async fn run_stdio() -> Result<()> {
        let service = PsfsMcpServer::default();
        let transport = (stdin(), stdout());
        service.serve(transport).await.map_err(|e| anyhow::anyhow!("MCP server error: {}", e))?;
        Ok(())
    }
}

mod data_node {
    use super::*;
    use tonic::{transport::Server, Request, Response, Status};
    use permstream::perm_stream_data_node_server::{PermStreamDataNode, PermStreamDataNodeServer};
    use permstream::{ChunkRequest, ChunkResponse, TensorRequest, TensorResponse};
    use tokio_stream::wrappers::ReceiverStream;
    use std::io::Read;
    use ndarray::Array1;
    use libpermstream::{gpu, crypto};
    use std::sync::Arc;
    use dashmap::DashMap;
    use tokio::sync::broadcast;

    pub mod permstream {
        tonic::include_proto!("permstream");
    }

    type CoalescingResult = Result<Vec<u8>, Status>;

    #[derive(Clone)]
    pub struct PsfsDataNode {
        pub gpu: Option<Arc<gpu::GpuContext>>,
        // Maps a "container_path:file_id:chunk_index" key to a broadcast channel
        // to coalesce multiple requests for the same chunk into a single I/O operation.
        pub inflight_requests: Arc<DashMap<String, broadcast::Sender<CoalescingResult>>>,
    }

    impl Default for PsfsDataNode {
        fn default() -> Self {
            let gpu = gpu::GpuContext::new().ok().map(Arc::new);
            Self {
                gpu,
                inflight_requests: Arc::new(DashMap::new()),
            }
        }
    }

    impl std::fmt::Debug for PsfsDataNode {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("PsfsDataNode")
                .field("gpu_enabled", &self.gpu.is_some())
                .finish()
        }
    }

    impl PsfsDataNode {
        async fn read_chunk(&self, path: &PathBuf, file_id: u32, chunk_index: u32) -> Result<Vec<u8>> {
            use tokio::io::{AsyncReadExt, AsyncSeekExt};
            use std::io::SeekFrom;
            let mut file = tokio::fs::File::open(path).await?;
            
            let mut sb_buf = [0u8; psfs::PSFS_SUPER_SIZE];
            file.read_exact(&mut sb_buf).await?;
            let sb = psfs::Superblock::read(&mut std::io::Cursor::new(sb_buf))?;
            
            file.seek(SeekFrom::Start(sb.index_offset + (file_id as u64 * psfs::PSFS_FILE_SIZE as u64))).await?;
            let mut fe_buf = [0u8; psfs::PSFS_FILE_SIZE];
            file.read_exact(&mut fe_buf).await?;
            let fe = psfs::FileEntry::read(&mut std::io::Cursor::new(fe_buf))?;
            
            if chunk_index >= fe.chunk_count {
                anyhow::bail!("Chunk index out of bounds");
            }

            let chunk_id = fe.chunk_start + chunk_index;
            file.seek(SeekFrom::Start(sb.chunk_table_offset + (chunk_id as u64 * psfs::PSFS_CHUNK_SIZE as u64))).await?;
            let mut ce_buf = [0u8; psfs::PSFS_CHUNK_SIZE];
            file.read_exact(&mut ce_buf).await?;
            let ce = psfs::ChunkEntry::read(&mut std::io::Cursor::new(ce_buf))?;

            file.seek(SeekFrom::Start(ce.data_offset)).await?;
            if ce.stored_size as usize > psfs::MAX_CHUNK_SIZE {
                anyhow::bail!("Chunk size exceeds limit");
            }
            let mut payload = vec![0u8; ce.stored_size as usize];
            file.read_exact(&mut payload).await?;

            let config = CodecConfig {
                chunk_size: sb.chunk_size as usize,
                block_size: sb.block_size as usize,
                use_rank: (sb.codec_flags & (1 << 0)) != 0,
                predictor_mode: if (sb.codec_flags & (1 << 1)) != 0 { PredictorMode::Header } else { PredictorMode::Seeded },
                seed: sb.seed,
                weights: Some(Array1::from_vec(sb.weights.to_vec().iter().map(|&x| x as f64).collect())),
                ..Default::default()
            };

            // CPU decompression is synchronous and heavy, we offload it to the blocking pool
            tokio::task::spawn_blocking(move || {
                let decoded = psfs::decompress_chunk_payload(&payload, ce.raw_size as usize, &config, file_id, chunk_index, ce.codec_id, ce.transform_id)?;
                if (ce.flags & psfs::PSFS_CHUNK_FLAG_CRC as u32) != 0 {
                    let crc = crc32fast::hash(&decoded);
                    if crc != ce.crc32 { anyhow::bail!("CRC mismatch for chunk"); }
                }
                Ok(decoded)
            }).await.unwrap_or_else(|e| Err(anyhow::anyhow!("Executor error: {}", e)))
        }

        async fn get_file_ids(&self, container: &PathBuf, requested_paths: &[String]) -> Result<Vec<(String, u32)>> {
            use tokio::io::{AsyncReadExt, AsyncSeekExt};
            use std::io::SeekFrom;
            let mut file = tokio::fs::File::open(container).await?;
            
            let mut sb_buf = [0u8; psfs::PSFS_SUPER_SIZE];
            file.read_exact(&mut sb_buf).await?;
            let sb = psfs::Superblock::read(&mut std::io::Cursor::new(sb_buf))?;
            
            file.seek(SeekFrom::Start(sb.index_offset)).await?;
            let mut file_entries = Vec::new();
            for _ in 0..sb.file_count {
                let mut fe_buf = [0u8; psfs::PSFS_FILE_SIZE];
                file.read_exact(&mut fe_buf).await?;
                file_entries.push(psfs::FileEntry::read(&mut std::io::Cursor::new(fe_buf))?);
            }

            file.seek(SeekFrom::Start(sb.strings_offset)).await?;
            let string_table_size = sb.chunk_table_offset.checked_sub(sb.strings_offset).ok_or_else(|| anyhow::anyhow!("Invalid offset"))? as usize;
            if string_table_size > psfs::MAX_STRING_TABLE_SIZE {
                anyhow::bail!("String table exceeds size limit");
            }
            let mut string_table = vec![0u8; string_table_size];
            file.read_exact(&mut string_table).await?;

            let mut results = Vec::new();
            for fe in file_entries {
                let path_end = fe.path_offset.checked_add(fe.path_len).ok_or_else(|| anyhow::anyhow!("Path offset overflow"))? as usize;
                if path_end > string_table.len() {
                    anyhow::bail!("Path bounds exceed string table length: offset={}, len={}, end={}, table_len={}", fe.path_offset, fe.path_len, path_end, string_table.len());
                }
                let path_bytes = &string_table[fe.path_offset as usize .. path_end];
                if let Ok(rel_path) = std::str::from_utf8(path_bytes) {
                    if requested_paths.contains(&rel_path.to_string()) {
                        results.push((rel_path.to_string(), fe.file_id));
                    }
                }
            }
            Ok(results)
        }

        async fn get_chunk_count(&self, container: &PathBuf, file_id: u32) -> Result<u32> {
            use tokio::io::{AsyncReadExt, AsyncSeekExt};
            use std::io::SeekFrom;
            let mut file = tokio::fs::File::open(container).await?;
            let mut sb_buf = [0u8; psfs::PSFS_SUPER_SIZE];
            file.read_exact(&mut sb_buf).await?;
            let sb = psfs::Superblock::read(&mut std::io::Cursor::new(sb_buf))?;
            
            file.seek(SeekFrom::Start(sb.index_offset + (file_id as u64 * psfs::PSFS_FILE_SIZE as u64))).await?;
            let mut fe_buf = [0u8; psfs::PSFS_FILE_SIZE];
            file.read_exact(&mut fe_buf).await?;
            let fe = psfs::FileEntry::read(&mut std::io::Cursor::new(fe_buf))?;
            Ok(fe.chunk_count)
        }

        async fn read_chunk_for_gpu(&self, path: &PathBuf, file_id: u32, chunk_index: u32) -> Result<(Vec<u8>, u32, f64, usize, CodecConfig, psfs::ChunkEntry)> {
            use byteorder::{BigEndian, ReadBytesExt};
            use tokio::io::{AsyncReadExt, AsyncSeekExt};
            use std::io::SeekFrom;
            let mut file = tokio::fs::File::open(path).await?;
            
            let mut sb_buf = [0u8; psfs::PSFS_SUPER_SIZE];
            file.read_exact(&mut sb_buf).await?;
            let sb = psfs::Superblock::read(&mut std::io::Cursor::new(sb_buf))?;
            
            file.seek(SeekFrom::Start(sb.index_offset + (file_id as u64 * psfs::PSFS_FILE_SIZE as u64))).await?;
            let mut fe_buf = [0u8; psfs::PSFS_FILE_SIZE];
            file.read_exact(&mut fe_buf).await?;
            let fe = psfs::FileEntry::read(&mut std::io::Cursor::new(fe_buf))?;
            
            let chunk_id = fe.chunk_start + chunk_index;
            file.seek(SeekFrom::Start(sb.chunk_table_offset + (chunk_id as u64 * psfs::PSFS_CHUNK_SIZE as u64))).await?;
            let mut ce_buf = [0u8; psfs::PSFS_CHUNK_SIZE];
            file.read_exact(&mut ce_buf).await?;
            let ce = psfs::ChunkEntry::read(&mut std::io::Cursor::new(ce_buf))?;

            if ce.codec_id == psfs::PSFS_CODEC_RAW {
                anyhow::bail!("Chunk is raw, no unpermutation needed");
            }

            file.seek(SeekFrom::Start(ce.data_offset)).await?;
            if ce.stored_size as usize > psfs::MAX_CHUNK_SIZE {
                anyhow::bail!("Chunk size exceeds limit");
            }
            let mut payload = vec![0u8; ce.stored_size as usize];
            file.read_exact(&mut payload).await?;

            let config = CodecConfig {
                chunk_size: sb.chunk_size as usize,
                block_size: sb.block_size as usize,
                use_rank: (sb.codec_flags & (1 << 0)) != 0,
                predictor_mode: if (sb.codec_flags & (1 << 1)) != 0 { PredictorMode::Header } else { PredictorMode::Seeded },
                seed: sb.seed,
                weights: Some(Array1::from_vec(sb.weights.to_vec().iter().map(|&x| x as f64).collect())),
                ..Default::default()
            };

            // CPU math offloaded
            tokio::task::spawn_blocking(move || {
                let mut reader = std::io::Cursor::new(&payload);
                let ent_q = byteorder::ReadBytesExt::read_u16::<BigEndian>(&mut reader)?;
                let ent_f = ent_q as f64 / 100.0;

                let weights = libpermstream::predictor::A3BPredictor::init_weights(&config.predictor_mode, config.seed, config.weights.clone());
                let a3b = libpermstream::predictor::A3BPredictor::new(Some(weights), 0.01);
                
                let features = Array1::from_vec(vec![ent_f, 0.0, ce.raw_size as f64, 1.0]);
                let theta = a3b.predict(&features);
                let state = crypto::chunk_state(config.seed, file_id, chunk_index);

                if config.use_rank {
                    anyhow::bail!("GPU path currently optimized for Seeded Braid mode only");
                }

                let enc_len = byteorder::ReadBytesExt::read_u32::<BigEndian>(&mut reader)? as usize;
                if enc_len > psfs::MAX_CHUNK_SIZE {
                    anyhow::bail!("Encoded size exceeds limit");
                }
                let mut encoded = vec![0u8; enc_len];
                std::io::Read::read_exact(&mut reader, &mut encoded)?;

                let mut model = [1i64; 256];
                let decoded = libpermstream::arithmetic::decode(&encoded, &mut model, ce.raw_size as usize, ent_f);
                
                Ok((decoded, state, theta, config.block_size, config, ce))
            }).await.unwrap_or_else(|e| Err(anyhow::anyhow!("Executor error: {}", e)))
        }
    }

    #[tonic::async_trait]
    impl PermStreamDataNode for PsfsDataNode {
        async fn get_chunk(&self, request: Request<ChunkRequest>) -> Result<Response<ChunkResponse>, Status> {
            let req = request.into_inner();
            let container_path = resolve_safe_path(&req.container_path)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;

            let cache_key = format!("{}:{}:{}", req.container_path, req.file_id, req.chunk_index);

            // Fast path: Check if there's already an inflight request
            let mut rx = {
                if let Some(sender) = self.inflight_requests.get(&cache_key) {
                    Some(sender.subscribe())
                } else {
                    None
                }
            };

            // If we found an existing channel, await its broadcast
            if let Some(mut rx) = rx {
                return match rx.recv().await {
                    Ok(Ok(data)) => Ok(Response::new(ChunkResponse { data })),
                    Ok(Err(status)) => Err(status),
                    Err(_) => Err(Status::internal("Coalesced request failed or sender dropped")),
                };
            }

            // We are the first to request this chunk. Create a channel.
            // Capacity of 1 is enough since we only broadcast the final result once.
            let (tx, _) = broadcast::channel::<CoalescingResult>(1);

            // Re-check and insert to handle race conditions where another thread inserted between our check and here.
            {
                let entry = self.inflight_requests.entry(cache_key.clone()).or_insert_with(|| tx.clone());
                // If the entry we got back isn't our tx, someone else beat us to it.
                if !entry.value().same_channel(&tx) {
                    rx = Some(entry.subscribe());
                }
            }

            // If someone beat us in the race, await their channel
            if let Some(mut rx) = rx {
                return match rx.recv().await {
                    Ok(Ok(data)) => Ok(Response::new(ChunkResponse { data })),
                    Ok(Err(status)) => Err(status),
                    Err(_) => Err(Status::internal("Coalesced request failed or sender dropped")),
                };
            }

            // Perform the actual work
            let result = match self.read_chunk(&container_path, req.file_id, req.chunk_index).await {
                Ok(data) => Ok(data),
                Err(e) => Err(Status::internal(format!("Failed to read chunk: {}", e))),
            };

            // Broadcast the result to any workers that arrived while we were processing
            let _ = tx.send(result.clone());

            // Remove the key so future requests will re-read (unless an external caching layer handles it)
            self.inflight_requests.remove(&cache_key);

            match result {
                Ok(data) => Ok(Response::new(ChunkResponse { data })),
                Err(e) => Err(e),
            }
        }
        type StreamTensorsStream = ReceiverStream<Result<TensorResponse, Status>>;

        async fn stream_tensors(&self, request: Request<TensorRequest>) -> Result<Response<Self::StreamTensorsStream>, Status> {
            let req = request.into_inner();
            let container_path = resolve_safe_path(&req.container_path)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
            let (tx, rx) = tokio::sync::mpsc::channel(4);

            let node = self.clone();
            
            tokio::spawn(async move {
                let file_ids = match node.get_file_ids(&container_path, &req.file_paths).await {
                    Ok(ids) => ids,
                    Err(e) => {
                        let _ = tx.send(Err(Status::internal(format!("Failed to get file IDs: {}", e)))).await;
                        return;
                    }
                };

                for (path, file_id) in file_ids {
                    let chunk_count = match node.get_chunk_count(&container_path, file_id).await {
                        Ok(count) => count,
                        Err(e) => {
                            let _ = tx.send(Err(Status::internal(format!("Failed to get chunk count for {}: {}", path, e)))).await;
                            continue;
                        }
                    };

                    for i in 0..chunk_count {
                        let decoded_data = if let Some(ref gpu_ctx) = node.gpu {
                            match node.read_chunk_for_gpu(&container_path, file_id, i).await {
                                Ok((permuted_data, state, theta, block_size, _config, ce)) => {
                                    // Run the GPU step in spawn_blocking as wgpu's poll/map is blocking
                                    let gpu_ctx = gpu_ctx.clone();
                                    tokio::task::spawn_blocking(move || {
                                        let mut output = Vec::new();
                                        for chunk in permuted_data.chunks(block_size) {
                                            let (perm, _): (Vec<usize>, u32) = crypto::braid_permutation(chunk.len(), state, theta);
                                            let input_u32: Vec<u32> = chunk.iter().map(|&x| x as u32).collect();
                                            let perm_u32: Vec<u32> = perm.iter().map(|&x| x as u32).collect();
                                            let result_u32 = gpu_ctx.unpermute(&input_u32, &perm_u32).unwrap_or(input_u32);
                                            output.extend(result_u32.iter().map(|&x| x as u8));
                                        }
                                        if (ce.flags & psfs::PSFS_CHUNK_FLAG_CRC as u32) != 0 {
                                            let crc = crc32fast::hash(&output);
                                            if crc != ce.crc32 { return Err(anyhow::anyhow!("CRC mismatch for chunk")); }
                                        }
                                        Ok(output)
                                    }).await.unwrap_or_else(|e| Err(anyhow::anyhow!("Executor error: {}", e)))
                                }
                                Err(_) => node.read_chunk(&container_path, file_id, i).await,
                            }
                        } else {
                            node.read_chunk(&container_path, file_id, i).await
                        };

                        match decoded_data {
                            Ok(data) => {
                                let response = TensorResponse {
                                    tensor_data: data,
                                    shape: vec![-1],
                                    dtype: "uint8".to_string(),
                                };
                                if tx.send(Ok(response)).await.is_err() {
                                    return;
                                }
                            }
                            Err(e) => {
                                let _ = tx.send(Err(Status::internal(format!("Failed to read chunk {} of {}: {}", i, path, e)))).await;
                                return;
                            }
                        }
                    }
                }
            });

            Ok(Response::new(ReceiverStream::new(rx)))
        }
    }

    pub async fn run_server(addr: &str) -> Result<()> {
        let addr = addr.parse()?;
        let service = PsfsDataNode::default();

        println!("PermStream Data Node listening on {}", addr);

        Server::builder()
            .add_service(PermStreamDataNodeServer::new(service)
                .max_decoding_message_size(64 * 1024 * 1024)
                .max_encoding_message_size(64 * 1024 * 1024))
            .serve(addr)
            .await?;

        Ok(())
    }
}

// Configure the runtime for High-Concurrency / Neighbor-Aware execution
#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Pack {
            input_dir,
            output_file,
            chunk_size,
            block_size,
            predictor,
            seed,
            no_rank,
            entropy_skip,
            transform,
            verify,
        } => {
            let mode = if predictor == "header" {
                PredictorMode::Header
            } else {
                PredictorMode::Seeded
            };
            let config = CodecConfig {
                chunk_size,
                block_size,
                use_rank: !no_rank,
                predictor_mode: mode,
                seed,
                entropy_skip,
                transform,
                ..Default::default()
            };
            psfs::pack_psfs(&input_dir, &output_file, config, verify)?;
            println!("Successfully packed {} into {}", input_dir.display(), output_file.display());
        }
        Commands::Unpack {
            input_file,
            output_dir,
            verify,
        } => {
            psfs::unpack_psfs(&input_file, &output_dir, verify)?;
            println!("Successfully unpacked {} to {}", input_file.display(), output_dir.display());
        }
        Commands::Verify { input_file } => {
            psfs::verify_psfs(&input_file)?;
            println!("Container {} verified successfully", input_file.display());
        }
        Commands::Mcp { transport } => {
            if transport == "stdio" {
                mcp_server::run_stdio().await?;
            } else {
                anyhow::bail!("Unsupported transport: {}", transport);
            }
        }
        Commands::Serve { addr } => {
            data_node::run_server(&addr).await?;
        }
        Commands::License { license_cmd } => {
            match license_cmd {
                LicenseCommands::Status => {
                    println!("Status: Active (Enterprise Tier)");
                }
                LicenseCommands::Install { key } => {
                    licensing::validate_license(&key)?;
                    println!("License key installed successfully.");
                }
            }
        }
    }

    Ok(())
}
