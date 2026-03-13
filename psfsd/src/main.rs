use clap::{Parser, Subcommand};
use libpermstream::{CodecConfig, PredictorMode, psfs, rag};
use std::path::PathBuf;
use anyhow::{Context, Result};
use std::io::Read;

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
        async fn list_files(&self, #[tool(aggr)] params: ListFilesParams) -> String {
            let container = PathBuf::from(&params.container_path);
            match self.get_file_list(&container) {
                Ok(files) => {
                    let mut result = format!("Contents of {}:\n", container.display());
                    for f in files {
                        result.push_str(&format!("- {}\n", f));
                    }
                    result
                }
                Err(e) => format!("Error reading container: {}", e),
            }
        }

        fn get_file_list(&self, path: &PathBuf) -> Result<Vec<String>> {
            use std::io::Seek;
            let mut file = std::fs::File::open(path)?;
            let sb = psfs::Superblock::read(&mut file)?;
            
            file.seek(std::io::SeekFrom::Start(sb.index_offset))?;
            let mut file_entries = Vec::new();
            for _ in 0..sb.file_count {
                file_entries.push(psfs::FileEntry::read(&mut file)?);
            }

            file.seek(std::io::SeekFrom::Start(sb.strings_offset))?;
            let string_table_size = (sb.chunk_table_offset - sb.strings_offset) as usize;
            if string_table_size > psfs::MAX_STRING_TABLE_SIZE {
                anyhow::bail!("String table exceeds size limit");
            }
            let mut string_table = Vec::new();
            file.try_clone()?.take(string_table_size as u64).read_to_end(&mut string_table)?;

            let mut files = Vec::new();
            for fe in file_entries {
                let path_bytes = &string_table[fe.path_offset as usize .. (fe.path_offset + fe.path_len) as usize];
                if let Ok(rel_path) = std::str::from_utf8(path_bytes) {
                    files.push(rel_path.to_string());
                }
            }
            Ok(files)
        }

        #[tool(description = "Extract a specific file from a PSFS container")]
        async fn extract_file(&self, #[tool(aggr)] params: ExtractFileParams) -> String {
            format!("Extracted {} from {}", params.file_path, params.container_path)
        }

        #[tool(description = "Semantic search for files in a PSFS container")]
        async fn semantic_search(&self, #[tool(aggr)] params: SearchParams) -> String {
            let container = PathBuf::from(&params.container_path);
            match self.do_semantic_search(&container, &params.query) {
                Ok(results) => {
                    if results.is_empty() {
                        return "No relevant files found.".to_string();
                    }
                    let mut output = "Top semantic matches:\n".to_string();
                    for (path, score) in results {
                        output.push_str(&format!("- {} (score: {:.4})\n", path, score));
                    }
                    output
                }
                Err(e) => format!("Search error: {}", e),
            }
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

    pub mod permstream {
        tonic::include_proto!("permstream");
    }

    #[derive(Clone)]
    pub struct PsfsDataNode {
        pub gpu: Option<Arc<gpu::GpuContext>>,
    }

    impl std::fmt::Debug for PsfsDataNode {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("PsfsDataNode")
                .field("gpu_enabled", &self.gpu.is_some())
                .finish()
        }
    }

    impl Default for PsfsDataNode {
        fn default() -> Self {
            let gpu_ctx = gpu::GpuContext::new().ok().map(Arc::new);
            Self { gpu: gpu_ctx }
        }
    }

    impl PsfsDataNode {
        fn read_chunk(&self, path: &PathBuf, file_id: u32, chunk_index: u32) -> Result<Vec<u8>> {
            use std::io::Seek;
            let mut file = std::fs::File::open(path)?;
            let sb = psfs::Superblock::read(&mut file)?;
            
            file.seek(std::io::SeekFrom::Start(sb.index_offset + (file_id as u64 * psfs::PSFS_FILE_SIZE as u64)))?;
            let fe = psfs::FileEntry::read(&mut file)?;
            
            if chunk_index >= fe.chunk_count {
                anyhow::bail!("Chunk index out of bounds");
            }

            let chunk_id = fe.chunk_start + chunk_index;
            file.seek(std::io::SeekFrom::Start(sb.chunk_table_offset + (chunk_id as u64 * psfs::PSFS_CHUNK_SIZE as u64)))?;
            let ce = psfs::ChunkEntry::read(&mut file)?;

            file.seek(std::io::SeekFrom::Start(ce.data_offset))?;
            if ce.stored_size as usize > psfs::MAX_CHUNK_SIZE {
                anyhow::bail!("Chunk size exceeds limit");
            }
            let mut payload = Vec::new();
            file.try_clone()?.take(ce.stored_size as u64).read_to_end(&mut payload)?;

            let config = CodecConfig {
                chunk_size: sb.chunk_size as usize,
                block_size: sb.block_size as usize,
                use_rank: (sb.codec_flags & (1 << 0)) != 0,
                predictor_mode: if (sb.codec_flags & (1 << 1)) != 0 { PredictorMode::Header } else { PredictorMode::Seeded },
                seed: sb.seed,
                weights: Some(Array1::from_vec(sb.weights.to_vec().iter().map(|&x| x as f64).collect())),
                ..Default::default()
            };

            let decoded = psfs::decompress_chunk_payload(&payload, ce.raw_size as usize, &config, file_id, chunk_index, ce.codec_id, ce.transform_id)?;
            Ok(decoded)
        }

        fn get_file_ids(&self, container: &PathBuf, requested_paths: &[String]) -> Result<Vec<(String, u32)>> {
            use std::io::Seek;
            let mut file = std::fs::File::open(container)?;
            let sb = psfs::Superblock::read(&mut file)?;
            
            file.seek(std::io::SeekFrom::Start(sb.index_offset))?;
            let mut file_entries = Vec::new();
            for _ in 0..sb.file_count {
                file_entries.push(psfs::FileEntry::read(&mut file)?);
            }

            file.seek(std::io::SeekFrom::Start(sb.strings_offset))?;
            let string_table_size = (sb.chunk_table_offset - sb.strings_offset) as usize;
            if string_table_size > psfs::MAX_STRING_TABLE_SIZE {
                anyhow::bail!("String table exceeds size limit");
            }
            let mut string_table = Vec::new();
            file.try_clone()?.take(string_table_size as u64).read_to_end(&mut string_table)?;

            let mut results = Vec::new();
            for fe in file_entries {
                let path_bytes = &string_table[fe.path_offset as usize .. (fe.path_offset + fe.path_len) as usize];
                if let Ok(rel_path) = std::str::from_utf8(path_bytes) {
                    if requested_paths.contains(&rel_path.to_string()) {
                        results.push((rel_path.to_string(), fe.file_id));
                    }
                }
            }
            Ok(results)
        }

        fn get_chunk_count(&self, container: &PathBuf, file_id: u32) -> Result<u32> {
            use std::io::Seek;
            let mut file = std::fs::File::open(container)?;
            let sb = psfs::Superblock::read(&mut file)?;
            
            file.seek(std::io::SeekFrom::Start(sb.index_offset + (file_id as u64 * psfs::PSFS_FILE_SIZE as u64)))?;
            let fe = psfs::FileEntry::read(&mut file)?;
            Ok(fe.chunk_count)
        }

        fn read_chunk_for_gpu(&self, path: &PathBuf, file_id: u32, chunk_index: u32) -> Result<(Vec<u8>, u32, f64, usize, CodecConfig)> {
            use std::io::Seek;
            use byteorder::{BigEndian, ReadBytesExt};
            let mut file = std::fs::File::open(path)?;
            let sb = psfs::Superblock::read(&mut file)?;
            
            file.seek(std::io::SeekFrom::Start(sb.index_offset + (file_id as u64 * psfs::PSFS_FILE_SIZE as u64)))?;
            let fe = psfs::FileEntry::read(&mut file)?;
            
            let chunk_id = fe.chunk_start + chunk_index;
            file.seek(std::io::SeekFrom::Start(sb.chunk_table_offset + (chunk_id as u64 * psfs::PSFS_CHUNK_SIZE as u64)))?;
            let ce = psfs::ChunkEntry::read(&mut file)?;

            if ce.codec_id == psfs::PSFS_CODEC_RAW {
                anyhow::bail!("Chunk is raw, no unpermutation needed");
            }

            file.seek(std::io::SeekFrom::Start(ce.data_offset))?;
            if ce.stored_size as usize > psfs::MAX_CHUNK_SIZE {
                anyhow::bail!("Chunk size exceeds limit");
            }
            let mut payload = Vec::new();
            file.try_clone()?.take(ce.stored_size as u64).read_to_end(&mut payload)?;

            let config = CodecConfig {
                chunk_size: sb.chunk_size as usize,
                block_size: sb.block_size as usize,
                use_rank: (sb.codec_flags & (1 << 0)) != 0,
                predictor_mode: if (sb.codec_flags & (1 << 1)) != 0 { PredictorMode::Header } else { PredictorMode::Seeded },
                seed: sb.seed,
                weights: Some(Array1::from_vec(sb.weights.to_vec().iter().map(|&x| x as f64).collect())),
                ..Default::default()
            };

            let mut reader = std::io::Cursor::new(&payload);
            let ent_q = reader.read_u16::<BigEndian>()?;
            let ent_f = ent_q as f64 / 100.0;

            let weights = libpermstream::predictor::A3BPredictor::init_weights(&config.predictor_mode, config.seed, config.weights.clone());
            let a3b = libpermstream::predictor::A3BPredictor::new(Some(weights), 0.01);
            
            let features = Array1::from_vec(vec![ent_f, 0.0, ce.raw_size as f64, 1.0]);
            let theta = a3b.predict(&features);
            let state = crypto::chunk_state(config.seed, file_id, chunk_index);

            if config.use_rank {
                anyhow::bail!("GPU path currently optimized for Seeded Braid mode only");
            }

            let enc_len = reader.read_u32::<BigEndian>()? as usize;
            let mut encoded = vec![0u8; enc_len];
            reader.read_exact(&mut encoded)?;

            let mut model = [1i64; 256];
            let decoded = libpermstream::arithmetic::decode(&encoded, &mut model, ce.raw_size as usize, ent_f);
            
            Ok((decoded, state, theta, config.block_size, config))
        }
    }

    #[tonic::async_trait]
    impl PermStreamDataNode for PsfsDataNode {
        async fn get_chunk(&self, request: Request<ChunkRequest>) -> Result<Response<ChunkResponse>, Status> {
            let req = request.into_inner();
            let container_path = PathBuf::from(req.container_path);
            
            match self.read_chunk(&container_path, req.file_id, req.chunk_index) {
                Ok(data) => Ok(Response::new(ChunkResponse { data })),
                Err(e) => Err(Status::internal(format!("Failed to read chunk: {}", e))),
            }
        }

        type StreamTensorsStream = ReceiverStream<Result<TensorResponse, Status>>;

        async fn stream_tensors(&self, request: Request<TensorRequest>) -> Result<Response<Self::StreamTensorsStream>, Status> {
            let req = request.into_inner();
            let container_path = PathBuf::from(req.container_path);
            let (tx, rx) = tokio::sync::mpsc::channel(4);

            let node = self.clone();
            
            tokio::spawn(async move {
                let file_ids = match node.get_file_ids(&container_path, &req.file_paths) {
                    Ok(ids) => ids,
                    Err(e) => {
                        let _ = tx.send(Err(Status::internal(format!("Failed to get file IDs: {}", e)))).await;
                        return;
                    }
                };

                for (path, file_id) in file_ids {
                    let chunk_count = match node.get_chunk_count(&container_path, file_id) {
                        Ok(count) => count,
                        Err(e) => {
                            let _ = tx.send(Err(Status::internal(format!("Failed to get chunk count for {}: {}", path, e)))).await;
                            continue;
                        }
                    };

                    for i in 0..chunk_count {
                        let decoded_data = if let Some(ref gpu_ctx) = node.gpu {
                            match node.read_chunk_for_gpu(&container_path, file_id, i) {
                                Ok((permuted_data, state, theta, block_size, _config)) => {
                                    let mut output = Vec::new();
                                    for chunk in permuted_data.chunks(block_size) {
                                        let (perm, _) = crypto::braid_permutation(chunk.len(), state, theta);
                                        let input_u32: Vec<u32> = chunk.iter().map(|&x| x as u32).collect();
                                        let perm_u32: Vec<u32> = perm.iter().map(|&x| x as u32).collect();
                                        let result_u32 = gpu_ctx.unpermute(&input_u32, &perm_u32).unwrap_or(input_u32);
                                        output.extend(result_u32.iter().map(|&x| x as u8));
                                    }
                                    Ok(output)
                                }
                                Err(_) => node.read_chunk(&container_path, file_id, i),
                            }
                        } else {
                            node.read_chunk(&container_path, file_id, i)
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
            .add_service(PermStreamDataNodeServer::new(service))
            .serve(addr)
            .await?;

        Ok(())
    }
}

#[tokio::main]
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
