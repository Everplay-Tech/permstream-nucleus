use pollster::block_on;
use wgpu::util::DeviceExt;

const SHADER_CODE: &str = r#"
@group(0) @binding(0) var<storage, read> input_data: array<u32>;
@group(0) @binding(1) var<storage, read> indices: array<u32>;
@group(0) @binding(2) var<storage, read_write> output_data: array<u32>;

// FSST-GPU / DFloat11 optimization paradigm:
// Structure permutations as independent tiles fitting into 32KB shared memory.
// This allows braids to be unweaved entirely inside the L1 cache.
const TILE_SIZE: u32 = 1024u; // 4KB tile of u32s
var<workgroup> input_tile: array<u32, 1024>;
var<workgroup> output_tile: array<u32, 1024>;

@compute @workgroup_size(256)
fn main(
    @builtin(global_invocation_id) global_id: vec3<u32>,
    @builtin(local_invocation_id) local_id: vec3<u32>,
    @builtin(workgroup_id) group_id: vec3<u32>
) {
    let tile_offset = group_id.x * TILE_SIZE;
    
    // 1. Cooperative Load: Pull VRAM into SRAM (L1)
    for (var i: u32 = 0u; i < 4u; i = i + 1u) {
        let local_idx = local_id.x + i * 256u;
        let global_idx = tile_offset + local_idx;
        if (global_idx < arrayLength(&input_data)) {
            input_tile[local_idx] = input_data[global_idx];
        }
    }
    
    workgroupBarrier();
    
    // 2. Unweave Braid entirely within SRAM (Zero Warp Divergence in VRAM)
    for (var i: u32 = 0u; i < 4u; i = i + 1u) {
        let local_idx = local_id.x + i * 256u;
        let global_idx = tile_offset + local_idx;
        if (global_idx < arrayLength(&input_data)) {
            let target_idx = indices[global_idx];
            let local_target = target_idx - tile_offset;
            
            if (target_idx >= tile_offset && local_target < TILE_SIZE) {
                // Resolved entirely in L1 cache
                output_tile[local_target] = input_tile[local_idx];
            } else {
                // Fallback for permutations crossing tile boundaries (rare in PermStream)
                let safe_target = min(target_idx, arrayLength(&output_data) - 1u);
                output_data[safe_target] = input_tile[local_idx];
            }
        }
    }
    
    workgroupBarrier();
    
    // 3. Coalesced Store: Flush resolved SRAM back to VRAM
    for (var i: u32 = 0u; i < 4u; i = i + 1u) {
        let local_idx = local_id.x + i * 256u;
        let global_idx = tile_offset + local_idx;
        if (global_idx < arrayLength(&output_data)) {
            // Because PermStream permutations are block-aligned bi-jections that fit 
            // entirely inside TILE_SIZE, output_tile is fully populated and valid.
            output_data[global_idx] = output_tile[local_idx];
        }
    }
}
"#;

pub struct GpuContext {
    device: wgpu::Device,
    queue: wgpu::Queue,
    compute_pipeline: wgpu::ComputePipeline,
    bind_group_layout: wgpu::BindGroupLayout,
}

impl GpuContext {
    pub fn new() -> anyhow::Result<Self> {
        block_on(Self::new_async())
    }

    async fn new_async() -> anyhow::Result<Self> {
        let instance = wgpu::Instance::new(wgpu::InstanceDescriptor {
            backends: wgpu::Backends::all(),
            ..Default::default()
        });

        let adapter = instance
            .request_adapter(&wgpu::RequestAdapterOptions {
                power_preference: wgpu::PowerPreference::HighPerformance,
                compatible_surface: None,
                force_fallback_adapter: false,
            })
            .await
            .ok_or_else(|| anyhow::anyhow!("Failed to find an appropriate GPU adapter"))?;

        let (device, queue) = adapter
            .request_device(&wgpu::DeviceDescriptor::default(), None)
            .await?;

        let shader = device.create_shader_module(wgpu::ShaderModuleDescriptor {
            label: Some("Unpermute Shader"),
            source: wgpu::ShaderSource::Wgsl(SHADER_CODE.into()),
        });

        let bind_group_layout = device.create_bind_group_layout(&wgpu::BindGroupLayoutDescriptor {
            label: Some("Unpermute Bind Group Layout"),
            entries: &[
                wgpu::BindGroupLayoutEntry {
                    binding: 0,
                    visibility: wgpu::ShaderStages::COMPUTE,
                    ty: wgpu::BindingType::Buffer {
                        ty: wgpu::BufferBindingType::Storage { read_only: true },
                        has_dynamic_offset: false,
                        min_binding_size: None,
                    },
                    count: None,
                },
                wgpu::BindGroupLayoutEntry {
                    binding: 1,
                    visibility: wgpu::ShaderStages::COMPUTE,
                    ty: wgpu::BindingType::Buffer {
                        ty: wgpu::BufferBindingType::Storage { read_only: true },
                        has_dynamic_offset: false,
                        min_binding_size: None,
                    },
                    count: None,
                },
                wgpu::BindGroupLayoutEntry {
                    binding: 2,
                    visibility: wgpu::ShaderStages::COMPUTE,
                    ty: wgpu::BindingType::Buffer {
                        ty: wgpu::BufferBindingType::Storage { read_only: false },
                        has_dynamic_offset: false,
                        min_binding_size: None,
                    },
                    count: None,
                },
            ],
        });

        let pipeline_layout = device.create_pipeline_layout(&wgpu::PipelineLayoutDescriptor {
            label: Some("Unpermute Pipeline Layout"),
            bind_group_layouts: &[&bind_group_layout],
            push_constant_ranges: &[],
        });

        let compute_pipeline = device.create_compute_pipeline(&wgpu::ComputePipelineDescriptor {
            label: Some("Unpermute Compute Pipeline"),
            layout: Some(&pipeline_layout),
            module: &shader,
            entry_point: "main",
            compilation_options: Default::default(),
        });

        Ok(Self {
            device,
            queue,
            compute_pipeline,
            bind_group_layout,
        })
    }

    /// Run the unpermutation kernel on the GPU. 
    /// Note: the current shader operates on u32s for alignment reasons.
    pub fn unpermute(&self, data: &[u32], indices: &[u32]) -> anyhow::Result<Vec<u32>> {
        if data.len() != indices.len() {
            anyhow::bail!("Data and indices length mismatch");
        }
        if data.is_empty() {
            return Ok(Vec::new());
        }

        let data_buffer = self.device.create_buffer_init(&wgpu::util::BufferInitDescriptor {
            label: Some("Input Data Buffer"),
            contents: bytemuck::cast_slice(data),
            usage: wgpu::BufferUsages::STORAGE,
        });

        let indices_buffer = self.device.create_buffer_init(&wgpu::util::BufferInitDescriptor {
            label: Some("Indices Buffer"),
            contents: bytemuck::cast_slice(indices),
            usage: wgpu::BufferUsages::STORAGE,
        });

        let output_buffer = self.device.create_buffer(&wgpu::BufferDescriptor {
            label: Some("Output Data Buffer"),
            size: (data.len() * std::mem::size_of::<u32>()) as wgpu::BufferAddress,
            usage: wgpu::BufferUsages::STORAGE | wgpu::BufferUsages::COPY_SRC,
            mapped_at_creation: false,
        });

        let bind_group = self.device.create_bind_group(&wgpu::BindGroupDescriptor {
            label: Some("Unpermute Bind Group"),
            layout: &self.bind_group_layout,
            entries: &[
                wgpu::BindGroupEntry {
                    binding: 0,
                    resource: data_buffer.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 1,
                    resource: indices_buffer.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 2,
                    resource: output_buffer.as_entire_binding(),
                },
            ],
        });

        let mut encoder = self.device.create_command_encoder(&wgpu::CommandEncoderDescriptor {
            label: Some("Unpermute Encoder"),
        });

        {
            let mut cpass = encoder.begin_compute_pass(&wgpu::ComputePassDescriptor {
                label: Some("Unpermute Compute Pass"),
                timestamp_writes: None,
            });
            cpass.set_pipeline(&self.compute_pipeline);
            cpass.set_bind_group(0, &bind_group, &[]);
            let workgroup_count = ((data.len() as u32) + 1023) / 1024;
            cpass.dispatch_workgroups(workgroup_count, 1, 1);
        }

        // Copy output to staging buffer to read back to CPU
        let staging_buffer = self.device.create_buffer(&wgpu::BufferDescriptor {
            label: Some("Staging Buffer"),
            size: (data.len() * std::mem::size_of::<u32>()) as wgpu::BufferAddress,
            usage: wgpu::BufferUsages::MAP_READ | wgpu::BufferUsages::COPY_DST,
            mapped_at_creation: false,
        });

        encoder.copy_buffer_to_buffer(
            &output_buffer, 0,
            &staging_buffer, 0,
            (data.len() * std::mem::size_of::<u32>()) as wgpu::BufferAddress,
        );

        self.queue.submit(Some(encoder.finish()));

        let buffer_slice = staging_buffer.slice(..);
        let (sender, receiver) = futures_channel::oneshot::channel::<Result<(), wgpu::BufferAsyncError>>();
        buffer_slice.map_async(wgpu::MapMode::Read, move |v| {
            let _ = sender.send(v);
        });

        self.device.poll(wgpu::Maintain::Wait);

        block_on(async {
            receiver.await
                .map_err(|_| anyhow::anyhow!("Channel dropped before GPU map result"))?
                .map_err(|e| anyhow::anyhow!("GPU buffer map failed: {}", e))
        })?;

        let mapped_data = buffer_slice.get_mapped_range();
        let result: Vec<u32> = bytemuck::cast_slice(&mapped_data).to_vec();
        drop(mapped_data);
        staging_buffer.unmap();

        Ok(result)
    }
}
