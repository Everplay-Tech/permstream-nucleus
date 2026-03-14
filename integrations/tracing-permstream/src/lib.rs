use tracing::{Event, Subscriber};
use tracing_subscriber::layer::Context;
use tracing_subscriber::Layer;
use std::sync::{Arc, Mutex};
use std::io::Write;
use libpermstream::{CodecConfig, compress_stream};

/// A custom Tracing Layer that intercepts structured log events, buffers them,
/// and compresses them in real-time using PermStream Nucleus.
/// This fulfills the "Trojan Horse" strategy for high-volume observability stacks.
pub struct PermStreamLayer<W: Write + Send + 'static> {
    writer: Arc<Mutex<W>>,
    buffer: Arc<Mutex<Vec<u8>>>,
    flush_threshold: usize,
    config: CodecConfig,
}

impl<W: Write + Send + 'static> PermStreamLayer<W> {
    pub fn new(writer: W, flush_threshold: usize, config: CodecConfig) -> Self {
        Self {
            writer: Arc::new(Mutex::new(writer)),
            buffer: Arc::new(Mutex::new(Vec::with_capacity(flush_threshold))),
            flush_threshold,
            config,
        }
    }

    fn flush_buffer(&self) {
        let mut buf = self.buffer.lock().unwrap();
        if buf.is_empty() { return; }

        let mut writer = self.writer.lock().unwrap();
        
        // In a true high-performance async environment, this compression step
        // would be offloaded to a dedicated Tokio worker thread to avoid blocking
        // the application's hot path.
        let mut compressed = Vec::new();
        let _ = compress_stream(&buf[..], &mut compressed, self.config.clone());
        
        let _ = writer.write_all(&compressed);
        let _ = writer.flush();
        
        buf.clear();
    }
}

impl<S, W> Layer<S> for PermStreamLayer<W>
where
    S: Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
    W: Write + Send + 'static,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        // We use a basic JSON formatter to serialize the event.
        // In production, you would use tracing_serde or similar.
        let mut serialized = format!(
            "{{\"level\":\"{}\", \"target\":\"{}\"}}",
            event.metadata().level(),
            event.metadata().target()
        );
        serialized.push('\n');

        let mut buf = self.buffer.lock().unwrap();
        buf.extend_from_slice(serialized.as_bytes());

        if buf.len() >= self.flush_threshold {
            // Drop lock before flushing to prevent deadlocks
            drop(buf);
            self.flush_buffer();
        }
    }
}

impl<W: Write + Send + 'static> Drop for PermStreamLayer<W> {
    fn drop(&mut self) {
        self.flush_buffer();
    }
}
