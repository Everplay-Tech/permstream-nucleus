use iceoryx2::prelude::*;
use iceoryx2::port::publisher::Publisher;
use anyhow::Result;

const SERVICE_NAME: &str = "PermStreamTensors";

pub struct IpcServer {
    publisher: Publisher<ipc::Service, [u8], ()>,
}

impl IpcServer {
    pub fn new() -> Result<Self> {
        let node = NodeBuilder::new().create::<ipc::Service>()?;
        
        let service = node.service_builder(&ServiceName::new(SERVICE_NAME).unwrap())
            .publish_subscribe::<[u8]>()
            .max_publishers(1)
            .max_subscribers(128) // Adjust based on thundering herd needs
            .history_size(10)
            .subscriber_max_buffer_size(10)
            .create()?;
            
        let publisher = service.publisher_builder().create()?;
        
        Ok(Self { publisher })
    }

    pub fn publish_chunk(&self, data: &[u8]) -> Result<()> {
        let mut sample = self.publisher.loan_slice(data.len())?;
        sample.payload_mut().copy_from_slice(data);
        sample.send()?;
        Ok(())
    }
}
