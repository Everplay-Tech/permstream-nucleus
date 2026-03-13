use std::path::Path;
use tract_onnx::prelude::*;
use tokenizers::Tokenizer;

pub struct VectorIndexer {
    model: Option<SimplePlan<TypedFact, Box<dyn TypedOp>, Graph<TypedFact, Box<dyn TypedOp>>>>,
    tokenizer: Option<Tokenizer>,
}

impl VectorIndexer {
    /// Initialize the Vector Generation Pipeline.
    /// In an enterprise setup, the model_path would point to an optimized
    /// all-MiniLM-L6-v2 ONNX model and tokenizer.json.
    pub fn new(model_path: Option<&Path>, tokenizer_path: Option<&Path>) -> anyhow::Result<Self> {
        let model = if let Some(path) = model_path {
            let model = tract_onnx::onnx()
                .model_for_path(path)?
                .into_optimized()?
                .into_runnable()?;
            Some(model)
        } else {
            None
        };

        let tokenizer = if let Some(path) = tokenizer_path {
            Some(Tokenizer::from_file(path).map_err(|e| anyhow::anyhow!("Tokenizer error: {}", e))?)
        } else {
            None
        };

        Ok(Self { model, tokenizer })
    }

    /// Generates a vector embedding for the given text.
    pub fn generate_embedding(&self, text: &str) -> anyhow::Result<Vec<f32>> {
        // If a real model and tokenizer are loaded, execute the pipeline
        if let (Some(model), Some(tokenizer)) = (&self.model, &self.tokenizer) {
            let encoding = tokenizer.encode(text, true).map_err(|e| anyhow::anyhow!("Encoding error: {}", e))?;
            
            // Prepare inputs for MiniLM-L6-v2 (input_ids, attention_mask, token_type_ids)
            let input_ids: Vec<i64> = encoding.get_ids().iter().map(|&x| x as i64).collect();
            let attention_mask: Vec<i64> = encoding.get_attention_mask().iter().map(|&x| x as i64).collect();
            let token_type_ids: Vec<i64> = encoding.get_type_ids().iter().map(|&x| x as i64).collect();
            
            let seq_len = input_ids.len();
            
            let tensor_input_ids = tract_ndarray::Array2::from_shape_vec((1, seq_len), input_ids)?.into_tensor();
            let tensor_attention_mask = tract_ndarray::Array2::from_shape_vec((1, seq_len), attention_mask)?.into_tensor();
            let tensor_token_type_ids = tract_ndarray::Array2::from_shape_vec((1, seq_len), token_type_ids)?.into_tensor();

            let result = model.run(tvec!(
                tensor_input_ids.into(),
                tensor_attention_mask.into(),
                tensor_token_type_ids.into()
            ))?;

            // Extract the [CLS] token embedding (first token of last hidden state)
            let embeddings_tensor = result[0].to_array_view::<f32>()?;
            let cls_embedding = embeddings_tensor.slice(tract_ndarray::s![0, 0, ..]).to_vec();
            
            return Ok(cls_embedding);
        }

        // Mock mode for local testing without ONNX files downloaded
        let mut mock_embedding = vec![0.0f32; 384]; // Standard MiniLM dim
        let bytes = text.as_bytes();
        for (i, &b) in bytes.iter().enumerate().take(384) {
            mock_embedding[i] = (b as f32) / 255.0;
        }
        
        // Normalize
        let norm: f32 = mock_embedding.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 0.0 {
            for x in mock_embedding.iter_mut() {
                *x /= norm;
            }
        }
        
        Ok(mock_embedding)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mock_embedding() {
        let indexer = VectorIndexer::new(None, None).unwrap();
        let embedding = indexer.generate_embedding("PermStream Nucleus GPU accelerated RAG").unwrap();
        
        assert_eq!(embedding.len(), 384);
        
        // Ensure it is roughly normalized
        let norm: f32 = embedding.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((norm - 1.0).abs() < 1e-5);
    }
}
