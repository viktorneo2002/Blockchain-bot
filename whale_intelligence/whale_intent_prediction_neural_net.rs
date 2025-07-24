#![deny(unsafe_code)]
#![deny(warnings)]

use ndarray::{Array1, Array2, Axis};
use std::path::Path;
use thiserror::Error;
use serde::{Deserialize, Serialize};
use blake3;

/// Whale intent classification categories
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IntentClass {
    Accumulate,
    Dump,
    Rotate,
    Spoof,
    Snipe,
}

impl IntentClass {
    fn from_index(index: usize) -> Result<Self, PredictionError> {
        match index {
            0 => Ok(IntentClass::Accumulate),
            1 => Ok(IntentClass::Dump),
            2 => Ok(IntentClass::Rotate),
            3 => Ok(IntentClass::Spoof),
            4 => Ok(IntentClass::Snipe),
            _ => Err(PredictionError::InvalidClassIndex(index)),
        }
    }
}

/// Whale intent prediction result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WhaleIntentScore {
    pub class: IntentClass,
    pub confidence: f32,
    pub slot: u64,
}

/// Neural network model weights and configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ModelWeights {
    input_weights: Vec<Vec<f32>>,
    hidden_weights: Vec<Vec<f32>>,
    output_weights: Vec<Vec<f32>>,
    input_bias: Vec<f32>,
    hidden_bias: Vec<f32>,
    output_bias: Vec<f32>,
    feature_count: usize,
    hidden_size: usize,
    output_size: usize,
}

/// Prediction errors
#[derive(Error, Debug)]
pub enum PredictionError {
    #[error("Model file not found: {0}")]
    ModelNotFound(String),
    #[error("Invalid model format: {0}")]
    InvalidModelFormat(String),
    #[error("Feature vector size mismatch: expected {expected}, got {actual}")]
    FeatureSizeMismatch { expected: usize, actual: usize },
    #[error("Invalid class index: {0}")]
    InvalidClassIndex(usize),
    #[error("Confidence value out of range: {0}")]
    InvalidConfidence(f32),
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Serialization error: {0}")]
    SerializationError(#[from] bincode::Error),
    #[error("Numerical computation error")]
    NumericalError,
}

/// Neural network whale intent predictor
pub struct IntentPredictor {
    weights: ModelWeights,
    input_hash_cache: Option<[u8; 32]>,
}

impl IntentPredictor {
    /// Load model from disk with secure deserialization
    pub fn new(path_to_model: &Path) -> Result<Self, PredictionError> {
        let model_bytes = std::fs::read(path_to_model)
            .map_err(|_| PredictionError::ModelNotFound(path_to_model.to_string_lossy().to_string()))?;
        
        let weights: ModelWeights = bincode::deserialize(&model_bytes)
            .map_err(|e| PredictionError::InvalidModelFormat(e.to_string()))?;
        
        // Validate model dimensions
        if weights.input_weights.is_empty() || weights.hidden_weights.is_empty() || weights.output_weights.is_empty() {
            return Err(PredictionError::InvalidModelFormat("Empty weight matrices".to_string()));
        }
        
        if weights.output_size != 5 {
            return Err(PredictionError::InvalidModelFormat("Output size must be 5 for intent classes".to_string()));
        }
        
        Ok(IntentPredictor {
            weights,
            input_hash_cache: None,
        })
    }
    
    /// Predict whale intent from feature vector
    pub fn predict(&mut self, features: &[f32], slot: u64) -> Result<WhaleIntentScore, PredictionError> {
        // Validate input dimensions
        if features.len() != self.weights.feature_count {
            return Err(PredictionError::FeatureSizeMismatch {
                expected: self.weights.feature_count,
                actual: features.len(),
            });
        }
        
        // Validate feature values are finite
        for &val in features {
            if !val.is_finite() {
                return Err(PredictionError::NumericalError);
            }
        }
        
        // Hash input tensor for audit logging
        let input_hash = hash_input_tensor(features);
        self.input_hash_cache = Some(input_hash);
        
        // Convert to ndarray for efficient computation
        let input = Array1::from_vec(features.to_vec());
        
        // Forward pass through neural network
        let output = self.forward_pass(&input)?;
        
        // Apply softmax to get probabilities
        let probabilities = self.softmax(&output)?;
        
        // Find class with highest probability
        let (class_index, confidence) = probabilities
            .iter()
            .enumerate()
            .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
            .ok_or(PredictionError::NumericalError)?;
        
        let class = IntentClass::from_index(class_index)?;
        
        // Validate confidence is in valid range
        if !confidence.is_finite() || *confidence < 0.0 || *confidence > 1.0 {
            return Err(PredictionError::InvalidConfidence(*confidence));
        }
        
        Ok(WhaleIntentScore {
            class,
            confidence: *confidence,
            slot,
        })
    }
    
    /// Forward pass through the neural network
    fn forward_pass(&self, input: &Array1<f32>) -> Result<Array1<f32>, PredictionError> {
        // Input to hidden layer
        let input_weights = Array2::from_shape_vec(
            (self.weights.hidden_size, self.weights.feature_count),
            self.weights.input_weights.iter().flatten().copied().collect(),
        ).map_err(|_| PredictionError::NumericalError)?;
        
        let input_bias = Array1::from_vec(self.weights.input_bias.clone());
        let hidden = input_weights.dot(input) + &input_bias;
        let hidden_activated = hidden.mapv(|x| self.relu(x));
        
        // Hidden to output layer
        let hidden_weights = Array2::from_shape_vec(
            (self.weights.output_size, self.weights.hidden_size),
            self.weights.hidden_weights.iter().flatten().copied().collect(),
        ).map_err(|_| PredictionError::NumericalError)?;
        
        let hidden_bias = Array1::from_vec(self.weights.hidden_bias.clone());
        let output = hidden_weights.dot(&hidden_activated) + &hidden_bias;
        
        // Output layer
        let output_weights = Array2::from_shape_vec(
            (self.weights.output_size, self.weights.output_size),
            self.weights.output_weights.iter().flatten().copied().collect(),
        ).map_err(|_| PredictionError::NumericalError)?;
        
        let output_bias = Array1::from_vec(self.weights.output_bias.clone());
        let final_output = output_weights.dot(&output) + &output_bias;
        
        Ok(final_output)
    }
    
    /// ReLU activation function
    fn relu(&self, x: f32) -> f32 {
        if x > 0.0 { x } else { 0.0 }
    }
    
    /// Softmax activation for output layer
    fn softmax(&self, logits: &Array1<f32>) -> Result<Array1<f32>, PredictionError> {
        let max_logit = logits.fold(f32::NEG_INFINITY, |a, &b| a.max(b));
        let exp_logits = logits.mapv(|x| (x - max_logit).exp());
        let sum_exp = exp_logits.sum();
        
        if sum_exp == 0.0 || !sum_exp.is_finite() {
            return Err(PredictionError::NumericalError);
        }
        
        Ok(exp_logits / sum_exp)
    }
    
    /// Get last input hash for audit logging
    pub fn get_last_input_hash(&self) -> Option<[u8; 32]> {
        self.input_hash_cache
    }
    
    /// Reload model weights from disk for cold start recovery
    pub fn reload_weights(&mut self, path_to_model: &Path) -> Result<(), PredictionError> {
        let new_predictor = Self::new(path_to_model)?;
        self.weights = new_predictor.weights;
        self.input_hash_cache = None;
        Ok(())
    }
}

/// Hash input tensor using Blake3 for audit logging
pub fn hash_input_tensor(features: &[f32]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    
    // Convert f32 values to bytes for hashing
    for &feature in features {
        hasher.update(&feature.to_le_bytes());
    }
    
    *hasher.finalize().as_bytes()
}

/// Create a dummy model for testing
#[cfg(test)]
fn create_test_model() -> ModelWeights {
    ModelWeights {
        input_weights: vec![
            vec![0.1, 0.2, 0.3, 0.4, 0.5],
            vec![0.2, 0.3, 0.4, 0.5, 0.6],
            vec![0.3, 0.4, 0.5, 0.6, 0.7],
            vec![0.4, 0.5, 0.6, 0.7, 0.8],
        ],
        hidden_weights: vec![
            vec![0.1, 0.2, 0.3, 0.4],
            vec![0.2, 0.3, 0.4, 0.5],
            vec![0.3, 0.4, 0.5, 0.6],
            vec![0.4, 0.5, 0.6, 0.7],
        ],
        output_weights: vec![
            vec![0.1, 0.2, 0.3, 0.4],
            vec![0.2, 0.3, 0.4, 0.5],
            vec![0.3, 0.4, 0.5, 0.6],
            vec![0.4, 0.5, 0.6, 0.7],
            vec![0.5, 0.6, 0.7, 0.8],
        ],
        input_bias: vec![0.1, 0.2, 0.3, 0.4],
        hidden_bias: vec![0.1, 0.2, 0.3, 0.4],
        output_bias: vec![0.1, 0.2, 0.3, 0.4, 0.5],
        feature_count: 5,
        hidden_size: 4,
        output_size: 5,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    
    #[test]
    fn test_prediction_consistency() {
        // Create test model
        let model = create_test_model();
        let model_bytes = bincode::serialize(&model).unwrap();
        
        // Write to temporary file
        let temp_path = PathBuf::from("/tmp/test_model.bin");
        std::fs::write(&temp_path, &model_bytes).unwrap();
        
        // Load predictor
        let mut predictor = IntentPredictor::new(&temp_path).unwrap();
        
        // Test feature vector
        let features = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let slot = 12345;
        
        // Make multiple predictions with same input
        let result1 = predictor.predict(&features, slot).unwrap();
        let result2 = predictor.predict(&features, slot).unwrap();
        let result3 = predictor.predict(&features, slot).unwrap();
        
        // Verify consistency
        assert_eq!(result1.class, result2.class);
        assert_eq!(result2.class, result3.class);
        assert_eq!(result1.confidence, result2.confidence);
        assert_eq!(result2.confidence, result3.confidence);
        assert_eq!(result1.slot, result2.slot);
        assert_eq!(result2.slot, result3.slot);
        
        // Clean up
        std::fs::remove_file(&temp_path).unwrap();
    }
    
    #[test]
    fn test_hash_input_tensor() {
        let features = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let hash1 = hash_input_tensor(&features);
        let hash2 = hash_input_tensor(&features);
        
        // Same input should produce same hash
        assert_eq!(hash1, hash2);
        
        // Different input should produce different hash
        let different_features = vec![2.0, 3.0, 4.0, 5.0, 6.0];
        let hash3 = hash_input_tensor(&different_features);
        assert_ne!(hash1, hash3);
    }
    
    #[test]
    fn test_invalid_feature_size() {
        let model = create_test_model();
        let model_bytes = bincode::serialize(&model).unwrap();
        let temp_path = PathBuf::from("/tmp/test_model_invalid.bin");
        std::fs::write(&temp_path, &model_bytes).unwrap();
        
        let mut predictor = IntentPredictor::new(&temp_path).unwrap();
        
        // Wrong feature size
        let features = vec![1.0, 2.0, 3.0]; // Should be 5 elements
        let result = predictor.predict(&features, 12345);
        
        assert!(matches!(result, Err(PredictionError::FeatureSizeMismatch { .. })));
        
        std::fs::remove_file(&temp_path).unwrap();
    }
    
    #[test]
    fn test_intent_class_from_index() {
        assert_eq!(IntentClass::from_index(0).unwrap(), IntentClass::Accumulate);
        assert_eq!(IntentClass::from_index(1).unwrap(), IntentClass::Dump);
        assert_eq!(IntentClass::from_index(2).unwrap(), IntentClass::Rotate);
        assert_eq!(IntentClass::from_index(3).unwrap(), IntentClass::Spoof);
        assert_eq!(IntentClass::from_index(4).unwrap(), IntentClass::Snipe);
        
        assert!(matches!(IntentClass::from_index(5), Err(PredictionError::InvalidClassIndex(5))));
    }
}
