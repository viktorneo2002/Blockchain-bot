// Simple FlatBuffers test
use flatbuffers::{FlatBufferBuilder, WIPOffset, Vector, Table, ForwardsUOffset};

mod flatbuffers_generated {
    pub mod model_weights_generated;
}

use flatbuffers_generated::model_weights_generated::{ModelWeights, FloatVector, ModelWeightsArgs, FloatVectorArgs};

fn main() {
    println!("Testing FlatBuffers implementation...");
    
    let mut builder = FlatBufferBuilder::new();
    
    // Create a simple FloatVector
    let values = vec![1.0, 2.0, 3.0];
    let values_vec = builder.create_vector(&values);
    let float_vector = FloatVector::create(&mut builder, &FloatVectorArgs {
        values: Some(values_vec),
    });
    
    // Create a simple ModelWeights
    let model_weights = ModelWeights::create(&mut builder, &ModelWeightsArgs {
        input_weights: None,
        hidden_weights: None,
        output_weights: None,
        input_bias: None,
        hidden_bias: None,
        output_bias: None,
        feature_count: 5,
        hidden_size: 4,
        output_size: 5,
    });
    
    builder.finish(model_weights, None);
    println!("FlatBuffers test completed successfully!");
}
