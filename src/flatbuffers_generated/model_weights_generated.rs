// Automatically generated FlatBuffers Rust code for model_weights.fbs
// Manual implementation due to flatc compiler issues on Windows

use flatbuffers::{Table, Vector, Follow, Push, EndianScalar, ForwardsUOffset, WIPOffset, FlatBufferBuilder};
use std::mem;

// Forward declarations
pub struct FloatVector<'a> {
    pub _tab: Table<'a>,
}

pub struct ModelWeights<'a> {
    pub _tab: Table<'a>,
}

// Builder structs
pub struct FloatVectorBuilder<'a> {
    pub(crate) fbb: &'a mut FlatBufferBuilder<'a>,
    pub(crate) start: flatbuffers::WIPOffset<flatbuffers::TableUnfinishedWIPOffset>,
}

impl<'a> FloatVectorBuilder<'a> {
    pub fn new(fbb: &'a mut FlatBufferBuilder<'a>) -> Self {
        let start = fbb.start_table();
        Self { fbb, start }
    }

    pub fn add_values(&mut self, values: flatbuffers::WIPOffset<Vector<f32>>) {
        self.fbb.push_slot_always::<flatbuffers::WIPOffset<Vector<f32>>>(FloatVector::VT_VALUES, values);
    }

    pub fn finish(self) -> WIPOffset<FloatVector<'a>> {
        WIPOffset::new(self.fbb.end_table(self.start))
    }
}

pub struct ModelWeightsBuilder<'a> {
    pub(crate) fbb: &'a mut FlatBufferBuilder<'a>,
    pub(crate) start: flatbuffers::WIPOffset<flatbuffers::TableUnfinishedWIPOffset>,
}

impl<'a> ModelWeightsBuilder<'a> {
    pub fn new(fbb: &'a mut FlatBufferBuilder<'a>) -> Self {
        let start = fbb.start_table();
        Self { fbb, start }
    }

    pub fn add_input_weights(&mut self, input_weights: flatbuffers::WIPOffset<Vector<ForwardsUOffset<FloatVector>>>) {
        self.fbb.push_slot_always::<flatbuffers::WIPOffset<Vector<ForwardsUOffset<FloatVector>>>>(
            ModelWeights::VT_INPUT_WEIGHTS, input_weights);
    }

    pub fn add_hidden_weights(&mut self, hidden_weights: flatbuffers::WIPOffset<Vector<ForwardsUOffset<FloatVector>>>) {
        self.fbb.push_slot_always::<flatbuffers::WIPOffset<Vector<ForwardsUOffset<FloatVector>>>>(
            ModelWeights::VT_HIDDEN_WEIGHTS, hidden_weights);
    }

    pub fn add_output_weights(&mut self, output_weights: flatbuffers::WIPOffset<Vector<ForwardsUOffset<FloatVector>>>) {
        self.fbb.push_slot_always::<flatbuffers::WIPOffset<Vector<ForwardsUOffset<FloatVector>>>>(
            ModelWeights::VT_OUTPUT_WEIGHTS, output_weights);
    }

    pub fn add_input_bias(&mut self, input_bias: flatbuffers::WIPOffset<Vector<f32>>) {
        self.fbb.push_slot_always::<flatbuffers::WIPOffset<Vector<f32>>>(
            ModelWeights::VT_INPUT_BIAS, input_bias);
    }

    pub fn add_hidden_bias(&mut self, hidden_bias: flatbuffers::WIPOffset<Vector<f32>>) {
        self.fbb.push_slot_always::<flatbuffers::WIPOffset<Vector<f32>>>(
            ModelWeights::VT_HIDDEN_BIAS, hidden_bias);
    }

    pub fn add_output_bias(&mut self, output_bias: flatbuffers::WIPOffset<Vector<f32>>) {
        self.fbb.push_slot_always::<flatbuffers::WIPOffset<Vector<f32>>>(
            ModelWeights::VT_OUTPUT_BIAS, output_bias);
    }

    pub fn add_feature_count(&mut self, feature_count: u32) {
        self.fbb.push_slot_always::<u32>(ModelWeights::VT_FEATURE_COUNT, feature_count);
    }

    pub fn add_hidden_size(&mut self, hidden_size: u32) {
        self.fbb.push_slot_always::<u32>(ModelWeights::VT_HIDDEN_SIZE, hidden_size);
    }

    pub fn add_output_size(&mut self, output_size: u32) {
        self.fbb.push_slot_always::<u32>(ModelWeights::VT_OUTPUT_SIZE, output_size);
    }

    pub fn finish(self) -> WIPOffset<ModelWeights<'a>> {
        WIPOffset::new(self.fbb.end_table(self.start))
    }
}

// Args structs
pub struct FloatVectorArgs {
    pub values: Option<Vector<f32>>,
}

impl Default for FloatVectorArgs {
    fn default() -> Self {
        FloatVectorArgs {
            values: None,
        }
    }
}

pub struct ModelWeightsArgs {
    pub input_weights: Option<Vector<ForwardsUOffset<FloatVector>>>,
    pub hidden_weights: Option<Vector<ForwardsUOffset<FloatVector>>>,
    pub output_weights: Option<Vector<ForwardsUOffset<FloatVector>>>,
    pub input_bias: Option<Vector<f32>>,
    pub hidden_bias: Option<Vector<f32>>,
    pub output_bias: Option<Vector<f32>>,
    pub feature_count: u32,
    pub hidden_size: u32,
    pub output_size: u32,
}

impl Default for ModelWeightsArgs {
    fn default() -> Self {
        ModelWeightsArgs {
            input_weights: None,
            hidden_weights: None,
            output_weights: None,
            input_bias: None,
            hidden_bias: None,
            output_bias: None,
            feature_count: 0,
            hidden_size: 0,
            output_size: 0,
        }
    }
}

impl<'a> Clone for FloatVector<'a> {
    fn clone(&self) -> Self {
        FloatVector {
            _tab: self._tab,
        }
    }
}

impl<'a> Clone for ModelWeights<'a> {
    fn clone(&self) -> Self {
        ModelWeights {
            _tab: self._tab,
        }
    }
}

impl<'a> Copy for FloatVector<'a> {}
impl<'a> Copy for ModelWeights<'a> {}

impl<'a> FloatVector<'a> {
    pub const VT_VALUES: flatbuffers::VOffsetT = 4;

    pub fn values(&self) -> Option<Vector<'a, f32>> {
        self._tab.get_vector(Self::VT_VALUES)
    }
    
    pub fn create(builder: &mut FlatBufferBuilder<'a>, args: &FloatVectorArgs) -> WIPOffset<FloatVector<'a>> {
        let values = args.values.map(|v| v.into())
            .unwrap_or_else(|| builder.create_vector(&[]));
        
        let mut inner = FloatVectorBuilder::new(builder);
        inner.add_values(values);
        inner.finish()
    }
}

impl<'a> ModelWeights<'a> {
    pub const VT_INPUT_WEIGHTS: flatbuffers::VOffsetT = 4;
    pub const VT_HIDDEN_WEIGHTS: flatbuffers::VOffsetT = 6;
    pub const VT_OUTPUT_WEIGHTS: flatbuffers::VOffsetT = 8;
    pub const VT_INPUT_BIAS: flatbuffers::VOffsetT = 10;
    pub const VT_HIDDEN_BIAS: flatbuffers::VOffsetT = 12;
    pub const VT_OUTPUT_BIAS: flatbuffers::VOffsetT = 14;
    pub const VT_FEATURE_COUNT: flatbuffers::VOffsetT = 16;
    pub const VT_HIDDEN_SIZE: flatbuffers::VOffsetT = 18;
    pub const VT_OUTPUT_SIZE: flatbuffers::VOffsetT = 20;

    pub fn input_weights(&self) -> Option<Vector<'a, ForwardsUOffset<FloatVector<'a>>>> {
        self._tab.get_vector(Self::VT_INPUT_WEIGHTS)
    }

    pub fn hidden_weights(&self) -> Option<Vector<'a, ForwardsUOffset<FloatVector<'a>>>> {
        self._tab.get_vector(Self::VT_HIDDEN_WEIGHTS)
    }

    pub fn output_weights(&self) -> Option<Vector<'a, ForwardsUOffset<FloatVector<'a>>>> {
        self._tab.get_vector(Self::VT_OUTPUT_WEIGHTS)
    }

    pub fn input_bias(&self) -> Option<Vector<'a, f32>> {
        self._tab.get_vector(Self::VT_INPUT_BIAS)
    }

    pub fn hidden_bias(&self) -> Option<Vector<'a, f32>> {
        self._tab.get_vector(Self::VT_HIDDEN_BIAS)
    }

    pub fn output_bias(&self) -> Option<Vector<'a, f32>> {
        self._tab.get_vector(Self::VT_OUTPUT_BIAS)
    }

    pub fn feature_count(&self) -> u32 {
        self._tab.get::<u32>(Self::VT_FEATURE_COUNT, Some(0)).unwrap()
    }

    pub fn hidden_size(&self) -> u32 {
        self._tab.get::<u32>(Self::VT_HIDDEN_SIZE, Some(0)).unwrap()
    }

    pub fn output_size(&self) -> u32 {
        self._tab.get::<u32>(Self::VT_OUTPUT_SIZE, Some(0)).unwrap()
    }
    
    pub fn create(builder: &mut FlatBufferBuilder<'a>, args: &ModelWeightsArgs) -> WIPOffset<ModelWeights<'a>> {
        let input_weights = args.input_weights.map(|v| v.into())
            .unwrap_or_else(|| builder.create_vector(&[]));
        let hidden_weights = args.hidden_weights.map(|v| v.into())
            .unwrap_or_else(|| builder.create_vector(&[]));
        let output_weights = args.output_weights.map(|v| v.into())
            .unwrap_or_else(|| builder.create_vector(&[]));
        let input_bias = args.input_bias.map(|v| v.into())
            .unwrap_or_else(|| builder.create_vector(&[]));
        let hidden_bias = args.hidden_bias.map(|v| v.into())
            .unwrap_or_else(|| builder.create_vector(&[]));
        let output_bias = args.output_bias.map(|v| v.into())
            .unwrap_or_else(|| builder.create_vector(&[]));
        
        let mut inner = ModelWeightsBuilder::new(builder);
        inner.add_input_weights(input_weights);
        inner.add_hidden_weights(hidden_weights);
        inner.add_output_weights(output_weights);
        inner.add_input_bias(input_bias);
        inner.add_hidden_bias(hidden_bias);
        inner.add_output_bias(output_bias);
        inner.add_feature_count(args.feature_count);
        inner.add_hidden_size(args.hidden_size);
        inner.add_output_size(args.output_size);
        inner.finish()
    }
}

impl<'a> Follow<'a> for FloatVector<'a> {
    type Inner = FloatVector<'a>;
    fn follow(buf: &'a [u8], loc: usize) -> Self::Inner {
        FloatVector {
            _tab: Table::new(buf, loc),
        }
    }
}

impl<'a> Follow<'a> for ModelWeights<'a> {
    type Inner = ModelWeights<'a>;
    fn follow(buf: &'a [u8], loc: usize) -> Self::Inner {
        ModelWeights {
            _tab: Table::new(buf, loc),
        }
    }
}
