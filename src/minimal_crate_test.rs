// Minimal crate test to verify flatbuffers crate works independently
extern crate flatbuffers;

use flatbuffers::FlatBufferBuilder;

fn main() {
    let mut builder = FlatBufferBuilder::new();
    println!("FlatBufferBuilder created successfully!");
}
