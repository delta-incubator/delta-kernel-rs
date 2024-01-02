mod adapter;
mod conversion;
mod fields;

pub(crate) use adapter::*;

const MAP_ROOT_DEFAULT: &str = "entries";
const MAP_KEY_DEFAULT: &str = "key";
const MAP_VALUE_DEFAULT: &str = "value";
const LIST_ROOT_DEFAULT: &str = "element";
