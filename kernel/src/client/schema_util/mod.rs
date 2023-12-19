mod adapter;
mod conversion;
mod fields;

pub(crate) use adapter::*;

const MAP_ROOT_DEFAULT: &str = "entries";
const MAP_KEY_DEFAULT: &str = "keys";
const MAP_VALUE_DEFAULT: &str = "values";
const LIST_ROOT_DEFAULT: &str = "element";
