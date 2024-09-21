use crate::engine_data::{EngineMap, OptMapItem};
use crate::{
    engine_data::{GetData, ListItem, MapItem},
    DeltaResult,
};
use arrow_array::cast::AsArray;
use arrow_array::{
    types::{GenericStringType, Int32Type, Int64Type},
    Array, BooleanArray, GenericByteArray, GenericListArray, MapArray, OffsetSizeTrait,
    PrimitiveArray,
};
use std::collections::HashMap;
// actual impls (todo: could macro these)

impl<'a> GetData<'a> for BooleanArray {
    fn get_bool(&self, row_index: usize, _field_name: &str) -> DeltaResult<Option<bool>> {
        if self.is_valid(row_index) {
            Ok(Some(self.value(row_index)))
        } else {
            Ok(None)
        }
    }
}

impl<'a> GetData<'a> for PrimitiveArray<Int32Type> {
    fn get_int(&self, row_index: usize, _field_name: &str) -> DeltaResult<Option<i32>> {
        if self.is_valid(row_index) {
            Ok(Some(self.value(row_index)))
        } else {
            Ok(None)
        }
    }
}

impl<'a> GetData<'a> for PrimitiveArray<Int64Type> {
    fn get_long(&self, row_index: usize, _field_name: &str) -> DeltaResult<Option<i64>> {
        if self.is_valid(row_index) {
            Ok(Some(self.value(row_index)))
        } else {
            Ok(None)
        }
    }
}

impl<'a> GetData<'a> for GenericByteArray<GenericStringType<i32>> {
    fn get_str(&'a self, row_index: usize, _field_name: &str) -> DeltaResult<Option<&'a str>> {
        if self.is_valid(row_index) {
            Ok(Some(self.value(row_index)))
        } else {
            Ok(None)
        }
    }
}

impl<'a, OffsetSize> GetData<'a> for GenericListArray<OffsetSize>
where
    OffsetSize: OffsetSizeTrait,
{
    fn get_list(
        &'a self,
        row_index: usize,
        _field_name: &str,
    ) -> DeltaResult<Option<ListItem<'a>>> {
        if self.is_valid(row_index) {
            Ok(Some(ListItem::new(self, row_index)))
        } else {
            Ok(None)
        }
    }

    fn get_map_opt(
        &'a self,
        row_index: usize,
        _field_name: &str,
    ) -> DeltaResult<Option<OptMapItem<'a>>> {
        if self.is_valid(row_index) {
            Ok(Some(OptMapItem::new(self, row_index)))
        } else {
            Ok(None)
        }
    }
}

// Maps are internally represented as a struct of key/value columns, so this impl assumes the list
// has those structs for it's members
impl<OffsetSize: OffsetSizeTrait> EngineMap for GenericListArray<OffsetSize> {
    fn get(&self, row_index: usize, key: &str) -> Option<&str> {
        let offsets = self.offsets();
        let start_offset = offsets[row_index].as_usize();
        let count = offsets[row_index + 1].as_usize() - start_offset;
        let key_col = self.values().as_struct().column_by_name("key").unwrap();
        let keys = key_col.as_string::<i32>();
        for (idx, map_key) in keys.iter().enumerate().skip(start_offset).take(count) {
            if let Some(map_key) = map_key {
                if key == map_key {
                    let vals = self.values().as_string::<i32>();
                    return Some(vals.value(idx));
                }
            }
        }
        None
    }

    fn materialize(&self, row_index: usize) -> HashMap<String, String> {
        let mut ret = HashMap::new();
        let map_val = self.value(row_index);
        let map_struct = map_val.as_struct();
        let keys = map_struct.column(0).as_string::<i32>();
        let values = map_struct.column(1).as_string::<i32>();
        for (key, value) in keys.iter().zip(values.iter()) {
            if let (Some(key), Some(value)) = (key, value) {
                ret.insert(key.into(), value.into());
            }
        }
        ret
    }

    fn materialize_opt(&self, row_index: usize) -> HashMap<String, Option<String>> {
        let mut ret = HashMap::new();
        let map_val = self.value(row_index);
        let map_struct = map_val.as_struct();
        let keys = map_struct.column(0).as_string::<i32>();
        let values = map_struct.column(1).as_string::<i32>();
        for (key, value) in keys.iter().zip(values.iter()) {
            if let (Some(key), value) = (key, value) {
                ret.insert(key.into(), value.map(Into::into));
            }
        }
        ret
    }
}

impl<'a> GetData<'a> for MapArray {
    fn get_map(&'a self, row_index: usize, _field_name: &str) -> DeltaResult<Option<MapItem<'a>>> {
        if self.is_valid(row_index) {
            Ok(Some(MapItem::new(self, row_index)))
        } else {
            Ok(None)
        }
    }
    fn get_map_opt(
        &'a self,
        row_index: usize,
        _field_name: &str,
    ) -> DeltaResult<Option<OptMapItem<'a>>> {
        if self.is_valid(row_index) {
            Ok(Some(OptMapItem::new(self, row_index)))
        } else {
            Ok(None)
        }
    }
}
