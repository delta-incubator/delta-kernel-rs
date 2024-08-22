use crate::error::Error;
use crate::Variant;
use serde::ser::SerializeMap;

use crate::utils::Type;
use serde::Serializer;

pub fn to_json(variant: Variant) -> Result<String, Error> {
    let writer = Vec::with_capacity(128);
    let mut serializer = serde_json::Serializer::new(writer);
    let mut obj = serializer.serialize_map(Some(variant.object_size()))?;

    for i in 0..variant.object_size() {
        variant.get_field_at_index(i)?.variant();
        match variant.get_next_type(i) {
            Type::BOOLEAN => {
                obj.serialize_key(&variant.get_metadata_key(i)?)?;
                obj.serialize_value(&variant.get_boolean(i)?)?
            }
            Type::LONG => {
                obj.serialize_key(&variant.get_metadata_key(i)?)?;
                obj.serialize_value(&variant.get_long(i)?)?
            }
            Type::STRING => {
                obj.serialize_key(&variant.get_metadata_key(i)?)?;
                obj.serialize_value(&variant.get_string(i)?)?
            }
            Type::DOUBLE => {
                obj.serialize_key(&variant.get_metadata_key(i)?)?;
                obj.serialize_value(&variant.get_double(i)?)?
            }
            Type::DECIMAL => {
                obj.serialize_key(&variant.get_metadata_key(i)?)?;
                obj.serialize_value(&variant.get_decimal(i)?)?
            }
            Type::FLOAT => {
                obj.serialize_key(&variant.get_metadata_key(i)?)?;
                obj.serialize_value(&variant.get_float(i)?)?
            }
            Type::TIMESTAMP | Type::TimestampNtz => {
                obj.serialize_key(&variant.get_metadata_key(i)?)?;
                obj.serialize_value(&variant.get_long(i)?)?
            }
            t => {
                // println!("{i}, {t} {}", &variant.get_metadata_key(i)?);
            }
        };
    }

    obj.end()?;
    Ok(String::from_utf8(serializer.into_inner())?)
}
