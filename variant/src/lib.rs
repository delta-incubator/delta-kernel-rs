#![feature(const_float_bits_conv, const_fn_floating_point_arithmetic)]

use crate::error::Error;
use crate::utils::*;

pub mod error;
pub mod json;
pub mod reader;
pub mod utils;

#[derive(Debug, Clone, Default)]
pub struct Variant<'a> {
    value: &'a [u8],
    metadata: &'a [u8],
    pos: usize,
}

impl<'a> Variant<'a> {
    const fn offset_size(&self) -> usize {
        (((self.metadata[0] >> 6) & 0x3) + 1) as usize
    }

    const fn dictionary_size(&self) -> usize {
        self.metadata[1] as usize
    }

    const fn basic_type(&self, pos: usize) -> usize {
        (self.value[pos] & BASIC_TYPE_MASK) as usize
    }

    const fn check_type<const BT: usize>(&self, pos: usize) -> Result<(), Error> {
        if self.basic_type(pos) == BT {
            Ok(())
        } else {
            Err(Error::IncorrectType(
                Type::NULL,
                self.get_type_from_pos(pos),
            ))
        }
    }

    const fn type_info(&self, pos: usize) -> usize {
        ((self.value[pos] >> BASIC_TYPE_BITS) & TYPE_INFO_MASK) as usize
    }

    const fn type_size(&self, pos: usize) -> usize {
        if ((self.type_info(pos) >> 4) & 0x1) != 0 {
            U32_SIZE
        } else {
            1
        }
    }

    const fn id_offset_sizes(&self, pos: usize) -> (usize, usize) {
        let id_size = ((self.type_info(pos) >> 2) & 0x3) + 1;
        let offset_size = (self.type_info(pos) & 0x3) + 1;
        (id_size, offset_size)
    }

    const fn object_size(&self) -> usize {
        self.value[self.pos + 1] as usize
    }

    pub fn get_metadata_key(&self, key: usize) -> Result<&'a str, Error> {
        let dict_size = self.dictionary_size();
        let size_bytes = self.type_size(self.pos);
        let (id_size, offset_size) = self.id_offset_sizes(0);
        let id_start = 1 + size_bytes;
        let id = self.value[id_start + id_size * key] as usize;
        let string_start = 1 + (dict_size + 2) * offset_size;
        let start = self.metadata[id + 2] as usize + string_start;
        let end = self.metadata[id + 2 + 1] as usize + string_start;
        Ok(std::str::from_utf8(&self.metadata[start..end])?)
    }

    pub fn get_metadata_key_string(&self, key: usize) -> Result<String, Error> {
        self.get_metadata_key(key).map(ToString::to_string)
    }

    pub const fn get_next_type(&self, idx: usize) -> Type {
        let pos = self.get_field_pos(idx);
        self.get_type_from_pos(pos)
    }

    pub fn get_field_at_index(&self, index: usize) -> Result<ObjectField<'_>, Error> {
        self.check_type::<OBJECT>(self.pos)?;
        let size_bytes = self.type_size(self.pos);
        let size = self.value[self.pos + 1..self.pos + 1 + size_bytes][0] as usize;
        let (id_size, offset_size) = self.id_offset_sizes(self.pos);
        let id_start = self.pos + 1 + size_bytes;
        let offset_start = id_start + size * id_size;
        let data_start = offset_start + (size + 1) * offset_size;
        let id = self.value[id_start + id_size * index] as usize;
        let offset_place = offset_start + offset_size * index;
        let offset = &self.value[offset_place..offset_place + offset_size];
        let key_field = self.get_metadata_key(id)?;

        Ok(ObjectField(
            key_field,
            Box::new(Variant {
                value: self.value,
                pos: data_start + offset[0] as usize,
                metadata: self.metadata,
            }),
        ))
    }

    pub const fn get_field_pos(&self, index: usize) -> usize {
        let size_bytes = self.type_size(0);
        // let size = self.value[self.pos + 1..self.pos + 1 + size_bytes][0] as usize;
        let (id_size, offset_size) = self.id_offset_sizes(0);
        let id_start = 1 + size_bytes;
        let offset_start = id_start + 8 * id_size;
        let data_start = offset_start + (8 + 1) * offset_size;
        let offset_place = offset_start + offset_size * index;
        // let offset = &self.value[offset_place..offset_place + offset_size];
        let offset = self.value[offset_place];

        data_start + offset as usize
    }

    pub fn get_array_element(&self, index: usize) -> Result<Option<Variant<'a>>, Error> {
        let type_info = self.type_info(self.pos);
        self.check_type::<ARRAY>(self.pos)?;
        let large_size = ((type_info >> 4) & 0x1) != 0;
        let size_bytes = if large_size { U32_SIZE } else { 1 };
        let size = self.value[self.pos + 1..self.pos + 1 + size_bytes][0] as usize;
        if index >= size {
            return Ok(None);
        }
        let offset_size = (type_info & 0x3) + 1;
        let offset_start = self.pos + 1 + size_bytes;
        let offset_place = offset_start + offset_size * index;
        let data_start = offset_start + (size + 1) * offset_size;
        let offset = &self.value[offset_place..offset_place + offset_size];

        Ok(Some(Variant::<'a> {
            value: self.value,
            metadata: self.metadata,
            pos: data_start + offset[0] as usize,
        }))
    }

    pub const fn get_boolean(&self, idx: usize) -> Result<bool, Error> {
        let field_pos = self.get_field_pos(idx);
        let basic_type = self.basic_type(field_pos);
        let type_info = self.type_info(field_pos);
        if basic_type != PRIMITIVE || (type_info != TRUE && type_info != FALSE) {
            return Err(Error::IncorrectType(Type::BOOLEAN, self.get_next_type(idx)));
        }

        Ok(type_info == TRUE)
    }

    pub const fn get_float(&self, idx: usize) -> Result<f32, Error> {
        let field_pos = self.get_field_pos(idx);
        let basic_type = self.basic_type(field_pos);
        let type_info = self.type_info(field_pos);
        if basic_type != PRIMITIVE || type_info != FLOAT {
            return Err(Error::IncorrectType(Type::FLOAT, self.get_next_type(idx)));
        }
        Ok(f32::from_le_bytes(
            self.read_value(field_pos + 1, field_pos + 1 + 4),
        ))
    }

    pub const fn get_double(&self, idx: usize) -> Result<f64, Error> {
        let field_pos = self.get_field_pos(idx);
        let basic_type = self.basic_type(field_pos);
        let type_info = self.type_info(field_pos);
        if basic_type != PRIMITIVE || type_info != DOUBLE {
            return Err(Error::IncorrectType(Type::DOUBLE, self.get_next_type(idx)));
        }
        Ok(f64::from_le_bytes(
            self.read_value(field_pos + 1, field_pos + 1 + 8),
        ))
    }

    pub const fn get_decimal(&self, idx: usize) -> Result<f64, Error> {
        let field_pos = self.get_field_pos(idx);
        let type_info = self.type_info(field_pos);
        self.check_type::<PRIMITIVE>(field_pos)?;

        let scale = self.value[field_pos + 1] as i64;
        match type_info {
            DECIMAL4 => {
                let value_bytes = self.read_value(field_pos + 2, field_pos + 2 + 4);
                let unsigned_value = u32::from_le_bytes(value_bytes);
                Ok(convert(unsigned_value as u64, scale))
            }
            DECIMAL8 => {
                let value_bytes = self.read_value(field_pos + 2, field_pos + 2 + 8);
                let unsigned_value = u64::from_le_bytes(value_bytes);
                Ok(convert(unsigned_value, scale))
            }
            DECIMAL16 => {
                let value_bytes = self.read_value(field_pos + 2, field_pos + 2 + 16);
                let unsigned_value = u128::from_le_bytes(value_bytes);
                Ok(convert_u128(unsigned_value, scale))
            }
            _ => Err(Error::IncorrectType(Type::DECIMAL, self.get_next_type(idx))),
        }
    }

    pub fn get_string(&self, idx: usize) -> Result<&'a str, Error> {
        let field_pos = self.get_field_pos(idx);
        let basic_type = self.basic_type(field_pos);
        let type_info = self.type_info(field_pos);
        // self.check_type::<SHORT_STR>(field_pos)?;

        if basic_type == SHORT_STR || basic_type == PRIMITIVE && type_info == LONG_STR {
            let start = if basic_type == SHORT_STR {
                field_pos + 1
            } else {
                field_pos + 1 + U32_SIZE
            };
            let length = if basic_type == SHORT_STR {
                type_info
            } else {
                self.value[field_pos + 1..field_pos + 1 + U32_SIZE][0] as usize
            };
            return Ok(std::str::from_utf8(&self.value[start..start + length])?);
        }
        Err(Error::IncorrectType(Type::STRING, self.get_next_type(idx)))
    }

    pub const fn get_long(&self, idx: usize) -> Result<u64, Error> {
        let field_pos = self.get_field_pos(idx);
        let basic_type = self.basic_type(field_pos);
        let type_info = self.type_info(field_pos);
        if basic_type != PRIMITIVE {
            return Err(Error::IncorrectType(Type::LONG, self.get_next_type(idx)));
        }
        match type_info {
            INT1 => Ok(self.value[field_pos + 1] as u64),
            INT2 => {
                Ok(u16::from_le_bytes(self.read_value(field_pos + 1, field_pos + 1 + 2)) as u64)
            }
            INT4 | DATE => {
                Ok(u32::from_le_bytes(self.read_value(field_pos + 1, field_pos + 1 + 4)) as u64)
            }
            INT8 | TIMESTAMP | TIMESTAMP_NTZ => Ok(u64::from_le_bytes(
                self.read_value(field_pos + 1, field_pos + 1 + 8),
            )),
            _ => Err(Error::IncorrectType(Type::LONG, self.get_next_type(idx))),
        }
    }

    #[inline]
    const fn read_value<const N: usize>(&self, start: usize, end: usize) -> [u8; N] {
        if end - start != N {
            return [0; N];
        }

        let (_, b) = self.value.split_at(start);
        let (r, _) = b.split_at((end + 1) - start);
        unsafe { *(r.as_ptr().cast::<[u8; N]>()) }
    }

    pub const fn get_type_from_pos(&self, field_pos: usize) -> Type {
        let basic_type = self.basic_type(field_pos);
        let type_info = self.type_info(field_pos);
        self.get_type(basic_type, type_info)
    }

    pub const fn get_type(&self, basic_type: usize, type_info: usize) -> Type {
        match basic_type {
            SHORT_STR => Type::STRING,
            OBJECT => Type::OBJECT,
            ARRAY => Type::ARRAY,
            _ => match type_info {
                NULL => Type::NULL,
                TRUE | FALSE => Type::BOOLEAN,
                INT1 | INT2 | INT4 | INT8 => Type::LONG,
                DOUBLE => Type::DOUBLE,
                DECIMAL4 | DECIMAL8 | DECIMAL16 => Type::DECIMAL,
                DATE => Type::DATE,
                TIMESTAMP => Type::TIMESTAMP,
                TIMESTAMP_NTZ => Type::TimestampNtz,
                FLOAT => Type::FLOAT,
                BINARY => Type::BINARY,
                LONG_STR => Type::STRING,
                _ => Type::NULL,
            },
        }
    }
}

#[derive(Clone, Debug)]
pub struct ObjectField<'a>(&'a str, Box<Variant<'a>>);

impl<'a> ObjectField<'a> {
    pub fn key(&self) -> &'a str {
        self.0
    }
    pub fn variant(&self) -> Box<Variant<'a>> {
        self.1.clone()
    }
}

#[cfg(test)]
pub mod tests {
    use crate::error::Error;
    use crate::Variant;

    #[test]
    pub fn test_version() -> Result<(), Error> {
        let _metadata = [
            1, 7, 0, 8, 13, 17, 19, 26, 37, 47, 107, 101, 121, 49, 49, 49, 49, 49, 97, 107, 101,
            121, 50, 107, 101, 121, 53, 104, 105, 107, 101, 121, 52, 52, 52, 52, 105, 110, 110,
            101, 114, 95, 107, 101, 121, 50, 50, 105, 110, 110, 101, 114, 95, 107, 101, 121, 50,
        ];

        let _value = [
            2, 5, 1, 3, 0, 4, 2, 2, 23, 0, 25, 8, 39, 12, 1, 21, 113, 119, 101, 114, 116, 3, 4, 0,
            2, 4, 6, 8, 12, 1, 12, 2, 12, 3, 12, 4, 12, 2, 2, 2, 6, 5, 2, 0, 7, 12, 1, 17, 98, 108,
            97, 104,
        ];

        let _int2_values = [
            2, 5, 1, 3, 0, 4, 2, 2, 23, 0, 26, 8, 40, 12, 1, 21, 113, 119, 101, 114, 116, 3, 4, 0,
            2, 4, 6, 8, 12, 1, 12, 2, 12, 3, 12, 4, 16, 206, 86, 2, 2, 6, 5, 2, 0, 7, 12, 1, 17,
            98, 108, 97, 104,
        ];

        let _int4_values = [
            2, 5, 1, 3, 0, 4, 2, 2, 23, 0, 28, 8, 42, 12, 1, 21, 113, 119, 101, 114, 116, 3, 4, 0,
            2, 4, 6, 8, 12, 1, 12, 2, 12, 3, 12, 4, 20, 142, 215, 62, 13, 2, 2, 6, 5, 2, 0, 7, 12,
            1, 17, 98, 108, 97, 104,
        ];

        let _int8_values = [
            2, 5, 1, 3, 0, 4, 2, 2, 23, 0, 32, 8, 46, 12, 1, 21, 113, 119, 101, 114, 116, 3, 4, 0,
            2, 4, 6, 8, 12, 1, 12, 2, 12, 3, 12, 4, 24, 142, 227, 136, 87, 86, 235, 214, 30, 2, 2,
            6, 5, 2, 0, 7, 12, 1, 17, 98, 108, 97, 104,
        ];

        let all_types_metadata = [
            1, 10, 0, 3, 7, 12, 18, 27, 35, 40, 46, 55, 64, 105, 110, 116, 98, 111, 111, 108, 102,
            108, 111, 97, 116, 100, 111, 117, 98, 108, 101, 115, 104, 111, 114, 116, 95, 115, 116,
            114, 108, 111, 110, 103, 95, 115, 116, 114, 97, 114, 114, 97, 121, 111, 98, 106, 101,
            99, 116, 105, 110, 110, 101, 114, 95, 111, 110, 101, 105, 110, 110, 101, 114, 95, 116,
            119, 111,
        ];
        let all_types_value = [
            2, 8, 6, 1, 3, 2, 0, 5, 7, 4, 124, 2, 9, 3, 0, 23, 142, 19, 163, 12, 1, 4, 32, 1, 32,
            0, 0, 0, 36, 9, 39, 22, 61, 186, 0, 0, 0, 0, 13, 97, 115, 100, 64, 96, 0, 0, 0, 97, 97,
            97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97,
            97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97,
            97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97,
            97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97,
            97, 97, 97, 97, 97, 97, 3, 5, 0, 2, 4, 6, 8, 10, 12, 1, 12, 2, 12, 3, 12, 4, 12, 5, 2,
            2, 8, 9, 0, 2, 14, 12, 1, 3, 3, 0, 2, 4, 6, 12, 1, 12, 2, 12, 3,
        ];

        let var = Variant {
            pos: 0,
            metadata: &all_types_metadata,
            value: &all_types_value,
        };

        // let nv = var.get_field_at_index(5)?;
        // println!("{:?}", nv.1.get_string());
        Ok(())
    }
}
