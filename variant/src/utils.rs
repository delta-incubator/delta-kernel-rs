use std::fmt::{Display, Formatter};

pub const BASIC_TYPE_BITS: u8 = 2;
pub const BASIC_TYPE_MASK: u8 = 0x3;
pub const TYPE_INFO_MASK: u8 = 0x3F;
pub const VERSION: u8 = 1;
pub const VERSION_MASK: u8 = 0x0F;
pub const U8_MAX: u8 = 0xFF;
pub const U16_MAX: u16 = 0xFFFF;
pub const U24_MAX: i32 = 0xFFFFFF;
pub const U24_SIZE: usize = 3;
pub const U32_SIZE: usize = 4;
pub const SIZE_LIMIT: usize = (U24_MAX + 1) as usize;
pub const MAX_DECIMAL4_PRECISION: i32 = 9;
pub const MAX_DECIMAL8_PRECISION: i32 = 18;
pub const MAX_DECIMAL16_PRECISION: i32 = 38;
pub const PRIMITIVE: usize = 0;
pub const SHORT_STR: usize = 1;
pub const OBJECT: usize = 2;
pub const ARRAY: usize = 3;
pub const NULL: usize = 0;
pub const TRUE: usize = 1;
pub const FALSE: usize = 2;
pub const INT1: usize = 3;
pub const INT2: usize = 4;
pub const INT4: usize = 5;
pub const INT8: usize = 6;
pub const DOUBLE: usize = 7;
pub const DECIMAL4: usize = 8;
pub const DECIMAL8: usize = 9;
pub const DECIMAL16: usize = 10;
pub const DATE: usize = 11;
pub const TIMESTAMP: usize = 12;
pub const TIMESTAMP_NTZ: usize = 13;
pub const FLOAT: usize = 14;
pub const BINARY: usize = 15;
pub const LONG_STR: usize = 16;

#[repr(usize)]
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Type {
    NULL,
    OBJECT,
    ARRAY,
    BOOLEAN,
    LONG,
    STRING,
    DOUBLE,
    DECIMAL,
    DATE,
    TIMESTAMP,
    TimestampNtz,
    FLOAT,
    BINARY,
}

impl Display for Type {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Type::OBJECT => write!(f, "OBJECT"),
            Type::ARRAY => write!(f, "ARRAY"),
            Type::NULL => write!(f, "NULL"),
            Type::BOOLEAN => write!(f, "BOOLEAN"),
            Type::LONG => write!(f, "LONG"),
            Type::STRING => write!(f, "STRING"),
            Type::DOUBLE => write!(f, "DOUBLE"),
            Type::DECIMAL => write!(f, "DECIMAL"),
            Type::DATE => write!(f, "DATE"),
            Type::TIMESTAMP => write!(f, "TIMESTAMP"),
            Type::TimestampNtz => write!(f, "TIMESTAMP_NTZ"),
            Type::FLOAT => write!(f, "FLOAT"),
            Type::BINARY => write!(f, "BINARY"),
        }
    }
}

pub const fn convert(value: u64, scale: i64) -> f64 {
    value as f64 * POWER_TABLE[scale as usize]
}

pub const fn convert_u128(value: u128, scale: i64) -> f64 {
    value as f64 * POWER_TABLE[scale as usize]
}

const POWER_TABLE: [f64; 33] = [
    0.1, 0.1, 0.01, 0.001, 0.0001, 1e-5, 1e-6, 1e-7, 1e-8, 1e-9, 1e-10, 1e-11, 1e-12, 1e-13, 1e-14,
    1e-15, 1e-16, 1e-17, 1e-18, 1e-19, 1e-20, 1e-21, 1e-22, 1e-23, 1e-24, 1e-25, 1e-26, 1e-27,
    1e-28, 1e-29, 1e-30, 1e-31, 1e-32,
];
