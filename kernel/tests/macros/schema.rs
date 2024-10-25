use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Eq)]
#[serde(rename_all = "camelCase")]
pub enum PrimitiveType {
    String,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Clone, Eq)]
pub struct StructType {
    pub type_name: String,
    pub fields: Vec<String>,
}

impl StructType {
    pub fn new(fields: impl IntoIterator<Item = StructField>) -> Self {
        Self {
            type_name: "struct".into(),
            fields: fields.into_iter().map(|f| f.name.clone()).collect(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MapType {
    #[serde(rename = "type")]
    pub type_name: String,
    pub key_type: DataType,
    pub value_type: DataType,
    pub value_contains_null: bool,
}

impl MapType {
    pub fn new(key_type: DataType, value_type: DataType, value_contains_null: bool) -> Self {
        Self {
            type_name: "map".into(),
            key_type,
            value_type,
            value_contains_null,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Eq)]
#[serde(untagged, rename_all = "camelCase")]
pub enum DataType {
    Primitive(PrimitiveType),
    Struct(Box<StructType>),
    Map(Box<MapType>),
}

impl DataType {
    pub const STRING: Self = DataType::Primitive(PrimitiveType::String);

    pub fn struct_type(fields: impl IntoIterator<Item = StructField>) -> Self {
        StructType::new(fields).into()
    }
}

impl From<MapType> for DataType {
    fn from(map_type: MapType) -> Self {
        DataType::Map(Box::new(map_type))
    }
}

impl From<StructType> for DataType {
    fn from(struct_type: StructType) -> Self {
        DataType::Struct(Box::new(struct_type))
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Eq)]
pub struct StructField {
    pub name: String,
}
impl StructField {
    pub fn new(name: impl Into<String>, _data_type: impl Into<DataType>, _nullable: bool) -> Self {
        Self { name: name.into() }
    }
}
