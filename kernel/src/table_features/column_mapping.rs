//! Code to handle column mapping, including modes and schema transforms
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use super::ReaderFeatures;
use crate::actions::Protocol;
use crate::table_properties::TableProperties;
use crate::{DeltaResult, Error};

/// Modes of column mapping a table can be in
#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ColumnMappingMode {
    /// No column mapping is applied
    None,
    /// Columns are mapped by their field_id in parquet
    Id,
    /// Columns are mapped to a physical name
    Name,
}

/// Determine the column mapping mode for a table based on the [`Protocol`] and [`TableProperties`]
pub(crate) fn column_mapping_mode(
    protocol: &Protocol,
    table_properties: &TableProperties,
) -> ColumnMappingMode {
    match table_properties.column_mapping_mode {
        Some(mode) if protocol.min_reader_version() == 2 => mode,
        Some(mode)
            if protocol.min_reader_version() == 3
                && protocol.has_reader_feature(&ReaderFeatures::ColumnMapping) =>
        {
            mode
        }
        _ => ColumnMappingMode::None,
    }
}

impl TryFrom<&str> for ColumnMappingMode {
    type Error = Error;

    fn try_from(s: &str) -> DeltaResult<Self> {
        match s.to_ascii_lowercase().as_str() {
            "none" => Ok(Self::None),
            "id" => Ok(Self::Id),
            "name" => Ok(Self::Name),
            _ => Err(Error::invalid_column_mapping_mode(s)),
        }
    }
}

impl FromStr for ColumnMappingMode {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.try_into()
    }
}

impl Default for ColumnMappingMode {
    fn default() -> Self {
        Self::None
    }
}

impl AsRef<str> for ColumnMappingMode {
    fn as_ref(&self) -> &str {
        match self {
            Self::None => "none",
            Self::Id => "id",
            Self::Name => "name",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_column_mapping_mode() {
        let table_properties: HashMap<_, _> =
            [("delta.columnMapping.mode".to_string(), "id".to_string())]
                .into_iter()
                .collect();
        let table_properties = TableProperties::new(table_properties.iter()).unwrap();

        let protocol = Protocol::try_new(2, 5, None::<Vec<String>>, None::<Vec<String>>).unwrap();
        assert_eq!(
            column_mapping_mode(&protocol, &table_properties),
            ColumnMappingMode::Id
        );

        let empty_features = Some::<[String; 0]>([]);
        let protocol =
            Protocol::try_new(3, 7, empty_features.clone(), empty_features.clone()).unwrap();
        assert_eq!(
            column_mapping_mode(&protocol, &table_properties),
            ColumnMappingMode::None
        );

        let protocol = Protocol::try_new(
            3,
            7,
            Some([ReaderFeatures::DeletionVectors]),
            empty_features,
        )
        .unwrap();
        assert_eq!(
            column_mapping_mode(&protocol, &table_properties),
            ColumnMappingMode::None
        );
    }
}
