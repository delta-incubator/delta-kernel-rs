use std::borrow::{Borrow, Cow};
use std::fmt::{self, Display};
use std::ops::Deref;

use serde::{Deserialize, Deserializer, Serialize};

/// A metadata key with static storage for well-known annotations.
///
/// Known keys can be cloned without allocating. Unknown keys retain their owned spelling without
/// global interning. Equality, hashing, and serialization use the string contents in either case.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct MetadataKey(Cow<'static, str>);

impl MetadataKey {
    /// Returns the key's string contents.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for MetadataKey {
    fn from(key: String) -> Self {
        Self(match known_key(&key) {
            Some(key) => Cow::Borrowed(key),
            None => Cow::Owned(key),
        })
    }
}

impl From<&str> for MetadataKey {
    fn from(key: &str) -> Self {
        Self(match known_key(key) {
            Some(key) => Cow::Borrowed(key),
            None => Cow::Owned(key.to_owned()),
        })
    }
}

impl<'de> Deserialize<'de> for MetadataKey {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct KeyVisitor;
        impl<'de> serde::de::Visitor<'de> for KeyVisitor {
            type Value = MetadataKey;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a metadata key")
            }

            fn visit_str<E: serde::de::Error>(self, key: &str) -> Result<Self::Value, E> {
                Ok(key.into())
            }

            fn visit_string<E: serde::de::Error>(self, key: String) -> Result<Self::Value, E> {
                Ok(key.into())
            }
        }
        deserializer.deserialize_string(KeyVisitor)
    }
}

impl Borrow<str> for MetadataKey {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl AsRef<str> for MetadataKey {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Deref for MetadataKey {
    type Target = str;
    fn deref(&self) -> &str {
        self.as_str()
    }
}

impl Display for MetadataKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

fn known_key(key: &str) -> Option<&'static str> {
    match key {
        "comment" => Some("comment"),
        "delta.columnMapping.id" => Some("delta.columnMapping.id"),
        "delta.columnMapping.physicalName" => Some("delta.columnMapping.physicalName"),
        "delta.columnMapping.nested.ids" => Some("delta.columnMapping.nested.ids"),
        "delta.typeChanges" => Some("delta.typeChanges"),
        "CURRENT_DEFAULT" => Some("CURRENT_DEFAULT"),
        "EXISTS_DEFAULT" => Some("EXISTS_DEFAULT"),
        "delta.generationExpression" => Some("delta.generationExpression"),
        "parquet.field.id" => Some("parquet.field.id"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use rstest::rstest;

    use super::*;

    #[rstest]
    fn known_keys_share_storage_across_construction_and_clone(
        #[values(
            "comment",
            "delta.columnMapping.id",
            "delta.columnMapping.physicalName",
            "delta.columnMapping.nested.ids",
            "delta.typeChanges",
            "CURRENT_DEFAULT",
            "EXISTS_DEFAULT",
            "delta.generationExpression",
            "parquet.field.id"
        )]
        spelling: &str,
    ) {
        let borrowed = MetadataKey::from(spelling);
        let owned = MetadataKey::from(spelling.to_owned());
        let decoded: MetadataKey =
            serde_json::from_str(&serde_json::to_string(spelling).unwrap()).unwrap();
        for key in [borrowed.clone(), owned, decoded] {
            assert!(matches!(key.0, Cow::Borrowed(_)));
            assert_eq!(key, borrowed);
            assert_eq!(key.as_ptr(), borrowed.as_ptr());
        }
    }

    #[rstest]
    #[case("custom.key")]
    #[case("Comment")]
    #[case("delta.columnMapping.id.extra")]
    #[case("custom.\u{03bb}")]
    fn unknown_keys_keep_their_allocation_and_spelling(#[case] spelling: &str) {
        let original = spelling.to_owned();
        let pointer = original.as_ptr();
        let key = MetadataKey::from(original);
        assert!(matches!(key.0, Cow::Owned(_)));
        assert_eq!(key.as_ptr(), pointer);
        assert_eq!(key.as_str(), spelling);
        assert_eq!(key, MetadataKey::from(spelling));
        assert_ne!(key.as_ptr(), key.clone().as_ptr());
    }

    #[test]
    fn json_roundtrip_and_string_lookup_preserve_key_semantics() {
        let json = r#"{"comment":1,"custom.key":2,"delta.columnMapping.id":3}"#;
        let keys: HashMap<MetadataKey, i32> = serde_json::from_str(json).unwrap();
        assert_eq!(keys.get("comment"), Some(&1));
        assert_eq!(keys.get("custom.key"), Some(&2));
        let (known, _) = keys.get_key_value("comment").unwrap();
        let (custom, _) = keys.get_key_value("custom.key").unwrap();
        assert!(matches!(known.0, Cow::Borrowed(_)));
        assert!(matches!(custom.0, Cow::Owned(_)));
        assert_eq!(
            serde_json::to_value(&keys).unwrap(),
            serde_json::from_str::<serde_json::Value>(json).unwrap()
        );
    }

    #[test]
    fn escaped_json_keys_use_the_decoded_spelling() {
        let mut keys: HashMap<MetadataKey, i32> =
            serde_json::from_str(r#"{"comm\u0065nt":1,"comment":2,"custom\"key":3}"#).unwrap();
        assert_eq!(keys.len(), 2);
        assert_eq!(keys.remove("comment"), Some(2));
        assert_eq!(keys.remove("custom\"key"), Some(3));
    }
}
