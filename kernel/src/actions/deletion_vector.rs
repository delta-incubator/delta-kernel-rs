//! Code relating to parsing and using deletion vectors

use std::io::{Cursor, Read};
use std::sync::Arc;

use bytes::Bytes;
use delta_kernel_derive::Schema;
use roaring::RoaringTreemap;
use url::Url;

use crate::{DeltaResult, Error, FileSystemClient};

#[derive(Debug, Clone, PartialEq, Eq, Schema)]
pub struct DeletionVectorDescriptor {
    /// A single character to indicate how to access the DV. Legal options are: ['u', 'i', 'p'].
    pub storage_type: String,

    /// Three format options are currently proposed:
    /// - If `storageType = 'u'` then `<random prefix - optional><base85 encoded uuid>`:
    ///   The deletion vector is stored in a file with a path relative to the data
    ///   directory of this Delta table, and the file name can be reconstructed from
    ///   the UUID. See Derived Fields for how to reconstruct the file name. The random
    ///   prefix is recovered as the extra characters before the (20 characters fixed length) uuid.
    /// - If `storageType = 'i'` then `<base85 encoded bytes>`: The deletion vector
    ///   is stored inline in the log. The format used is the `RoaringBitmapArray`
    ///   format also used when the DV is stored on disk and described in [Deletion Vector Format].
    /// - If `storageType = 'p'` then `<absolute path>`: The DV is stored in a file with an
    ///   absolute path given by this path, which has the same format as the `path` field
    ///   in the `add`/`remove` actions.
    ///
    /// [Deletion Vector Format]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#Deletion-Vector-Format
    pub path_or_inline_dv: String,

    /// Start of the data for this DV in number of bytes from the beginning of the file it is stored in.
    /// Always None (absent in JSON) when `storageType = 'i'`.
    pub offset: Option<i32>,

    /// Size of the serialized DV in bytes (raw data size, i.e. before base85 encoding, if inline).
    pub size_in_bytes: i32,

    /// Number of rows the given DV logically removes from the file.
    pub cardinality: i64,
}

impl DeletionVectorDescriptor {
    pub fn unique_id(&self) -> String {
        if let Some(offset) = self.offset {
            format!("{}{}@{offset}", self.storage_type, self.path_or_inline_dv)
        } else {
            format!("{}{}", self.storage_type, self.path_or_inline_dv)
        }
    }

    pub fn absolute_path(&self, parent: &Url) -> DeltaResult<Option<Url>> {
        match self.storage_type.as_str() {
            "u" => {
                let path_len = self.path_or_inline_dv.len();
                if path_len < 20 {
                    return Err(Error::deletion_vector(
                        "Invalid length {path_len}, must be >20",
                    ));
                }
                let prefix_len = path_len - 20;
                let decoded = z85::decode(&self.path_or_inline_dv[prefix_len..])
                    .map_err(|_| Error::deletion_vector("Failed to decode DV uuid"))?;
                let uuid = uuid::Uuid::from_slice(&decoded)
                    .map_err(|err| Error::DeletionVector(err.to_string()))?;
                let dv_suffix = if prefix_len > 0 {
                    format!(
                        "{}/deletion_vector_{uuid}.bin",
                        &self.path_or_inline_dv[..prefix_len]
                    )
                } else {
                    format!("deletion_vector_{uuid}.bin")
                };
                let dv_path = parent
                    .join(&dv_suffix)
                    .map_err(|_| Error::DeletionVector(format!("invalid path: {dv_suffix}")))?;
                Ok(Some(dv_path))
            }
            "p" => Ok(Some(Url::parse(&self.path_or_inline_dv).map_err(|_| {
                Error::DeletionVector(format!("invalid path: {}", self.path_or_inline_dv))
            })?)),
            "i" => Ok(None),
            other => Err(Error::DeletionVector(format!(
                "Unknown storage format: '{other}'."
            ))),
        }
    }

    /// Read a dv in stored form into a [`RoaringTreemap`]
    // A few notes:
    //  - dvs write integers in BOTH big and little endian format. The magic and dv itself are
    //  little, while the version, size, and checksum are big
    //  - dvs can potentially indicate the size in the delta log, and _also_ in the file. If both
    //  are present, we assert they are the same
    pub fn read(
        &self,
        fs_client: Arc<dyn FileSystemClient>,
        parent: &Url,
    ) -> DeltaResult<RoaringTreemap> {
        match self.absolute_path(parent)? {
            None => {
                let bytes = z85::decode(&self.path_or_inline_dv)
                    .map_err(|_| Error::deletion_vector("Failed to decode DV"))?;
                RoaringTreemap::deserialize_from(&bytes[12..])
                    .map_err(|err| Error::DeletionVector(err.to_string()))
            }
            Some(path) => {
                let offset = self.offset;
                let size_in_bytes = self.size_in_bytes;

                let dv_data = fs_client
                    .read_files(vec![(path, None)])?
                    .next()
                    .ok_or(Error::missing_data("No deletion vector data"))??;

                let mut cursor = Cursor::new(dv_data);
                let mut version_buf = [0; 1];
                cursor
                    .read(&mut version_buf)
                    .map_err(|err| Error::DeletionVector(err.to_string()))?;
                let version = u8::from_be_bytes(version_buf);
                if version != 1 {
                    return Err(Error::DeletionVector(format!("Invalid version: {version}")));
                }

                if let Some(offset) = offset {
                    cursor.set_position(offset as u64);
                }
                let dv_size = read_u32(&mut cursor, Endian::Big)?;
                if dv_size != size_in_bytes as u32 {
                    return Err(Error::DeletionVector(format!(
                        "DV size mismatch. Log indicates {size_in_bytes}, file says: {dv_size}"
                    )));
                }
                let magic = read_u32(&mut cursor, Endian::Little)?;

                if magic != 1681511377 {
                    return Err(Error::DeletionVector(format!("Invalid magic: {magic}")));
                }

                // get the Bytes back out and limit it to dv_size
                let position = cursor.position();
                let mut bytes = cursor.into_inner();
                let truncate_pos = position + dv_size as u64;
                assert!(
                    truncate_pos <= usize::MAX as u64,
                    "Can't truncate as truncate_pos is > usize::MAX"
                );
                bytes.truncate(truncate_pos as usize);
                let mut cursor = Cursor::new(bytes);
                cursor.set_position(position);
                RoaringTreemap::deserialize_from(cursor)
                    .map_err(|err| Error::DeletionVector(err.to_string()))
            }
        }
    }
}

enum Endian {
    Big,
    Little,
}

/// small helper to read a big or little endian u32 from a cursor
fn read_u32(cursor: &mut Cursor<Bytes>, endian: Endian) -> DeltaResult<u32> {
    let mut buf = [0; 4];
    cursor
        .read(&mut buf)
        .map_err(|err| Error::DeletionVector(err.to_string()))?;
    match endian {
        Endian::Big => Ok(u32::from_be_bytes(buf)),
        Endian::Little => Ok(u32::from_le_bytes(buf)),
    }
}

/// helper function to convert a treemap into a boolean vector where, for index i, if the bit is
/// set, the vector will be false, and otherwise at index i the vector will be true
pub(crate) fn treemap_to_bools(treemap: RoaringTreemap) -> Vec<bool> {
    fn combine(high_bits: u32, low_bits: u32) -> usize {
        ((u64::from(high_bits) << 32) | u64::from(low_bits)) as usize
    }

    match treemap.max() {
        Some(max) => {
            // there are values in the map
            //TODO(nick) panic if max is > MAX_USIZE
            let mut result = vec![true; max as usize + 1];
            let bitmaps = treemap.bitmaps();
            for (index, bitmap) in bitmaps {
                for bit in bitmap.iter() {
                    let vec_index = combine(index, bit);
                    result[vec_index] = false;
                }
            }
            result
        }
        None => {
            // empty set, return empty vec
            vec![]
        }
    }
}

#[cfg(test)]
mod tests {
    use roaring::RoaringTreemap;
    use std::path::PathBuf;

    use super::*;
    use crate::{client::sync::SyncEngineInterface, EngineInterface};

    use super::DeletionVectorDescriptor;

    fn dv_relative() -> DeletionVectorDescriptor {
        DeletionVectorDescriptor {
            storage_type: "u".to_string(),
            path_or_inline_dv: "ab^-aqEH.-t@S}K{vb[*k^".to_string(),
            offset: Some(4),
            size_in_bytes: 40,
            cardinality: 6,
        }
    }

    fn dv_absolute() -> DeletionVectorDescriptor {
        DeletionVectorDescriptor {
            storage_type: "p".to_string(),
            path_or_inline_dv:
                "s3://mytable/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin".to_string(),
            offset: Some(4),
            size_in_bytes: 40,
            cardinality: 6,
        }
    }

    fn dv_inline() -> DeletionVectorDescriptor {
        DeletionVectorDescriptor {
            storage_type: "i".to_string(),
            path_or_inline_dv: "wi5b=000010000siXQKl0rr91000f55c8Xg0@@D72lkbi5=-{L".to_string(),
            offset: None,
            size_in_bytes: 40,
            cardinality: 6,
        }
    }

    fn dv_example() -> DeletionVectorDescriptor {
        DeletionVectorDescriptor {
            storage_type: "u".to_string(),
            path_or_inline_dv: "vBn[lx{q8@P<9BNH/isA".to_string(),
            offset: Some(1),
            size_in_bytes: 36,
            cardinality: 2,
        }
    }

    #[test]
    fn test_deletion_vector_absolute_path() {
        let parent = Url::parse("s3://mytable/").unwrap();

        let relative = dv_relative();
        let expected =
            Url::parse("s3://mytable/ab/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin")
                .unwrap();
        assert_eq!(expected, relative.absolute_path(&parent).unwrap().unwrap());

        let absolute = dv_absolute();
        let expected =
            Url::parse("s3://mytable/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin")
                .unwrap();
        assert_eq!(expected, absolute.absolute_path(&parent).unwrap().unwrap());

        let inline = dv_inline();
        assert_eq!(None, inline.absolute_path(&parent).unwrap());

        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let parent = url::Url::from_directory_path(path).unwrap();
        let dv_url = parent
            .join("deletion_vector_61d16c75-6994-46b7-a15b-8b538852e50e.bin")
            .unwrap();
        let example = dv_example();
        assert_eq!(dv_url, example.absolute_path(&parent).unwrap().unwrap());
    }

    #[test]
    fn test_deletion_vector_read() {
        let path =
            std::fs::canonicalize(PathBuf::from("./tests/data/table-with-dv-small/")).unwrap();
        let parent = url::Url::from_directory_path(path).unwrap();
        let sync_interface = SyncEngineInterface::new();
        let fs_client = sync_interface.get_file_system_client();

        let example = dv_example();
        let tree_map = example.read(fs_client, &parent).unwrap();

        let expected: Vec<u64> = vec![0, 9];
        let found = tree_map.iter().collect::<Vec<_>>();
        assert_eq!(found, expected)
    }

    // this test is ignored by default as it's expensive to allocate such big vecs full of `true`. you can run it via:
    // cargo test actions::action_definitions::tests::test_dv_to_bools
    #[test]
    #[ignore]
    fn test_dv_to_bools() {
        let mut rb = RoaringTreemap::new();
        rb.insert(0);
        rb.insert(2);
        rb.insert(7);
        rb.insert(30854);
        rb.insert(4294967297);
        rb.insert(4294967300);
        let bools = super::treemap_to_bools(rb);
        let mut expected = vec![true; 4294967301];
        expected[0] = false;
        expected[2] = false;
        expected[7] = false;
        expected[30854] = false;
        expected[4294967297] = false;
        expected[4294967300] = false;
        assert_eq!(bools, expected);
    }
}
