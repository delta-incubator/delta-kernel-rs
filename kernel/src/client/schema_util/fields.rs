use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, ListArray, MapArray, StructArray};
use arrow_schema::{ArrowError, DataType, Field, FieldRef, Fields};

use super::{LIST_ROOT_DEFAULT, MAP_KEY_DEFAULT, MAP_ROOT_DEFAULT, MAP_VALUE_DEFAULT};

pub(crate) trait FieldsExt {
    fn visit_fields<F: FnMut(usize, &mut ColumnPath, &FieldRef) -> Result<(), ArrowError>>(
        &self,
        filter: F,
    ) -> Result<(), ArrowError>;

    fn pick_arrays<
        F: FnMut(usize, &ColumnPath, &FieldRef) -> Result<ArrayRef, ArrowError>,
        G: FnMut(&ColumnPath, &FieldRef) -> Result<ArrayRef, ArrowError>,
    >(
        &self,
        filter: F,
        picker: G,
    ) -> Result<Vec<ArrayRef>, ArrowError>;

    fn leaf_map(&self) -> Result<Vec<(ColumnPath, (usize, FieldRef))>, ArrowError>;
}

impl FieldsExt for Fields {
    fn leaf_map(&self) -> Result<Vec<(ColumnPath, (usize, FieldRef))>, ArrowError> {
        let mut leaf_map = Vec::new();
        let visit = |idx: usize, path: &mut ColumnPath, field: &FieldRef| {
            leaf_map.push((path.clone(), (idx, field.clone())));
            Ok(())
        };
        self.visit_fields(visit)?;
        Ok(leaf_map)
    }

    fn visit_fields<F: FnMut(usize, &mut ColumnPath, &FieldRef) -> Result<(), ArrowError>>(
        &self,
        mut filter: F,
    ) -> Result<(), ArrowError> {
        fn visit_field<F: FnMut(&FieldRef, &mut ColumnPath) -> Result<(), ArrowError>>(
            f: &FieldRef,
            visit: &mut F,
            path: &mut ColumnPath,
        ) -> Result<(), ArrowError> {
            use DataType::*;

            path.push_field(f);

            let v = match f.data_type() {
                Dictionary(_, v) => v.as_ref(),       // Key must be integer
                RunEndEncoded(_, v) => v.data_type(), // Run-ends must be integer
                d => d,
            };
            match v {
                List(child) => {
                    visit_field(child, visit, path)?;
                }
                LargeList(child) => {
                    visit_field(child, visit, path)?;
                }
                Map(child, _ordered) => {
                    visit_field(child, visit, path)?;
                }
                FixedSizeList(child, _size) => {
                    visit_field(child, visit, path)?;
                }
                Struct(fields) => {
                    if let Some(ArrayScope::Map) = path.scope() {
                        // Structs contained in a map should have exactly two child fields
                        if let (Some(key), Some(value)) = (fields.first(), fields.last()) {
                            visit_field(
                                key,
                                visit,
                                &mut path.with_scope(Some(ArrayScope::MapKey)),
                            )?;
                            visit_field(
                                value,
                                visit,
                                &mut path.with_scope(Some(ArrayScope::MapValue)),
                            )?;
                        }
                    } else {
                        for field in fields {
                            visit_field(field, visit, &mut path.clone())?;
                        }
                    };
                }
                Union(fields, _mode) => {
                    for (_, field) in fields.iter() {
                        visit_field(field, visit, path)?;
                    }
                }
                _ => {
                    visit(f, path)?;
                }
            };
            // let d = match f.data_type() {
            //     Dictionary(k, _) => Dictionary(k.clone(), Box::new(d)),
            //     RunEndEncoded(v, f) => {
            //         RunEndEncoded(v.clone(), Arc::new(f.as_ref().clone().with_data_type(d)))
            //     }
            //     _ => d,
            // };
            Ok(())
        }

        let mut leaf_idx = 0;
        let mut visitor = |f: &FieldRef, path: &mut ColumnPath| {
            let t = filter(leaf_idx, path, f);
            leaf_idx += 1;
            t
        };

        for f in self.iter() {
            visit_field(f, &mut visitor, &mut ColumnPath::new())?;
        }

        Ok(())
    }

    fn pick_arrays<
        F: FnMut(usize, &ColumnPath, &FieldRef) -> Result<ArrayRef, ArrowError>,
        G: FnMut(&ColumnPath, &FieldRef) -> Result<ArrayRef, ArrowError>,
    >(
        &self,
        mut filter: F,
        mut picker: G,
    ) -> Result<Vec<ArrayRef>, ArrowError> {
        fn pick_array<
            F: FnMut(&FieldRef, &mut ColumnPath) -> Result<ArrayRef, ArrowError>,
            G: FnMut(&ColumnPath, &FieldRef) -> Result<ArrayRef, ArrowError>,
        >(
            f: &FieldRef,
            visit: &mut F,
            pick: &mut G,
            path: &mut ColumnPath,
        ) -> Result<ArrayRef, ArrowError> {
            use DataType::*;

            path.push_field(f);

            let v = match f.data_type() {
                Dictionary(_, v) => v.as_ref(),       // Key must be integer
                RunEndEncoded(_, v) => v.data_type(), // Run-ends must be integer
                d => d,
            };
            let d: ArrayRef = match v {
                List(child) => {
                    let list_arr = pick(path, f)?;
                    let list_arr = list_arr.as_any().downcast_ref::<ListArray>().unwrap();
                    let items = pick_array(child, visit, pick, path)?;
                    Arc::new(ListArray::try_new(
                        child.clone(),
                        list_arr.offsets().clone(),
                        items,
                        list_arr.nulls().cloned(),
                    )?)
                }
                LargeList(_child) => {
                    //LargeList(pick_array(child, visit, path)?)
                    todo!()
                }
                Map(child, ordered) => {
                    let entries = pick_array(child, visit, pick, path)?;
                    let entries = entries
                        .as_any()
                        .downcast_ref::<StructArray>()
                        .unwrap()
                        .clone();

                    let map_arr = pick(path, f)?;
                    let map_arr = map_arr.as_any().downcast_ref::<MapArray>().unwrap();

                    Arc::new(MapArray::try_new(
                        Arc::new(Field::new(
                            MAP_ROOT_DEFAULT,
                            entries.data_type().clone(),
                            false,
                        )),
                        map_arr.offsets().clone(),
                        entries,
                        // NOTE: map array should never contain nulls
                        None,
                        *ordered,
                    )?)
                }
                FixedSizeList(_child, _size) => {
                    // FixedSizeList(pick_array(child, visit, path)?, *size)
                    todo!()
                }
                Struct(fields) => {
                    let struct_arr = pick(path, f)?;
                    if matches!(path.scope(), Some(ArrayScope::Map)) {
                        let map_arr = struct_arr.as_any().downcast_ref::<MapArray>().unwrap();
                        Arc::new(map_arr.entries().clone())
                    } else {
                        let arrays: Vec<_> = fields
                            .iter()
                            .map(|f| {
                                Ok((
                                    f.name().as_str(),
                                    pick_array(f, visit, pick, &mut path.clone())?,
                                ))
                            })
                            .collect::<Result<_, ArrowError>>()?;
                        Arc::new(StructArray::try_from(arrays)?)
                    }
                }
                Union(_fields, _mode) => {
                    todo!()
                }
                _ => {
                    return visit(f, path);
                }
            };

            let d = match f.data_type() {
                Dictionary(_k, _) => todo!(),
                RunEndEncoded(_v, _f) => todo!(),
                _ => d,
            };

            Ok(d)
        }

        let mut leaf_idx = 0;
        let mut visitor = |f: &FieldRef, path: &mut ColumnPath| {
            let t = filter(leaf_idx, path, f);
            leaf_idx += 1;
            t
        };

        self.iter()
            .map(|f| pick_array(f, &mut visitor, &mut picker, &mut ColumnPath::new()))
            .collect::<Result<Vec<_>, _>>()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum ArrayScope {
    ListRoot,
    Map,
    MapRoot,
    MapKey,
    MapValue,
}

#[derive(Clone, Debug, Eq, PartialOrd, Ord)]
pub(crate) enum ColumnSegment {
    Field(String),
    MapRoot(String),
    MapKey(String),
    MapValue(String),
    ListRoot(String),
}

impl AsRef<str> for ColumnSegment {
    fn as_ref(&self) -> &str {
        match self {
            ColumnSegment::Field(name) => name.as_ref(),
            ColumnSegment::MapRoot(_) => MAP_ROOT_DEFAULT,
            ColumnSegment::MapKey(_) => MAP_KEY_DEFAULT,
            ColumnSegment::MapValue(_) => MAP_VALUE_DEFAULT,
            ColumnSegment::ListRoot(_) => LIST_ROOT_DEFAULT,
        }
    }
}

impl PartialEq for ColumnSegment {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (ColumnSegment::Field(a), ColumnSegment::Field(b)) => a == b,
            (ColumnSegment::MapRoot(_), ColumnSegment::MapRoot(_)) => true,
            (ColumnSegment::MapKey(_), ColumnSegment::MapKey(_)) => true,
            (ColumnSegment::MapValue(_), ColumnSegment::MapValue(_)) => true,
            (ColumnSegment::ListRoot(_), ColumnSegment::ListRoot(_)) => true,
            _ => false,
        }
    }
}

impl ColumnSegment {
    pub(crate) fn name(&self) -> String {
        match self {
            ColumnSegment::Field(name) => name.clone(),
            ColumnSegment::MapRoot(name) => name.clone(),
            ColumnSegment::MapKey(name) => name.clone(),
            ColumnSegment::MapValue(name) => name.clone(),
            ColumnSegment::ListRoot(name) => name.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialOrd, Ord)]
pub(crate) struct ColumnPath {
    pub(crate) segments: Vec<ColumnSegment>,
    scope: Option<ArrayScope>,
}

impl ColumnPath {
    #[cfg(test)]
    pub(crate) fn new_from_segemnts(segments: impl IntoIterator<Item = ColumnSegment>) -> Self {
        Self {
            segments: segments.into_iter().collect(),
            scope: None,
        }
    }

    pub(crate) fn new() -> Self {
        Self {
            segments: Vec::new(),
            scope: None,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn contains(&self, other: &Self) -> bool {
        other
            .segments
            .iter()
            .zip(self.segments.iter())
            .all(|(a, b)| a == b)
    }

    pub(crate) fn scope(&self) -> &Option<ArrayScope> {
        &self.scope
    }

    pub(crate) fn identifiers(&self) -> Vec<&str> {
        self.segments.iter().map(|s| s.as_ref()).collect()
    }

    pub(crate) fn names(&self) -> Vec<String> {
        self.segments.iter().map(|s| s.name()).collect()
    }

    pub(crate) fn parquet_names(&self) -> Vec<String> {
        self.segments
            .iter()
            .flat_map(|s| {
                if matches!(s, ColumnSegment::ListRoot(_)) {
                    vec!["list".into(), "element".into()]
                } else {
                    vec![s.name()]
                }
            })
            .collect()
    }

    pub(crate) fn push_field(&mut self, field: &FieldRef) {
        match field.data_type() {
            DataType::Map(_, _) => self.push_scope(field.name(), Some(ArrayScope::MapRoot)),
            DataType::List(_) | DataType::LargeList(_) => {
                self.push_scope(field.name(), Some(ArrayScope::ListRoot))
            }
            _ => self.push_scope(field.name(), None),
        }
    }

    fn push_scope(&mut self, name: &str, scope: Option<ArrayScope>) {
        match self.scope {
            Some(ArrayScope::MapRoot) => {
                self.push(ColumnSegment::MapRoot(name.to_string()));
                self.scope = Some(ArrayScope::Map);
            }
            Some(ArrayScope::MapKey) => {
                self.push(ColumnSegment::MapKey(name.to_string()));
                self.scope = None;
            }
            Some(ArrayScope::MapValue) => {
                self.push(ColumnSegment::MapValue(name.to_string()));
                self.scope = None;
            }
            Some(ArrayScope::ListRoot) => {
                self.push(ColumnSegment::ListRoot(name.to_string()));
                self.scope = scope;
            }
            _ => {
                self.push(ColumnSegment::Field(name.to_string()));
                self.scope = scope;
            }
        }
    }

    pub(crate) fn with_scope(&mut self, scope: Option<ArrayScope>) -> Self {
        let mut new = self.clone();
        new.scope = scope;
        new
    }

    pub(crate) fn push(&mut self, segment: ColumnSegment) {
        self.segments.push(segment);
    }
}

impl IntoIterator for ColumnPath {
    type Item = ColumnSegment;
    type IntoIter = std::vec::IntoIter<ColumnSegment>;

    fn into_iter(self) -> Self::IntoIter {
        self.segments.into_iter()
    }
}

impl PartialEq for ColumnPath {
    fn eq(&self, other: &Self) -> bool {
        self.segments
            .iter()
            .zip(other.segments.iter())
            .all(|(a, b)| a == b)
    }
}

impl Eq for ColumnPath {}

impl<'a> IntoIterator for &'a ColumnPath {
    type Item = &'a ColumnSegment;
    type IntoIter = std::slice::Iter<'a, ColumnSegment>;

    fn into_iter(self) -> Self::IntoIter {
        self.segments.iter()
    }
}

impl Hash for ColumnPath {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.identifiers().hash(state);
    }
}

impl ColumnPath {}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn test_column_segment() {
        let s1 = ColumnSegment::Field("options".into());
        let s2 = ColumnSegment::MapRoot("entries".into());
        let s3 = ColumnSegment::MapValue("values".into());
        let s4 = ColumnSegment::MapKey("values".into());
        assert_eq!(s1, s1);
        assert_eq!(s2, s2);
        assert_eq!(s3, s3);
        assert_eq!(s4, s4);
        assert_ne!(s1, s2);
        assert_ne!(s1, s3);
        assert_ne!(s1, s4);
        assert_ne!(s2, s3);
        assert_ne!(s2, s4);
        assert_ne!(s3, s4);

        let s5 = ColumnSegment::MapRoot("asd".into());
        let s6 = ColumnSegment::MapValue("fgh".into());
        let s7 = ColumnSegment::MapKey("jkl".into());
        assert_eq!(s2, s5);
        assert_eq!(s3, s6);
        assert_eq!(s4, s7);
    }

    #[test]
    fn test_column_path_comparison() {
        let p1 = ColumnPath::new_from_segemnts(vec![
            ColumnSegment::Field("options".into()),
            ColumnSegment::MapRoot("entries".into()),
            ColumnSegment::MapValue("values".into()),
        ]);
        let p2 = ColumnPath::new_from_segemnts(vec![
            ColumnSegment::Field("options".into()),
            ColumnSegment::MapRoot("key_value".into()),
            ColumnSegment::MapValue("value".into()),
        ]);
        assert!(p1 == p2);
        assert!(p1.contains(&p2));

        let mut map = HashMap::new();
        map.insert(p1, 10);
        assert_eq!(map.get(&p2), Some(&10));

        let p3 = ColumnPath::new_from_segemnts(vec![
            ColumnSegment::Field("other".into()),
            ColumnSegment::MapRoot("key_value".into()),
            ColumnSegment::MapValue("value".into()),
        ]);
        assert_eq!(map.get(&p3), None);

        assert!(!p2.contains(&p3));
    }
}
