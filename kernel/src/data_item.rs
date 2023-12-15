use std::any::TypeId;
use std::sync::Arc;

// ---- on kernel side ----

macro_rules! gen_prim_type_none_methods {
    (($fnname: ident, $typ: ty)) => {
        fn $fnname(&self) -> Option<$typ> { None }
    };
    (($fnname: ident, $typ: ty), $(($fnname_rest: ident, $typ_rest: ty)),+) => {
        gen_prim_type_none_methods!(($fnname, $typ));
        gen_prim_type_none_methods!($(($fnname_rest, $typ_rest)),+);
    };
}

macro_rules! gen_prim_impls {
    (($fnname: ident, $typ: ty)) => {
        impl DataItem for $typ {
            fn $fnname(&self) -> Option<$typ> { Some(*self) }
        }
    };
    (($fnname: ident, $typ: ty), $(($fnname_rest: ident, $typ_rest: ty)),+) => {
        gen_prim_impls!(($fnname, $typ));
        gen_prim_impls!($(($fnname_rest, $typ_rest)),+);
    };
}

// new trait, similar to Any, that wraps any type we may want to pass
// This will be a lot of code, but it's all on kernel side, also we can use macros to probably make
// it a lot nicer to write
trait DataItem {
    gen_prim_type_none_methods!(
        (as_bool, bool),
        (as_f32, f32),
        (as_f64, f64),
        (as_i32, i32),
        (as_u32, u32),
        (as_u64, u64),
        (as_str, &str),
        (as_map, &dyn MapItem) // ... and so on for all types we want to support ...
    );
}

gen_prim_impls!(
    (as_bool, bool),
    (as_f32, f32),
    (as_f64, f64),
    (as_i32, i32),
    (as_u32, u32),
    (as_u64, u64)
);

impl DataItem for &str {
    fn as_str(&self) -> Option<&str> {
        Some(self)
    }
}

// a map that can be a DataItem
trait MapItem {
    fn get<'a>(&'a self, key: &str) -> Option<&'a str>;
}

impl DataItem for &dyn MapItem {
    fn as_map(&self) -> Option<&dyn MapItem> {
        Some(*self)
    }
}

// Something that can be called back to visit extracted data.
trait DataVisitor {
    fn visit(&mut self, val: &[Option<&dyn DataItem>]);
}

// Just a dummy action to show what we might extract into
#[derive(Debug)]
#[allow(dead_code)]
struct Action {
    i: u32,
    s: String,
    o: Option<bool>,
    m: String,
}

#[derive(Debug, Default)]
struct ActionVisitor {
    action: Option<Action>,
}

impl DataVisitor for ActionVisitor {
    fn visit(&mut self, vals: &[Option<&dyn DataItem>]) {
        let i = vals[0]
            .expect("Action must have an i")
            .as_u32()
            .expect("i must be u32");
        let s = vals[1]
            .expect("Action must have an s")
            .as_str()
            .expect("s must be str");
        let o = vals[2].map(|o| o.as_bool().expect("o must be bool"));
        let map = vals[3]
            .expect("Action mut have a map")
            .as_map()
            .expect("m must be a map");
        let m = map.get("thekey").expect("must have thekey");
        self.action = Some(Action {
            i,
            s: s.to_string(),
            o,
            m: m.to_string(),
        });
    }
}

// Can't use `:Eq / :PartialEq` as that's generic, and we want to return this trait as an object
// below. We require the 'static bound so we can be sure the TypeId will live long enough to
// return. In practice this just means that the type must be fully defined and not a generated type.
trait TypeTag: 'static {
    fn tag_id(&self) -> TypeId {
        TypeId::of::<Self>()
    }

    fn eq(&self, other: &dyn TypeTag) -> bool {
        let my_id = self.tag_id();
        let other_id = other.tag_id();
        my_id == other_id
    }
}

trait EngineData {
    fn type_tag(&self) -> &dyn TypeTag;
}

// thing the engine must implement, trait is defined on the kernel side
trait DataExtractor {
    // blob will actually be the opaque returned type from a handler's 'read'
    // schema will be a type we build that the engine understands
    fn extract(&self, blob: &dyn EngineData, schema: &str, visitor: &mut dyn DataVisitor);
}

trait JsonReader {
    fn get_data(&self) -> dyn EngineData;
}

trait EngineClient {
    fn get_json_client(&self) -> Arc<dyn JsonReader>;
    // include_bool is just for this demo to show passing with/without a value
    fn get_data_extractor(&self, include_bool: bool) -> Arc<dyn DataExtractor>;
}

// ---- on engine/connector side ----

#[derive(Default)]
struct StrTypeTag;
impl TypeTag for StrTypeTag {}

#[derive(Default)]
struct StrExtractor {
    expected_tag: StrTypeTag,
    include_bool: bool,
}

impl EngineData for String {
    fn type_tag(&self) -> &dyn TypeTag {
        &StrTypeTag
    }
}

struct ExampleMap {
    val: String,
}

impl MapItem for ExampleMap {
    fn get<'a>(&'a self, _key: &str) -> Option<&'a str> {
        Some(self.val.as_str())
    }
}

impl DataExtractor for StrExtractor {
    fn extract(&self, blob: &dyn EngineData, _schema: &str, visitor: &mut dyn DataVisitor) {
        assert!(self.expected_tag.eq(blob.type_tag())); // Ensure correct data type
        let mut x: Vec<Option<&dyn DataItem>> = vec![];
        // now the engine would actually extract values based on a schema and put them on the vec
        x.push(Some(&(1_u32)));
        let s = String::from("foot"); // ensure we use non_static string to test
        let ss = s.as_str();
        x.push(Some(&ss));
        if self.include_bool {
            x.push(Some(&true));
        } else {
            x.push(None);
        }
        let em = ExampleMap {
            val: "mapval".to_string(),
        };
        let emp: &dyn MapItem = &em;
        x.push(Some(&emp));
        visitor.visit(&x);
    }
}

struct Engine;
impl EngineClient for Engine {
    fn get_data_extractor(&self, include_bool: bool) -> Arc<dyn DataExtractor> {
        let s = StrExtractor {
            include_bool,
            ..Default::default()
        };
        Arc::new(s)
    }

    fn get_json_client(&self) -> Arc<dyn JsonReader> {
        todo!()
    }
}

// ---- example usage ----
#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_no_bool() {
        let engine = Engine;
        let mut visitor = ActionVisitor::default();
        let extractor = engine.get_data_extractor(false);
        // our extractor doesn't actually look at "blob", it's just a dummy value here
        extractor.extract(&"blob".to_string(), "schema", &mut visitor);
        assert!(visitor.action.is_some());
        let action = visitor.action.unwrap();
        assert_eq!(action.i, 1);
        assert_eq!(action.s.as_str(), "foot");
        assert_eq!(action.o, None);
        assert_eq!(action.m.as_str(), "mapval");
    }

    #[test]
    fn test_bool() {
        let engine = Engine;
        let mut visitor = ActionVisitor::default();
        let extractor = engine.get_data_extractor(true);
        // our extractor doesn't actually look at "blob", it's just a dummy value here
        extractor.extract(&"blob".to_string(), "schema", &mut visitor);
        assert!(visitor.action.is_some());
        let action = visitor.action.unwrap();
        assert_eq!(action.i, 1);
        assert_eq!(action.s.as_str(), "foot");
        assert_eq!(action.o, Some(true));
        assert_eq!(action.m.as_str(), "mapval");
    }
}
