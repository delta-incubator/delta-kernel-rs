use crate::schema::SchemaRef;

use std::any::{Any, TypeId};

/// This module defines an interface for the kernel to transer data between a client (engine) and
/// the kernel itself. (TODO: Explain more)

/// A `DataVisitor` can be called back to visit extracted data. Aside from calling [`visit`] on the
/// visitor passed to [`extract`], engines do not need to worry about this trait.
pub trait DataVisitor {
    // Receive some data from a call to `extract`. The data in the Vec should not be assumed to live
    // beyond the call to this funtion (i.e. it should be copied if needed)
    fn visit(&mut self, row: usize, col: usize, val: &dyn Any);

    fn visit_str(&mut self, row: usize, col: usize, val: &str);
}

/// A TypeTag identifies the class that an Engine is using to represent data read by its
/// json/parquet readers. We don't parameterize our client by this to avoid having to specify the
/// generic type _everywhere_, and to make the ffi story easier. TypeTags nevertheless allow us some
/// amount of runtime type-safety as an engine can check that it got called with a data type it
/// understands.
pub trait TypeTag: 'static {
    // Can't use `:Eq / :PartialEq` as that's generic, and we want to return this trait as an object
    // below. We require the 'static bound so we can be sure the TypeId will live long enough to
    // return. In practice this just means that the type must be fully defined and not a generated type.

    /// Return a [`std::any::TypeId`] for this tag.
    fn tag_id(&self) -> TypeId {
        TypeId::of::<Self>()
    }

    /// Check if this tag is equivalent to another tag
    fn eq(&self, other: &dyn TypeTag) -> bool {
        let my_id = self.tag_id();
        let other_id = other.tag_id();
        my_id == other_id
    }
}

/// Any type that an engine wants to return as "data" needs to implement this trait. This should be
/// as easy as defining a tag to represent it that implements [`TypeTag`], and then returning it for
/// the `type_tag` method.
/// ```
/// use std::any::Any;
/// use deltakernel::engine_data::{DataExtractor, DataVisitor, EngineData, TypeTag};
/// use deltakernel::schema::SchemaRef;
/// struct MyTypeTag;
/// impl TypeTag for MyTypeTag {}
/// struct MyDataType; // Whatever the engine wants here
/// impl EngineData for MyDataType {
///    fn type_tag(&self) -> &dyn TypeTag {
///        &MyTypeTag
///    }
///    fn as_any(&self) -> &(dyn Any + 'static) { todo!() }
/// }
/// struct MyDataExtractor {
///   expected_tag: MyTypeTag,
/// }
/// impl DataExtractor for MyDataExtractor {
///   fn extract(&self, blob: &dyn EngineData, _schema: SchemaRef, visitor: &mut dyn DataVisitor) -> () {
///     assert!(self.expected_tag.eq(blob.type_tag())); // Ensure correct data type
///     // extract the data and call back visitor
///   }
///   fn length(&self, blob: &dyn EngineData) -> usize {
///     assert!(self.expected_tag.eq(blob.type_tag())); // Ensure correct data type
///     let len = 0; // actually get the len here
///     len
///   }
/// }
/// ```
pub trait EngineData {
    fn type_tag(&self) -> &dyn TypeTag;

    fn as_any(&self) -> &dyn Any;
}

/// A data extractor can take whatever the engine defines as its `EngineData` type and can call back
/// into kernel with rows extracted from that data.
pub trait DataExtractor {
    /// Extract data as requested by [`schema`] and then call back into `visitor.visit` with a Vec
    /// of that data.
    fn extract(&self, blob: &dyn EngineData, schema: SchemaRef, visitor: &mut dyn DataVisitor);
    // Return the number of items (rows?) in blob
    fn length(&self, blob: &dyn EngineData) -> usize;
}
