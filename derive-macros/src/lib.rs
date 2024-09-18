use proc_macro2::{Ident, TokenStream};
use quote::{quote, quote_spanned};
use syn::spanned::Spanned;
use syn::{parse_macro_input, Data, DataStruct, DeriveInput, Fields, Meta, PathArguments, Type};

/// Derive a `delta_kernel::schemas::ToDataType` implementation for the annotated struct. The actual
/// field names in the schema (and therefore of the struct members) are all mandated by the Delta
/// spec, and so the user of this macro is responsible for ensuring that
/// e.g. `Metadata::schema_string` is the snake_case-ified version of `schemaString` from [Delta's
/// Change Metadata](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#change-metadata)
/// action (this macro allows the use of standard rust snake_case, and will convert to the correct
/// delta schema camelCase version).
#[proc_macro_derive(Schema, attributes(schema_container_values_null))]
pub fn derive_schema(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let struct_ident = input.ident;

    let schema_fields = gen_schema_fields(&input.data);
    let output = quote! {
        #[automatically_derived]
        impl crate::actions::schemas::ToDataType for #struct_ident {
            fn to_data_type() -> crate::schema::DataType {
                use crate::actions::schemas::{ToDataType, GetStructField, GetNullableContainerStructField};
                crate::schema::StructType::new(vec![
                    #schema_fields
                ]).into()
            }
        }
    };
    proc_macro::TokenStream::from(output)
}

// turn our struct name into the schema name, goes from snake_case to camelCase
fn get_schema_name(name: &Ident) -> Ident {
    let snake_name = name.to_string();
    let mut next_caps = false;
    let ret: String = snake_name
        .chars()
        .filter_map(|c| {
            if c == '_' {
                next_caps = true;
                None
            } else if next_caps {
                next_caps = false;
                // This assumes we're using ascii, should be okay
                Some(c.to_ascii_uppercase())
            } else {
                Some(c)
            }
        })
        .collect();
    Ident::new(&ret, name.span())
}

fn gen_schema_fields(data: &Data) -> TokenStream {
    let fields = match data {
        Data::Struct(DataStruct {
            fields: Fields::Named(fields),
            ..
        }) => &fields.named,
        _ => panic!("this derive macro only works on structs with named fields"),
    };

    let schema_fields = fields.iter().map(|field| {
        let name = field.ident.as_ref().unwrap(); // we know these are named fields
        let name = get_schema_name(name);
        let have_schema_null = field.attrs.iter().any(|attr| {
            // check if we have schema_map_values_null attr
            match &attr.meta {
                Meta::Path(path) => path.get_ident().map(|ident| ident == "schema_container_values_null").unwrap_or(false),
                _ => false,
            }
        });

        match field.ty {
            Type::Path(ref type_path) => {
                let type_path_quoted = type_path.path.segments.iter().map(|segment| {
                    let segment_ident = &segment.ident;
                    match &segment.arguments {
                        PathArguments::None => quote! { #segment_ident :: },
                        PathArguments::AngleBracketed(angle_args) => quote! { #segment_ident::#angle_args :: },
                        _ => panic!("Can only handle <> type path args"),
                    }
                });
                if have_schema_null {
                    if let Some(first_ident) = type_path.path.segments.first().map(|seg| &seg.ident) {
                        if first_ident != "HashMap" && first_ident != "Vec" {
                            panic!("Can only use schema_container_values_null on HashMap or Vec fields, not {first_ident:?}");
                        }
                    }
                    quote_spanned! { field.span() => #(#type_path_quoted),* get_nullable_container_struct_field(stringify!(#name))}
                } else {
                    quote_spanned! { field.span() => #(#type_path_quoted),* get_struct_field(stringify!(#name))}
                }
            }
            _ => {
                panic!("Can't handle type: {:?}", field.ty);
            }
        }
    });
    quote! { #(#schema_fields),* }
}
