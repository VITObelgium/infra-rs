extern crate proc_macro;
use inf::Result;
use proc_macro::TokenStream;
use punctuated::Punctuated;
use quote::quote;

use syn::*;

fn named_fields(ast: &syn::DeriveInput) -> &FieldsNamed {
    match &ast.data {
        Data::Struct(DataStruct {
            fields: Fields::Named(it), ..
        }) => it,
        _ => panic!("Expected a struct with named fields"),
    }
}

/// Check if the field has a `#[vector(skip)]` attribute
fn needs_to_be_skipped(field: &Field) -> bool {
    for attr in &field.attrs {
        if attr.path().is_ident("vector") {
            let nested = attr.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated).unwrap();
            for meta in nested {
                if let Meta::Path(path) = meta {
                    if path.is_ident("skip") {
                        return true;
                    }
                }
            }
        }
    }
    false
}

/// Check if the field has a `#[vector(column = "name")]` attribute and return the value of the name
fn check_col_attr(field: &Field) -> Option<String> {
    for attr in &field.attrs {
        if attr.path().is_ident("vector") {
            let nested = attr.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated).unwrap();
            for meta in nested {
                if let Meta::NameValue(name_value) = meta {
                    if name_value.path.is_ident("column") {
                        match name_value.value {
                            Expr::Lit(s) => match s.lit {
                                Lit::Str(ref s) => return Some(s.value()),
                                _ => {
                                    panic!("Expected a string literal as column name");
                                }
                            },
                            _ => {
                                panic!("Expected a string literal as column name");
                            }
                        }
                    }
                }
            }
        }
    }
    None
}

fn field_names(ast: &syn::DeriveInput) -> Result<Vec<proc_macro2::TokenStream>> {
    let fields = named_fields(ast);
    let name_list: Vec<_> = fields
        .named
        .iter()
        .filter_map(|item| {
            if needs_to_be_skipped(item) {
                return None;
            }

            let attr_name = check_col_attr(item);
            let name = item.ident.as_ref().unwrap().to_string();
            let name_str = attr_name.unwrap_or(name);
            Some(quote! {#name_str})
        })
        .collect();

    Ok(name_list)
}

fn is_option_type(tp: &Type) -> Option<&Type> {
    let Type::Path(p) = tp else {
        return None;
    };

    let final_segment = p.path.segments.iter().last()?;

    if final_segment.ident == "Option" {
        // This is an Option type, now obtain the inner type
        if let PathArguments::AngleBracketed(args) = &final_segment.arguments {
            if let Some(GenericArgument::Type(ty)) = args.args.first() {
                return Some(ty);
            }
        }
    }

    None
}

fn field_initializers(ast: &syn::DeriveInput) -> Result<Vec<proc_macro2::TokenStream>> {
    let fields = named_fields(ast);

    let struct_attrs: Vec<_> = fields
        .named
        .iter()
        .map(|item| {
            let attr_name = check_col_attr(item);
            let name = item.ident.as_ref().unwrap();
            let name_str = attr_name.unwrap_or(name.to_string());
            let tp: &Type = &item.ty;

            if needs_to_be_skipped(item) {
                quote! { #name: Default::default() }
            } else if let Some(inner_type) = is_option_type(tp) {
                quote! { #name: ::geo::vector::datarow::__private::read_feature_val::<#inner_type>(&feature, #name_str)? }
            } else {
                quote! { #name: ::geo::vector::datarow::__private::read_feature_val::<#tp>(&feature, #name_str)?.ok_or(
                    inf::Error::InvalidArgument(format!("Invalid field value for {}", #name_str)))?
                }
            }
        })
        .collect();

    Ok(struct_attrs)
}

fn impl_data_row(ast: &syn::DeriveInput) -> Result<TokenStream> {
    let name = &ast.ident;

    let field_names = field_names(ast)?;
    let field_initializers = field_initializers(ast)?;

    let generated = quote! {
        impl ::geo::vector::DataRow for #name {
            fn field_names() -> Vec<&'static str> {
                vec![#(#field_names),*]
            }

            fn from_feature(feature: gdal::vector::Feature) -> ::geo::Result<Self> {
                Ok(#name {
                    #(#field_initializers),*
                })
            }
        }
    };

    Ok(generated.into())
}

#[proc_macro_derive(DataRow, attributes(vector))]
pub fn data_row_derive(input: proc_macro::TokenStream) -> TokenStream {
    // Construct a representation of Rust code as a syntax tree
    // that we can manipulate
    let ast = syn::parse(input).unwrap();

    // Build the trait implementation
    impl_data_row(&ast).expect("Failed to generate DataRow implementation")
}
