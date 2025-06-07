// conditional compilation creates warnings when the feature is not enabled
#![allow(unused_mut)]
#![allow(unused_imports)]
#![allow(unused_variables)]

extern crate proc_macro;
use proc_macro::TokenStream;
use quote::quote;
use syn::{ItemFn, ItemImpl, parse::Parser as _, parse_macro_input};

#[proc_macro_attribute]
pub fn simd_bounds(attr: TokenStream, item: TokenStream) -> TokenStream {
    simd_bounds_impl(attr, item, true)
}

#[proc_macro_attribute]
pub fn geo_simd_bounds(attr: TokenStream, item: TokenStream) -> TokenStream {
    simd_bounds_impl(attr, item, false)
}

fn simd_bounds_impl(attr: TokenStream, item: TokenStream, crate_local: bool) -> TokenStream {
    let types: Vec<syn::Type> = if attr.is_empty() {
        vec![syn::parse_quote! { T }]
    } else {
        // Parse comma-separated type expressions
        let parser = syn::punctuated::Punctuated::<syn::Type, syn::Token![,]>::parse_terminated;
        match parser.parse(attr) {
            Ok(punctuated) => punctuated.into_iter().collect(),
            Err(e) => {
                let error_msg = e.to_string();
                return TokenStream::from(quote! {
                    compile_error!(concat!("Error parsing type arguments: ", #error_msg));
                });
            }
        }
    };

    let mut predicates = Vec::<syn::WherePredicate>::new();
    for typ in &types {
        if crate_local {
            predicates.push(syn::parse_quote! {
                std::simd::Simd<#typ, LANES>: crate::NodataSimd<Scalar = #typ, NodataMask = std::simd::Mask<<#typ as std::simd::SimdElement>::Mask, LANES>>
            });
        } else {
            predicates.push(syn::parse_quote! {
                std::simd::Simd<#typ, LANES>: ::geo::NodataSimd<Scalar = #typ, NodataMask = std::simd::Mask<<#typ as std::simd::SimdElement>::Mask, LANES>>
            });
        }
    }

    // Try parsing as free function
    if let Ok(mut function) = syn::parse::<ItemFn>(item.clone()) {
        #[cfg(feature = "simd")]
        {
            function.sig.generics.make_where_clause().predicates.extend(predicates.clone());
        }

        return quote!(#function).into();
    }

    // Try parsing as method inside an impl
    if let Ok(mut method) = syn::parse::<syn::ImplItemFn>(item.clone()) {
        #[cfg(feature = "simd")]
        {
            method.sig.generics.make_where_clause().predicates.extend(predicates.clone());
        }

        return quote!(#method).into();
    }

    // Try parsing as a trait method
    if let Ok(mut trait_method) = syn::parse::<syn::TraitItemFn>(item.clone()) {
        #[cfg(feature = "simd")]
        {
            trait_method.sig.generics.make_where_clause().predicates.extend(predicates.clone());
        }
        return quote!(#trait_method).into();
    }

    // Try parsing as an entire impl block
    if let Ok(mut impl_block) = syn::parse::<ItemImpl>(item.clone()) {
        #[cfg(feature = "simd")]
        {
            impl_block.generics.make_where_clause().predicates.extend(predicates.clone());
        }
        return quote!(#impl_block).into();
    }

    // Try parsing as an associated type
    if let Ok(mut assoc_type) = syn::parse::<syn::TraitItemType>(item.clone()) {
        #[cfg(feature = "simd")]
        {
            assoc_type.generics.make_where_clause().predicates.extend(predicates.clone());
        }
        return quote!(#assoc_type).into();
    }

    // Try parsing as an associated type
    if let Ok(mut assoc_type) = syn::parse::<syn::ImplItemType>(item.clone()) {
        #[cfg(feature = "simd")]
        {
            assoc_type.generics.make_where_clause().predicates.extend(predicates.clone());
        }
        return quote!(#assoc_type).into();
    }

    // Fallback in case of unsupported item
    let fallback: proc_macro2::TokenStream = item.into();
    TokenStream::from(quote! {
        compile_error!("`#[simd_bounds]` only works on functions or methods with generics.");
        #fallback
    })
}
