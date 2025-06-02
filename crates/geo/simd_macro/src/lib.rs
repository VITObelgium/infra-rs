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
    let type_expr: syn::Type = if attr.is_empty() {
        syn::parse_quote! { T }
    } else {
        syn::parse(attr).expect("expected a single type expression (e.g., T or Self::Pixel)")
    };

    let predicate: syn::WherePredicate = syn::parse_quote! {
        std::simd::Simd<#type_expr, LANES>: crate::NodataSimd
    };

    // Try parsing as free function
    if let Ok(mut function) = syn::parse::<ItemFn>(item.clone()) {
        #[cfg(feature = "simd")]
        {
            function.sig.generics.make_where_clause().predicates.push(predicate.clone());
        }

        return quote!(#function).into();
    }

    // Try parsing as method inside an impl
    if let Ok(mut method) = syn::parse::<syn::ImplItemFn>(item.clone()) {
        #[cfg(feature = "simd")]
        {
            method.sig.generics.make_where_clause().predicates.push(predicate.clone());
        }

        return quote!(#method).into();
    }

    // Try parsing as a trait method
    if let Ok(mut trait_method) = syn::parse::<syn::TraitItemFn>(item.clone()) {
        #[cfg(feature = "simd")]
        {
            trait_method.sig.generics.make_where_clause().predicates.push(predicate.clone());
        }
        return quote!(#trait_method).into();
    }

    // Try parsing as an entire impl block
    if let Ok(mut impl_block) = syn::parse::<ItemImpl>(item.clone()) {
        #[cfg(feature = "simd")]
        {
            impl_block.generics.make_where_clause().predicates.push(predicate.clone());
        }
        return quote!(#impl_block).into();
    }

    // Fallback in case of unsupported item
    let fallback: proc_macro2::TokenStream = item.into();
    TokenStream::from(quote! {
        compile_error!("`#[simd_bounds]` only works on functions or methods with generics.");
        #fallback
    })
}
