// Based on futures-await code
#![feature(proc_macro, match_default_bindings)]
#![recursion_limit = "128"]

extern crate proc_macro2;
extern crate proc_macro;
#[macro_use]
extern crate quote;
extern crate syn;

use proc_macro2::Span;
use proc_macro::{TokenStream, TokenNode};
use quote::{Tokens, ToTokens};
use syn::*;
use syn::punctuated::Punctuated;
use syn::fold::Fold;

macro_rules! quote_cs {
    ($($t:tt)*) => (quote_spanned!(Span::call_site() => $($t)*))
}

fn first_last(tokens: &ToTokens) -> (Span, Span) {
    let mut spans = Tokens::new();
    tokens.to_tokens(&mut spans);
    let good_tokens = proc_macro2::TokenStream::from(spans).into_iter().collect::<Vec<_>>();
    let first_span = good_tokens.first().map(|t| t.span).unwrap_or(Span::def_site());
    let last_span = good_tokens.last().map(|t| t.span).unwrap_or(first_span);
    (first_span, last_span)
}

fn respan(input: proc_macro2::TokenStream,
          &(first_span, last_span): &(Span, Span)) -> proc_macro2::TokenStream {
    let mut new_tokens = input.into_iter().collect::<Vec<_>>();
    if let Some(token) = new_tokens.first_mut() {
        token.span = first_span;
    }
    for token in new_tokens.iter_mut().skip(1) {
        token.span = last_span;
    }
    new_tokens.into_iter().collect()
}

fn async_inner(
    input: bool,
    function: TokenStream,
    gen_function: Tokens,
) -> TokenStream
{
    // Parse our item, expecting a function. This function may be an actual
    // top-level function or it could be a method (typically dictated by the
    // arguments). We then extract everything we'd like to use.
    let ItemFn {
        ident,
        vis,
        unsafety,
        constness,
        abi,
        block,
        decl,
        attrs,
        ..
    } = match syn::parse(function).expect("failed to parse tokens as a function") {
        Item::Fn(item) => item,
        _ => panic!("`job` attribute can only be applied to functions"),
    };
    let FnDecl {
        inputs,
        output,
        variadic,
        mut generics,
        fn_token,
        ..
    } = { *decl };
    let where_clause = &generics.where_clause;

    if variadic.is_some() {
        panic!("`job` attribute not supported for variadic functions");
    }
    if !generics.params.is_empty() {
        panic!("`job` attribute not supported for generic functions");
    }
    /*let (output, rarrow_token) = match output {
        ReturnType::Type(rarrow_token, t) => (*t, rarrow_token),
        ReturnType::Default => {
            (TypeTuple {
                elems: Default::default(),
                paren_token: Default::default(),
            }.into(), Default::default())
        }
    };*/

    // We've got to get a bit creative with our handling of arguments. For a
    // number of reasons we translate this:
    //
    //      fn foo(ref a: u32) -> Result<u32, u32> {
    //          // ...
    //      }
    //
    // into roughly:
    //
    //      fn foo(__arg_0: u32) -> impl Future<...> {
    //          gen_move(move || {
    //              let ref a = __arg0;
    //
    //              // ...
    //          })
    //      }
    //
    // The intention here is to ensure that all local function variables get
    // moved into the generator we're creating, and they're also all then bound
    // appropriately according to their patterns and whatnot.
    //
    // We notably skip everything related to `self` which typically doesn't have
    // many patterns with it and just gets captured naturally.
    let mut args = Vec::new();
    let mut types = Vec::new();
    let mut patterns = Vec::new();
    for (i, input) in inputs.into_iter().enumerate() {
        match input {
            FnArg::Captured(ArgCaptured { pat, ty, colon_token }) => {
                patterns.push(pat);
                types.push(ty);
                args.push(Ident::from(format!("__arg_{}", i)));
                /*let ident = Ident::from(format!("__arg_{}", i));
                temp_bindings.push(ident.clone());
                let pat = PatIdent {
                    by_ref: None,
                    mutability: None,
                    ident: ident,
                    subpat: None,
                };
                args_paired_with_types.push(ArgCaptured {
                    pat: pat.into(),
                    ty,
                    colon_token,
                }.into());*/
            }
            _ => panic!("unsupported function")
        }
    }


    // This is the point where we handle
    //
    //      #[async]
    //      for x in y {
    //      }
    //
    // Basically just take all those expression and expand them.
    //let block = ExpandAsyncFor.fold_block(*block);


    //let inputs_no_patterns = elision::unelide_lifetimes(&mut generics.params, inputs_no_patterns);
    //let lifetimes: Vec<_> = generics.lifetimes().map(|l| &l.lifetime).collect();
/*
    let block_inner = quote_cs! {
        #( let #patterns = #temp_bindings; )*
        #block
    };
    let mut result = Tokens::new();
    block.brace_token.surround(&mut result, |tokens| {
        block_inner.to_tokens(tokens);
    });
    syn::token::Semi([block.brace_token.0]).to_tokens(&mut result);
*/
    let args_c = args.clone();
    let types_c = types.clone();
    let gen_body_inner = quote_cs! {
        let __name = ::jobs::DepNodeName(concat!(module_path!(), "::", stringify!(#ident)));
        let __compute = move |(#(#patterns,)*): (#(#types_c,)*)| #block;
        let __key = (#(#args_c,)*);
        ::jobs::execute_job(__name, #input, __key, __compute)
    };
    let mut gen_body = Tokens::new();
    block.brace_token.surround(&mut gen_body, |tokens| {
        gen_body_inner.to_tokens(tokens);
    });
/*
    // Give the invocation of the `gen` function the same span as the output
    // as currently errors related to it being a result are targeted here. Not
    // sure if more errors will highlight this function call...
    let output_span = first_last(&output);
    let gen_function = respan(gen_function.into(), &output_span);
    let body_inner = if pinned {
        quote_cs! {
            #gen_function (static move || -> #output #gen_body)
        }
    } else {
        quote_cs! {
            #gen_function (move || -> #output #gen_body)
        }
    };
    let body_inner = if boxed {
        let body = quote_cs! { ::futures::__rt::std::boxed::Box::new(#body_inner) };
        respan(body.into(), &output_span)
    } else {
        body_inner.into()
    };
    let mut body = Tokens::new();
    block.brace_token.surround(&mut body, |tokens| {
        body_inner.to_tokens(tokens);
    });
*/
    let output = quote_cs! {
        #[allow(non_camel_case_types)]
        #vis struct #ident {}
        impl #ident {
            fn execute(key: Vec<u8>) -> Vec<u8> {
                let key = ::jobs::deserialize::<bool>(&key).unwrap();
                let value = true;//compute_orig(key);
                ::jobs::serialize(&value).unwrap()
            }
        }
        impl ::jobs::RecheckResultOfJob for #ident {
            fn forcer() -> (::jobs::DepNodeName, fn(Vec<u8>) -> Vec<u8>) {
                let name = ::jobs::DepNodeName(concat!(module_path!(), "::", stringify!(#ident)));
                (name, #ident::execute)
            }
        }
        #(#attrs)*
        #vis #unsafety #abi #constness
        #fn_token #ident #generics(#(#args: #types),*)
            #output
            #where_clause
        #gen_body
    };

    // println!("{}", output);
    output.into()
}

#[proc_macro_attribute]
pub fn job(attribute: TokenStream, function: TokenStream) -> TokenStream {
    eprintln!("ATTR {}", &attribute.to_string() as &str);
    let input = match &attribute.to_string() as &str {
        "" => false,
        "( input )" => true,
        _ => panic!("the #[job] attribute currently only takes no arguments"),
    };

    let r = async_inner(input, function, quote_cs! { ::futures::__rt::gen_pinned });
    eprintln!("OUTPUT ```\n{}\n```", r);
    r
}
