// Based on futures-await code
#![feature(proc_macro, match_default_bindings)]
#![recursion_limit = "128"]

extern crate proc_macro2;
extern crate proc_macro;
#[macro_use]
extern crate quote;
#[macro_use]
extern crate syn;

use proc_macro2::Span;
use proc_macro::{TokenStream, TokenTree, Delimiter, TokenNode};
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
    boxed: bool,
    pinned: bool,
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
        _ => panic!("#[job] can only be applied to functions"),
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
    assert!(variadic.is_none(), "variadic functions cannot be async");
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
    let mut inputs_no_patterns = Vec::new();
    let mut patterns = Vec::new();
    let mut temp_bindings = Vec::new();
    for (i, input) in inputs.into_iter().enumerate() {
        // `self: Box<Self>` will get captured naturally
        let mut is_input_no_pattern = false;
        if let FnArg::Captured(ref arg) = input {
            if let Pat::Ident(PatIdent { ref ident, ..}) = arg.pat {
                if ident == "self" {
                    is_input_no_pattern = true;
                }
            }
        }
        if is_input_no_pattern {
            inputs_no_patterns.push(input);
            continue
        }

        match input {
            FnArg::Captured(ArgCaptured {
                pat: syn::Pat::Ident(syn::PatIdent {
                    by_ref: None,
                    ..
                }),
                ..
            }) => {
                inputs_no_patterns.push(input);
            }

            // `ref a: B` (or some similar pattern)
            FnArg::Captured(ArgCaptured { pat, ty, colon_token }) => {
                patterns.push(pat);
                let ident = Ident::from(format!("__arg_{}", i));
                temp_bindings.push(ident.clone());
                let pat = PatIdent {
                    by_ref: None,
                    mutability: None,
                    ident: ident,
                    subpat: None,
                };
                inputs_no_patterns.push(ArgCaptured {
                    pat: pat.into(),
                    ty,
                    colon_token,
                }.into());
            }

            // Other `self`-related arguments get captured naturally
            _ => {
                inputs_no_patterns.push(input);
            }
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
    let gen_body_inner = quote_cs! {
        let __name = concat!(module_path!(), "::", stringify!(#ident));
        let __compute = move |(#(#patterns,)*)| #block;
        let __key = (#(#temp_bindings,)*);
        ::jobs::execute_job(__name, __key, __compute)
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
        #(#attrs)*
        #vis #unsafety #abi #constness
        #fn_token #ident #generics(#(#inputs_no_patterns),*)
            #output
            #where_clause
        #gen_body
    };

    // println!("{}", output);
    output.into()
}

#[proc_macro_attribute]
pub fn job(attribute: TokenStream, function: TokenStream) -> TokenStream {
    match &attribute.to_string() as &str {
        "" => (),
        _ => panic!("the #[job] attribute currently only takes no arguments"),
    };

    let r = async_inner(false, true, function, quote_cs! { ::futures::__rt::gen_pinned });
    eprintln!("OUTPUT ```\n{}\n```", r);
    r
}
