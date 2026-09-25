//! `#[derive(Record)]` for wahlberg. Use it through `wahlberg::Record`.
//!
//! ```
//! use serde::{Deserialize, Serialize};
//! use wahlberg::Record;
//!
//! #[derive(Serialize, Deserialize, Record)]
//! #[record(table = "users")]
//! struct User {
//!     #[record(id)]
//!     id: String,
//!     name: String,
//! }
//!
//! assert_eq!(<User as Record>::TABLE, "users");
//! ```
//!
//! The derive only supplies the table name and which field is the id; all
//! field (de)serialization goes through serde, so serde attributes apply.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::ext::IdentExt;
use syn::punctuated::Punctuated;
use syn::{Data, DeriveInput, Expr, Fields, Lit, LitStr, Meta, Token, parse_macro_input};

#[proc_macro_derive(Record, attributes(record))]
pub fn derive_record(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand(&input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

fn expand(input: &DeriveInput) -> syn::Result<TokenStream2> {
    let name = &input.ident;

    let Data::Struct(data) = &input.data else {
        return Err(syn::Error::new_spanned(
            name,
            "#[derive(Record)] only supports structs with named fields",
        ));
    };
    let Fields::Named(fields) = &data.fields else {
        return Err(syn::Error::new_spanned(
            name,
            "#[derive(Record)] only supports structs with named fields",
        ));
    };

    let mut table: Option<String> = None;
    for attr in input.attrs.iter().filter(|a| a.path().is_ident("record")) {
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("table") {
                let lit: LitStr = meta.value()?.parse()?;
                table = Some(lit.value());
                Ok(())
            } else {
                Err(meta.error("unsupported #[record] attribute; expected `table = \"...\"`"))
            }
        })?;
    }
    let table = table.unwrap_or_else(|| to_snake_case(&name.to_string()));

    let rename_all = serde_string_value(&input.attrs, "rename_all")?;

    // The id field: marked #[record(id)], or else a field named `id`.
    let mut marked = Vec::new();
    for field in &fields.named {
        for attr in field.attrs.iter().filter(|a| a.path().is_ident("record")) {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("id") {
                    marked.push(field);
                    Ok(())
                } else {
                    Err(meta.error("unsupported #[record] field attribute; expected `id`"))
                }
            })?;
        }
    }
    let id_field = match marked.as_slice() {
        [one] => *one,
        [] => fields
            .named
            .iter()
            .find(|f| f.ident.as_ref().is_some_and(|i| i.unraw() == "id"))
            .ok_or_else(|| {
                syn::Error::new_spanned(
                    name,
                    "no id field: mark one with #[record(id)] or name it `id`",
                )
            })?,
        [_, second, ..] => {
            return Err(syn::Error::new_spanned(
                second,
                "only one field can be #[record(id)]",
            ));
        }
    };
    let id_ident = id_field.ident.as_ref().unwrap();

    // The id's key in the serialized object, following serde's renaming.
    let id_key = match serde_string_value(&id_field.attrs, "rename")? {
        Some(rename) => rename,
        None => {
            let raw = id_ident.unraw().to_string();
            match &rename_all {
                Some(rule) => apply_rename_rule(rule, &raw).ok_or_else(|| {
                    syn::Error::new_spanned(
                        name,
                        format!("unknown serde rename_all rule \"{rule}\""),
                    )
                })?,
                None => raw.to_string(),
            }
        }
    };

    // Bound on Self rather than on each type parameter: serde's derives
    // already worked out which parameters need which bounds.
    let mut generics = input.generics.clone();
    generics
        .make_where_clause()
        .predicates
        .push(syn::parse_quote! {
            Self: ::wahlberg::record::__private::Serialize
                + ::wahlberg::record::__private::DeserializeOwned
        });
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    Ok(quote! {
        impl #impl_generics ::wahlberg::record::Record for #name #ty_generics #where_clause {
            const TABLE: &'static str = #table;
            const ID_FIELD: &'static str = #id_key;
            fn id(&self) -> &str {
                ::core::convert::AsRef::<str>::as_ref(&self.#id_ident)
            }
        }
    })
}

/// Finds `key = "value"` inside `#[serde(...)]` attributes. The list forms
/// (`rename(serialize = "..")`) are rejected since the store needs a single
/// key for both directions.
fn serde_string_value(attrs: &[syn::Attribute], key: &str) -> syn::Result<Option<String>> {
    let mut found = None;
    for attr in attrs.iter().filter(|a| a.path().is_ident("serde")) {
        let metas = attr.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)?;
        for meta in metas {
            match meta {
                Meta::NameValue(nv) if nv.path.is_ident(key) => {
                    if let Expr::Lit(expr) = &nv.value
                        && let Lit::Str(s) = &expr.lit
                    {
                        found = Some(s.value());
                    }
                }
                Meta::List(list) if list.path.is_ident(key) => {
                    return Err(syn::Error::new_spanned(
                        list,
                        format!(
                            "#[derive(Record)] needs the same name for serialize and deserialize; use `{key} = \"...\"`"
                        ),
                    ));
                }
                _ => {}
            }
        }
    }
    Ok(found)
}

/// serde's `rename_all` rules as applied to a (snake_case) field name.
fn apply_rename_rule(rule: &str, field: &str) -> Option<String> {
    let pascal = || {
        field
            .split('_')
            .map(|part| {
                let mut chars = part.chars();
                match chars.next() {
                    Some(c) => c.to_uppercase().chain(chars).collect::<String>(),
                    None => String::new(),
                }
            })
            .collect::<String>()
    };
    Some(match rule {
        "lowercase" => field.to_lowercase(),
        "UPPERCASE" => field.to_uppercase(),
        "PascalCase" => pascal(),
        "camelCase" => {
            let p = pascal();
            let mut chars = p.chars();
            match chars.next() {
                Some(c) => c.to_lowercase().chain(chars).collect(),
                None => String::new(),
            }
        }
        "snake_case" => field.to_string(),
        "SCREAMING_SNAKE_CASE" => field.to_uppercase(),
        "kebab-case" => field.replace('_', "-"),
        "SCREAMING-KEBAB-CASE" => field.replace('_', "-").to_uppercase(),
        _ => return None,
    })
}

/// `UserProfile` → `user_profile`: the default table name.
fn to_snake_case(name: &str) -> String {
    let mut out = String::new();
    for (i, c) in name.chars().enumerate() {
        if c.is_uppercase() {
            if i > 0 {
                out.push('_');
            }
            out.extend(c.to_lowercase());
        } else {
            out.push(c);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rename_rules_match_serde() {
        let f = "user_id";
        assert_eq!(apply_rename_rule("lowercase", f).unwrap(), "user_id");
        assert_eq!(apply_rename_rule("UPPERCASE", f).unwrap(), "USER_ID");
        assert_eq!(apply_rename_rule("PascalCase", f).unwrap(), "UserId");
        assert_eq!(apply_rename_rule("camelCase", f).unwrap(), "userId");
        assert_eq!(apply_rename_rule("snake_case", f).unwrap(), "user_id");
        assert_eq!(
            apply_rename_rule("SCREAMING_SNAKE_CASE", f).unwrap(),
            "USER_ID"
        );
        assert_eq!(apply_rename_rule("kebab-case", f).unwrap(), "user-id");
        assert_eq!(
            apply_rename_rule("SCREAMING-KEBAB-CASE", f).unwrap(),
            "USER-ID"
        );
        assert_eq!(apply_rename_rule("camelCase", "id").unwrap(), "id");
        assert!(apply_rename_rule("Title Case", f).is_none());
    }

    #[test]
    fn default_table_is_snake_case() {
        assert_eq!(to_snake_case("User"), "user");
        assert_eq!(to_snake_case("UserProfile"), "user_profile");
    }
}
