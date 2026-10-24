//! Builds the tests of every example into one test binary.
//!
//! Each example linked into a test binary of its own carries the whole dependency graph, and the
//! examples are the bulk of the build. [`examples!`] reads the package manifest and turns every
//! example into a module of the invoking test target instead, gated on the example's
//! `required-features`, so a feature leg compiles exactly the examples it would have built. The
//! manifest stays the one place an example is declared: a new example joins the suite without
//! being listed anywhere else.

use std::collections::BTreeSet;
use std::fmt::{Display, Write as _};
use std::path::{Path, PathBuf};
use std::{env, fs};

use proc_macro::{TokenStream, TokenTree};
use toml::{Table, Value};

/// Declares one module per example of the package, each at the example's file.
///
/// The input lists, comma separated, the examples whose tests keep a binary of their own (each
/// with `test = true` in the manifest); a comment beside each says why. Every other example with
/// tests becomes a module named after it, compiled only when its `required-features` are on.
///
/// The expansion is a compile error naming the example when an example in the suite still has
/// `test = true` (its tests would build and run twice), when a listed name is not an example of
/// the package, or when a listed example with tests has no `test = true` (its tests would run
/// nowhere).
#[proc_macro]
pub fn examples(input: TokenStream) -> TokenStream {
    match expand(input) {
        Ok(code) | Err(code) => code.parse().unwrap_or_default(),
    }
}

struct Example {
    name: String,
    path: PathBuf,
    features: Vec<String>,
    test: bool,
}

fn expand(input: TokenStream) -> Result<String, String> {
    let standalone = standalone(input)?;
    let root = PathBuf::from(env::var("CARGO_MANIFEST_DIR").map_err(|err| error(&err))?);
    let manifest = root.join("Cargo.toml");
    let examples = examples_of(&root, &manifest)?;
    for name in &standalone {
        if !examples.iter().any(|example| &example.name == name) {
            return Err(error(&format_args!(
                "`{name}` is not an example of this package"
            )));
        }
    }
    // rustc does not see a file a macro reads; including it makes an edit to the manifest, or
    // the first test added to an example the suite left out, expand the suite again.
    let mut code = format!(
        "const _: &[u8] = include_bytes!({:?});\n",
        manifest.display()
    );
    for example in &examples {
        let _ = writeln!(
            code,
            "const _: &[u8] = include_bytes!({:?});",
            example.path.display()
        );
        let has_tests = has_tests(&example.path)?;
        if !has_tests && !standalone.contains(&example.name) {
            continue;
        }
        if standalone.contains(&example.name) {
            if has_tests && !example.test {
                return Err(error(&format_args!(
                    "example `{}` has tests and keeps its own binary, so it needs `test = true`",
                    example.name
                )));
            }
            continue;
        }
        if example.test {
            return Err(error(&format_args!(
                "example `{}` runs in the suite: drop its `test = true`, or list it here with \
                 the reason it needs a binary of its own",
                example.name
            )));
        }
        let features: Vec<String> = example
            .features
            .iter()
            .map(|feature| format!("feature = {feature:?}"))
            .collect();
        // Each example is written as a crate root, and the lints below hold only for a module:
        // its `main` and the items only `main` reaches are dead here, its `pub` items reach no
        // further than the suite, the support files several examples include are each a module
        // of every one of them, and an inner module may share the example's name.
        let _ = writeln!(
            code,
            "#[cfg(all({}))]\n#[allow(dead_code, unreachable_pub, clippy::duplicate_mod, \
             clippy::module_inception)]\n#[path = {:?}]\nmod {};",
            features.join(", "),
            example.path.display(),
            example.name,
        );
    }
    Ok(code)
}

fn standalone(input: TokenStream) -> Result<BTreeSet<String>, String> {
    let mut names = BTreeSet::new();
    for token in input {
        match token {
            TokenTree::Ident(ident) => {
                names.insert(ident.to_string());
            }
            TokenTree::Punct(punct) if punct.as_char() == ',' => {}
            other => {
                return Err(error(&format_args!(
                    "expected example names, found `{other}`"
                )));
            }
        }
    }
    Ok(names)
}

/// The examples cargo builds: the `[[example]]` entries, and the files under `examples/` it
/// discovers on its own.
fn examples_of(root: &Path, manifest: &Path) -> Result<Vec<Example>, String> {
    let table: Table = fs::read_to_string(manifest)
        .map_err(|err| error(&err))?
        .parse()
        .map_err(|err| error(&err))?;
    let mut examples = Vec::new();
    for entry in table
        .get("example")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_table)
    {
        let name = entry
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| error(&"an `[[example]]` without a name"))?
            .to_owned();
        let path = entry.get("path").and_then(Value::as_str).map_or_else(
            || root.join("examples").join(format!("{name}.rs")),
            |path| root.join(path),
        );
        let features = entry
            .get("required-features")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect();
        let test = entry.get("test").and_then(Value::as_bool).unwrap_or(false);
        examples.push(Example {
            name,
            path,
            features,
            test,
        });
    }
    let autoexamples = table
        .get("package")
        .and_then(|package| package.get("autoexamples"))
        .and_then(Value::as_bool)
        .unwrap_or(true);
    if autoexamples {
        for file in fs::read_dir(root.join("examples")).map_err(|err| error(&err))? {
            let path = file.map_err(|err| error(&err))?.path();
            let declared = examples.iter().any(|example| example.path == path);
            if path.extension().is_some_and(|extension| extension == "rs") && !declared {
                let name = path
                    .file_stem()
                    .and_then(|stem| stem.to_str())
                    .ok_or_else(|| error(&format_args!("{} has no name", path.display())))?
                    .to_owned();
                examples.push(Example {
                    name,
                    path,
                    features: Vec::new(),
                    test: false,
                });
            }
        }
    }
    examples.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(examples)
}

fn has_tests(path: &Path) -> Result<bool, String> {
    let source = fs::read_to_string(path)
        .map_err(|err| error(&format_args!("{}: {err}", path.display())))?;
    Ok(source.contains("#[test]") || source.contains("#[tokio::test"))
}

fn error(message: &dyn Display) -> String {
    format!("compile_error!({:?});", message.to_string())
}
