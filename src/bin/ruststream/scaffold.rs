//! `ruststream new`: writes a ready-to-run project from a built-in template.
//!
//! Templates are embedded at compile time and rendered by substituting `{{name}}`. Each broker
//! kind maps to one template pair (manifest + `main.rs`); the generated `main.rs` uses
//! `#[ruststream::app]`, so the project has no hand-written runtime boilerplate.

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};

use crate::BrokerKind;

const MEMORY_CARGO: &str = include_str!("templates/memory/Cargo.toml.in");
const MEMORY_MAIN: &str = include_str!("templates/memory/main.rs.in");
const MEMORY_ORDERS: &str = include_str!("templates/memory/orders.rs.in");
const MEMORY_ROUTES: &str = include_str!("templates/memory/routes.rs.in");

const NATS_CARGO: &str = include_str!("templates/nats/Cargo.toml.in");
const NATS_MAIN: &str = include_str!("templates/nats/main.rs.in");
const NATS_ORDERS: &str = include_str!("templates/nats/orders.rs.in");

const NATS_JS_CARGO: &str = include_str!("templates/nats-js/Cargo.toml.in");
const NATS_JS_MAIN: &str = include_str!("templates/nats-js/main.rs.in");
const NATS_JS_ORDERS: &str = include_str!("templates/nats-js/orders.rs.in");

/// The files a broker template writes, as `(path relative to the project dir, contents)` pairs.
const fn template(broker: BrokerKind) -> &'static [(&'static str, &'static str)] {
    match broker {
        BrokerKind::Memory => &[
            ("Cargo.toml", MEMORY_CARGO),
            ("src/main.rs", MEMORY_MAIN),
            ("src/orders.rs", MEMORY_ORDERS),
            ("src/routes.rs", MEMORY_ROUTES),
        ],
        BrokerKind::Nats => &[
            ("Cargo.toml", NATS_CARGO),
            ("src/main.rs", NATS_MAIN),
            ("src/orders.rs", NATS_ORDERS),
        ],
        BrokerKind::NatsJs => &[
            ("Cargo.toml", NATS_JS_CARGO),
            ("src/main.rs", NATS_JS_MAIN),
            ("src/orders.rs", NATS_JS_ORDERS),
        ],
    }
}

/// Scaffolds a new project named `name` wired against `broker`.
///
/// # Errors
///
/// Fails if `name` is not a valid crate name, the target directory already exists, or a file
/// cannot be written.
pub(crate) fn create(name: &str, broker: BrokerKind) -> Result<()> {
    create_in(Path::new("."), name, broker)?;
    println!("Created `{name}`. Next:");
    println!("  cd {name}");
    println!("  ruststream run");
    Ok(())
}

/// Writes the project under `parent/name`, returning the created directory. Split out from
/// [`create`] so tests can target a temporary directory.
fn create_in(parent: &Path, name: &str, broker: BrokerKind) -> Result<PathBuf> {
    validate_name(name)?;
    let dir = parent.join(name);
    if dir.exists() {
        bail!("`{name}` already exists");
    }

    fs::create_dir_all(dir.join("src")).with_context(|| format!("creating `{name}/src`"))?;
    for (rel, contents) in template(broker) {
        fs::write(dir.join(rel), render(contents, name))
            .with_context(|| format!("writing `{name}/{rel}`"))?;
    }
    Ok(dir)
}

fn render(template: &str, name: &str) -> String {
    template.replace("{{name}}", name)
}

fn validate_name(name: &str) -> Result<()> {
    let valid = !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-');
    if !valid {
        bail!("invalid crate name `{name}`; use letters, digits, `-` or `_`");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{BrokerKind, create_in, render, validate_name};

    #[test]
    fn render_substitutes_name() {
        assert_eq!(render("hello {{name}}!", "svc"), "hello svc!");
    }

    #[test]
    fn rejects_invalid_names() {
        assert!(validate_name("ok_name-1").is_ok());
        assert!(validate_name("").is_err());
        assert!(validate_name("bad name").is_err());
        assert!(validate_name("../escape").is_err());
    }

    #[test]
    fn writes_project_files() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = create_in(tmp.path(), "svc", BrokerKind::Memory).unwrap();

        let cargo = std::fs::read_to_string(dir.join("Cargo.toml")).unwrap();
        let main = std::fs::read_to_string(dir.join("src/main.rs")).unwrap();
        let orders = std::fs::read_to_string(dir.join("src/orders.rs")).unwrap();
        let routes = std::fs::read_to_string(dir.join("src/routes.rs")).unwrap();
        assert!(cargo.contains("name = \"svc\""));
        assert!(cargo.contains("ruststream = "));
        assert!(main.contains("#[ruststream::app]"));
        assert!(main.contains("AppInfo::new(\"svc\""));
        assert!(routes.contains("include_router") || routes.contains("Router::new"));
        assert!(orders.contains("JsonSchema")); // payload schema for `asyncapi gen`
        assert!(!dir.join("src/stream.rs").exists());
        for file in [&cargo, &main, &orders, &routes] {
            assert!(!file.contains("{{name}}"));
        }
    }

    #[test]
    fn writes_nats_project() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = create_in(tmp.path(), "svc", BrokerKind::Nats).unwrap();

        let cargo = std::fs::read_to_string(dir.join("Cargo.toml")).unwrap();
        let main = std::fs::read_to_string(dir.join("src/main.rs")).unwrap();
        let orders = std::fs::read_to_string(dir.join("src/orders.rs")).unwrap();
        assert!(cargo.contains("ruststream-nats = "));
        assert!(main.contains("NatsBroker::new("));
        assert!(orders.contains("JsonSchema"));
        assert!(!dir.join("src/stream.rs").exists());
        for file in [&cargo, &main, &orders] {
            assert!(!file.contains("{{name}}"));
        }
    }

    #[test]
    fn writes_nats_js_project() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = create_in(tmp.path(), "svc", BrokerKind::NatsJs).unwrap();

        let main = std::fs::read_to_string(dir.join("src/main.rs")).unwrap();
        assert!(main.contains("jetstream(\"ORDERS\")"));
        assert!(main.contains("include_publishing_on"));
        assert!(!main.contains("{{name}}"));
    }

    #[test]
    fn refuses_existing_directory() {
        let tmp = tempfile::tempdir().unwrap();
        create_in(tmp.path(), "svc", BrokerKind::Memory).unwrap();
        assert!(create_in(tmp.path(), "svc", BrokerKind::Memory).is_err());
    }
}
