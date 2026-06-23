//! `ruststream new`: writes a ready-to-run project from a built-in template.
//!
//! Templates are embedded at compile time and rendered by substituting `{{name}}`. Each broker
//! kind maps to a small set of files (manifest, `main.rs`, handlers, router); the generated
//! `main.rs` uses `#[ruststream::app]`, so the project has no hand-written runtime boilerplate.

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
const NATS_ROUTES: &str = include_str!("templates/nats/routes.rs.in");

const NATS_JS_CARGO: &str = include_str!("templates/nats-js/Cargo.toml.in");
const NATS_JS_MAIN: &str = include_str!("templates/nats-js/main.rs.in");
const NATS_JS_ORDERS: &str = include_str!("templates/nats-js/orders.rs.in");
const NATS_JS_ROUTES: &str = include_str!("templates/nats-js/routes.rs.in");

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
            ("src/routes.rs", NATS_ROUTES),
        ],
        BrokerKind::NatsJs => &[
            ("Cargo.toml", NATS_JS_CARGO),
            ("src/main.rs", NATS_JS_MAIN),
            ("src/orders.rs", NATS_JS_ORDERS),
            ("src/routes.rs", NATS_JS_ROUTES),
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
    use std::fmt::Write as _;

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
        let routes = std::fs::read_to_string(dir.join("src/routes.rs")).unwrap();
        assert!(cargo.contains("ruststream-nats = "));
        assert!(main.contains("NatsBroker::new("));
        assert!(main.contains("include_router"));
        assert!(routes.contains("impl RouterDef<NatsBroker>"));
        assert!(orders.contains("JsonSchema"));
        assert!(!dir.join("src/stream.rs").exists());
        for file in [&cargo, &main, &orders, &routes] {
            assert!(!file.contains("{{name}}"));
        }
    }

    #[test]
    fn writes_nats_js_project() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = create_in(tmp.path(), "svc", BrokerKind::NatsJs).unwrap();

        let main = std::fs::read_to_string(dir.join("src/main.rs")).unwrap();
        let orders = std::fs::read_to_string(dir.join("src/orders.rs")).unwrap();
        let routes = std::fs::read_to_string(dir.join("src/routes.rs")).unwrap();
        // The JetStream `SubscribeOptions` builder sits in the subscriber decorator.
        assert!(orders.contains("jetstream(\"ORDERS\")"));
        assert!(orders.contains("durable(\"svc-worker\")"));
        assert!(main.contains("include_router"));
        assert!(routes.contains("impl RouterDef<NatsBroker>"));
        assert!(!dir.join("src/stream.rs").exists());
        for file in [&main, &orders, &routes] {
            assert!(!file.contains("{{name}}"));
        }
    }

    #[test]
    fn refuses_existing_directory() {
        let tmp = tempfile::tempdir().unwrap();
        create_in(tmp.path(), "svc", BrokerKind::Memory).unwrap();
        assert!(create_in(tmp.path(), "svc", BrokerKind::Memory).is_err());
    }

    /// Renders the `memory` scaffold and compiles it against this workspace, so template rot - an
    /// API drift or an invalid feature in `Cargo.toml.in` - fails CI here instead of in a user's
    /// first `cargo build`. The string-substitution tests above never compile the output; this one
    /// does, the `memory` template being the only broker the core owns.
    ///
    /// Heavy (a nested `cargo build`), so it is opt-in like the trybuild UI test: the stable CI job
    /// sets `RUN_SCAFFOLD_BUILD=1`; the 1.85 job, coverage, and a local `cargo test` leave it unset
    /// and skip. The template pins the published `ruststream = "0.x"`, which is not on crates.io
    /// during a pre-release cycle, so the generated manifest is patched to this workspace before the
    /// build - exercising the template's real feature list and code, sourced locally.
    #[test]
    fn memory_scaffold_compiles() {
        if std::env::var("RUN_SCAFFOLD_BUILD").as_deref() != Ok("1") {
            eprintln!(
                "skipping scaffold build; set RUN_SCAFFOLD_BUILD=1 (stable toolchain) to run"
            );
            return;
        }

        let tmp = tempfile::tempdir().unwrap();
        let dir = create_in(tmp.path(), "scaffold_check", BrokerKind::Memory).unwrap();

        let workspace = env!("CARGO_MANIFEST_DIR");
        let manifest = dir.join("Cargo.toml");
        let mut cargo = std::fs::read_to_string(&manifest).unwrap();
        write!(
            cargo,
            "\n[patch.crates-io]\nruststream = {{ path = {workspace:?} }}\n"
        )
        .unwrap();
        std::fs::write(&manifest, cargo).unwrap();

        let cargo_bin = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_owned());
        let status = std::process::Command::new(cargo_bin)
            .current_dir(&dir)
            .args(["build"])
            // A separate target dir keeps the nested build off the outer test run's lock.
            .env("CARGO_TARGET_DIR", tmp.path().join("target"))
            .status()
            .expect("spawn cargo build for the scaffolded project");
        assert!(
            status.success(),
            "the scaffolded memory project failed to compile"
        );
    }
}
