//! The document builder reads a service through `Described`, so a runner other than tokio's gets
//! its document from the same code.

use std::collections::BTreeMap;

use ruststream_asyncapi::build_spec;
use ruststream_core::ServerSpec;
use ruststream_runtime::{AppInfo, Described, HandlerMetadata};

struct Stub {
    info: AppInfo,
    servers: BTreeMap<String, ServerSpec>,
}

impl Described for Stub {
    fn described_info(&self) -> &AppInfo {
        &self.info
    }

    fn described_servers(&self) -> &BTreeMap<String, ServerSpec> {
        &self.servers
    }

    fn described_handlers(&self) -> &[HandlerMetadata] {
        &[]
    }
}

#[test]
fn a_type_that_is_not_the_tokio_app_gets_a_document() {
    let stub = Stub {
        info: AppInfo::new("stub", "1.2.3").description("a runner of its own"),
        servers: BTreeMap::from([(
            "main".to_owned(),
            ServerSpec::new("broker.example.com:4222", "nats"),
        )]),
    };

    let spec = build_spec(&stub);

    assert_eq!(spec.info.title, "stub");
    assert_eq!(spec.info.version, "1.2.3");
    assert_eq!(
        spec.servers["main"].host.as_deref(),
        Some("broker.example.com:4222")
    );
}
