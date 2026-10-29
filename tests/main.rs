//! The integration suite, one test binary: each file or directory under `tests/` is a module
//! of it.

#[cfg(feature = "memory")]
mod carrying;
#[cfg(feature = "memory")]
mod common;
#[cfg(all(
    feature = "macros",
    feature = "memory",
    feature = "json",
    feature = "testing"
))]
mod log_capture;

mod app_cli_coverage;
mod app_dispatch;
mod app_start;
mod app_trait;
mod asyncapi;
mod batch_reply_context;
mod batch_router_coverage;
mod batch_seek;
mod batch_subscriber;
mod batch_uniform_outcome;
mod carried_lane;
mod codec_free_lanes;
mod codecs;
mod composition_rules;
mod conformance_backlog;
mod conformance_carries;
mod conformance_message_shape;
mod conformance_self;
mod conformance_settlement;
mod context_extensions;
mod ctx_extractor;
mod debug_forms;
mod decode_error;
mod failure_policy;
mod from_context;
mod header_materialization;
mod health_probe;
mod in_messages;
mod in_process;
mod lane_traits_without_macros;
mod lanes;
mod macro_params;
mod macro_subscriber;
mod manual_matrix;
mod manual_out_injection;
mod manual_out_slots;
mod manual_publish_options;
mod manual_raw_subscriber;
mod manual_slot_capabilities;
mod memory_prelude;
mod memory_routing;
mod metrics;
mod mount_site_publisher;
mod opentelemetry;
mod out_injection;
mod out_slots;
mod out_transforms;
mod owned_transaction;
mod poll_diagnostics;
mod post_settle_hooks;
mod prelude;
mod publish_builder;
mod publish_context;
mod publish_coverage;
mod publish_options;
mod publish_payload;
mod publish_policy;
mod raw_subscriber;
mod redirect;
mod reply_destination;
mod reply_message_pair;
mod reply_wiring;
mod retry_broker_moved;
mod retry_copy_shutdown;
mod retry_declaration;
mod retry_declaration_bare_name;
mod retry_redelivery;
mod retry_semantics;
mod router_include;
mod router_parity;
mod router_publishing;
mod scope_codec;
mod seek_api;
mod self_serialising;
mod settings_read;
mod startup_pairing;
mod subscriber_return_forms;
mod subscriber_settings;
mod testing_harness;
mod testing_harness_edges;
mod testing_hooks;
mod testing_live;
mod testing_shutdown_order;
mod testing_startup_order;
mod testing_subscription_identity;
mod threads;
mod threads_placement;
mod transaction_scope;
mod typed_headers;
mod ui;
mod ui_codec_free;
mod workers;
