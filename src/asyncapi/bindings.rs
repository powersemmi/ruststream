//! Protocol bindings: what a broker adds to the document that no broker-agnostic core can know.
//!
//! The specification keeps a closed list of protocols and, per protocol, a binding object for each
//! of the four levels (server, channel, operation, message). A broker crate fills the levels it
//! has something to say about; the core carries the values without ever naming a broker's field.

use std::collections::BTreeMap;

use serde::ser::SerializeMap;
use serde::{Serialize, Serializer};
use serde_json::Value;
use thiserror::Error;

/// The protocol keys the specification allows in a bindings object, in its own order.
///
/// A key outside this list is rejected at construction: the document would carry an object no
/// tool can read, and the mistake surfaces at startup rather than in a published file.
const PROTOCOLS: &[&str] = &[
    "amqp",
    "amqp1",
    "anypointmq",
    "googlepubsub",
    "http",
    "ibmmq",
    "jms",
    "kafka",
    "mercure",
    "mqtt",
    "mqtt5",
    "nats",
    "pulsar",
    "redis",
    "ros2",
    "sns",
    "solace",
    "sqs",
    "stomp",
    "websockets",
];

/// One binding object: the protocol it belongs to, the binding version, and the body.
///
/// The body is whatever the broker's own descriptor or policy knows, serialized once at
/// construction. The core writes `bindingVersion` itself, so a broker cannot forget it, and the
/// protocol key is checked against the specification's list.
///
/// # Examples
///
/// ```
/// use ruststream::asyncapi::Binding;
/// use serde::Serialize;
///
/// #[derive(Serialize)]
/// struct KafkaOperation {
///     #[serde(rename = "groupId")]
///     group_id: String,
/// }
///
/// let binding = Binding::new("kafka", "0.5.0", &KafkaOperation { group_id: "billing".into() })?;
/// assert_eq!(binding.protocol(), "kafka");
///
/// // A protocol the specification does not list is refused here, not in the document.
/// assert!(Binding::new("kinesis", "0.1.0", &()).is_err());
/// # Ok::<_, ruststream::asyncapi::BindingError>(())
/// ```
///
/// # Errors
///
/// See [`BindingError`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Binding {
    key: &'static str,
    body: String,
}

impl Binding {
    /// A binding for `protocol`, at `version`, carrying `body`.
    ///
    /// `body` must serialize to a JSON object: a binding object holds named fields, and
    /// `bindingVersion` is written into it here.
    ///
    /// # Errors
    ///
    /// [`BindingError::UnknownProtocol`] when `protocol` is not one the specification lists,
    /// [`BindingError::NotAnObject`] when `body` does not serialize to an object, and
    /// [`BindingError::Serialize`] when serializing it fails.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::asyncapi::Binding;
    /// use serde::Serialize;
    ///
    /// #[derive(Serialize)]
    /// struct NatsOperation {
    ///     queue: &'static str,
    /// }
    ///
    /// let binding = Binding::new("nats", "0.1.0", &NatsOperation { queue: "workers" })?;
    /// assert_eq!(binding.protocol(), "nats");
    /// # Ok::<_, ruststream::asyncapi::BindingError>(())
    /// ```
    pub fn new<T: Serialize>(
        protocol: &'static str,
        version: &'static str,
        body: &T,
    ) -> Result<Self, BindingError> {
        if !PROTOCOLS.contains(&protocol) {
            return Err(BindingError::UnknownProtocol { protocol });
        }
        let mut value = serde_json::to_value(body)?;
        let object = value
            .as_object_mut()
            .ok_or(BindingError::NotAnObject { key: protocol })?;
        object.insert(
            "bindingVersion".to_owned(),
            Value::String(version.to_owned()),
        );
        Ok(Self {
            key: protocol,
            body: serde_json::to_string(&value)?,
        })
    }

    /// An `x-` extension at the level a binding would sit at, for a transport the specification
    /// has no binding for.
    ///
    /// `ZeroMQ`, Kinesis and a file transport have no binding object of their own, and the
    /// protocol keys are a closed list, so an extension is the only lawful place for what such a
    /// transport knows. There is no `bindingVersion` here: the field belongs to a binding, and
    /// an extension is not one.
    ///
    /// # Errors
    ///
    /// [`BindingError::NotAnExtension`] when `name` does not start with `x-` or carries a
    /// character the specification's extension pattern forbids, [`BindingError::NotAnObject`]
    /// when `body` does not serialize to an object, and [`BindingError::Serialize`] when
    /// serializing it fails.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::asyncapi::Binding;
    /// use serde::Serialize;
    ///
    /// #[derive(Serialize)]
    /// struct Zmq {
    ///     socket: &'static str,
    /// }
    ///
    /// let binding = Binding::extension("x-zeromq", &Zmq { socket: "PULL" })?;
    /// assert_eq!(binding.protocol(), "x-zeromq");
    ///
    /// assert!(Binding::extension("zeromq", &Zmq { socket: "PULL" }).is_err());
    /// # Ok::<_, ruststream::asyncapi::BindingError>(())
    /// ```
    pub fn extension<T: Serialize>(name: &'static str, body: &T) -> Result<Self, BindingError> {
        let legal = name.starts_with("x-")
            && name.len() > 2
            && name[2..]
                .chars()
                .all(|c| c.is_alphanumeric() || matches!(c, '_' | '-' | '.'));
        if !legal {
            return Err(BindingError::NotAnExtension { name });
        }
        let value = serde_json::to_value(body)?;
        if !value.is_object() {
            return Err(BindingError::NotAnObject { key: name });
        }
        Ok(Self {
            key: name,
            body: serde_json::to_string(&value)?,
        })
    }

    /// The protocol key this binding sits under, or the extension name.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::asyncapi::Binding;
    /// use std::collections::BTreeMap;
    ///
    /// let binding = Binding::new("redis", "0.1.0", &BTreeMap::<String, String>::new())?;
    /// assert_eq!(binding.protocol(), "redis");
    /// # Ok::<_, ruststream::asyncapi::BindingError>(())
    /// ```
    #[must_use]
    pub const fn protocol(&self) -> &'static str {
        self.key
    }
}

/// The bindings of one level: one body per protocol.
///
/// Empty by default, and an empty set never reaches the document. A broker that says nothing
/// changes nothing.
///
/// # Examples
///
/// ```
/// use ruststream::asyncapi::{Binding, Bindings};
/// use serde::Serialize;
///
/// #[derive(Serialize)]
/// struct Queue {
///     name: &'static str,
/// }
///
/// let bindings = Bindings::new().with(Binding::new("sqs", "0.3.0", &Queue { name: "orders" })?);
///
/// assert!(!bindings.is_empty());
/// assert!(Bindings::new().is_empty());
/// # Ok::<_, ruststream::asyncapi::BindingError>(())
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Bindings(BTreeMap<&'static str, String>);

impl Bindings {
    /// An empty set.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::asyncapi::Bindings;
    ///
    /// assert!(Bindings::new().is_empty());
    /// ```
    #[must_use]
    pub const fn new() -> Self {
        Self(BTreeMap::new())
    }

    /// Adds one binding, replacing whatever was under the same protocol key.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::asyncapi::{Binding, Bindings};
    /// use std::collections::BTreeMap;
    ///
    /// let bindings = Bindings::new()
    ///     .with(Binding::new("mqtt", "0.2.0", &BTreeMap::<String, u8>::new())?);
    ///
    /// assert!(!bindings.is_empty());
    /// # Ok::<_, ruststream::asyncapi::BindingError>(())
    /// ```
    #[must_use]
    pub fn with(mut self, binding: Binding) -> Self {
        self.0.insert(binding.key, binding.body);
        self
    }

    /// Adds every binding of `other` this set has no entry for, keeping its own on a clash.
    ///
    /// One message component is shared by every registration that carries the type, so two
    /// descriptors may each contribute to it. First one wins per protocol, the way a conflicting
    /// headers schema does.
    pub(crate) fn fill_from(&mut self, other: &Self) {
        for (key, body) in &other.0 {
            self.0.entry(key).or_insert_with(|| body.clone());
        }
    }

    /// True when nothing was added, so the document leaves the object out.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::asyncapi::Bindings;
    ///
    /// assert!(Bindings::new().is_empty());
    /// ```
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl Serialize for Bindings {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (key, body) in &self.0 {
            // The body round-trips through the string the constructor serialized, so parsing it
            // back cannot fail; Null is the unreachable fallback, not an error path.
            let value = serde_json::from_str::<Value>(body).unwrap_or(Value::Null);
            map.serialize_entry(key, &value)?;
        }
        map.end()
    }
}

/// What one subscription contributes to the document, one set per level.
///
/// Assembled by the runtime at mount time from a
/// [`SubscriptionSource`](crate::SubscriptionSource), and carried in the registration's
/// metadata until the document is built.
///
/// # Examples
///
/// ```
/// use ruststream::asyncapi::SubscriptionBindings;
///
/// let bindings = SubscriptionBindings::default();
///
/// assert!(bindings.channel.is_empty());
/// assert!(bindings.operation.is_empty());
/// assert!(bindings.message.is_empty());
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct SubscriptionBindings {
    /// What the subscription says about its channel.
    pub channel: Bindings,
    /// What it says about the `receive` operation.
    pub operation: Bindings,
    /// What it says about the messages that arrive on it.
    pub message: Bindings,
}

/// Why a value is not a [`Binding`].
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum BindingError {
    /// The protocol key is not one the specification lists.
    #[error(
        "`{protocol}` is not an AsyncAPI protocol key; a transport the specification has no \
         binding for goes in an `x-` extension instead (Binding::extension)"
    )]
    UnknownProtocol {
        /// The key that was offered.
        protocol: &'static str,
    },
    /// The extension name does not match the specification's `x-` pattern.
    #[error(
        "`{name}` is not an extension name; it must start with `x-` and continue with letters, \
         digits, `_`, `-` or `.`"
    )]
    NotAnExtension {
        /// The name that was offered.
        name: &'static str,
    },
    /// The body did not serialize to a JSON object.
    #[error("the body of `{key}` must serialize to a JSON object: a binding holds named fields")]
    NotAnObject {
        /// The protocol key or extension name the body was offered under.
        key: &'static str,
    },
    /// Serializing the body failed.
    #[error("serializing a binding body failed: {0}")]
    Serialize(#[from] serde_json::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Serialize)]
    struct Group {
        #[serde(rename = "groupId")]
        group_id: &'static str,
    }

    #[test]
    fn the_core_writes_the_binding_version_so_a_broker_cannot_forget_it() {
        let bindings = Bindings::new().with(
            Binding::new(
                "kafka",
                "0.5.0",
                &Group {
                    group_id: "billing",
                },
            )
            .unwrap(),
        );
        let json = serde_json::to_value(&bindings).unwrap();

        assert_eq!(json["kafka"]["groupId"], "billing");
        assert_eq!(json["kafka"]["bindingVersion"], "0.5.0");
    }

    #[test]
    fn an_unlisted_protocol_is_refused_at_construction() {
        let error = Binding::new("kinesis", "0.1.0", &Group { group_id: "x" }).unwrap_err();
        assert!(matches!(
            error,
            BindingError::UnknownProtocol {
                protocol: "kinesis"
            }
        ));
    }

    #[test]
    fn an_extension_needs_the_x_prefix_and_carries_no_binding_version() {
        assert!(matches!(
            Binding::extension("kinesis", &Group { group_id: "x" }).unwrap_err(),
            BindingError::NotAnExtension { name: "kinesis" },
        ));
        assert!(matches!(
            Binding::extension("x-", &Group { group_id: "x" }).unwrap_err(),
            BindingError::NotAnExtension { name: "x-" },
        ));

        let bindings = Bindings::new()
            .with(Binding::extension("x-kinesis", &Group { group_id: "x" }).unwrap());
        let json = serde_json::to_value(&bindings).unwrap();
        assert_eq!(json["x-kinesis"]["groupId"], "x");
        assert!(json["x-kinesis"].get("bindingVersion").is_none());
    }

    #[test]
    fn a_body_that_is_not_an_object_has_no_binding_to_be() {
        assert!(matches!(
            Binding::new("kafka", "0.5.0", &"a string").unwrap_err(),
            BindingError::NotAnObject { key: "kafka" },
        ));
    }

    #[test]
    fn one_protocol_carries_one_body() {
        let bindings = Bindings::new()
            .with(Binding::new("kafka", "0.5.0", &Group { group_id: "first" }).unwrap())
            .with(Binding::new("kafka", "0.5.0", &Group { group_id: "second" }).unwrap());
        let json = serde_json::to_value(&bindings).unwrap();

        assert_eq!(json["kafka"]["groupId"], "second");
    }
}
