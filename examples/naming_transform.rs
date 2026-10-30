//! Naming transforms: a transform that declares `Names` sets the destination per delivery, where
//! the reply type leaves it open. The scenarios are tests.
//!
//! ```text
//! cargo test --example naming_transform --features testing,macros,memory,json
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream::memory::prelude::*;
use ruststream::runtime::{Outgoing, PublishContext};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Question {
    id: u64,
}

/// Declares no destination, so the reply position offers the naming right.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Answer {
    id: u64,
}

// --8<-- [start:transform]
/// Answers on the inbox the question named in its `reply-to` header. The header's bytes become
/// the destination as they are: one UTF-8 check, no copy.
struct ToInbox;

impl<C, Options> PublishTransform<ForReply<C>, Options> for ToInbox {
    type Destination = Names;

    fn apply(
        &self,
        out: &mut Outgoing<'_>,
        _options: &mut Option<Options>,
        cx: &PublishContext<'_, C>,
    ) {
        if let Some(inbox) = cx.headers().get_shared("reply-to")
            && let Ok(name) = Str::try_from(inbox)
        {
            out.set_name(name);
        }
    }
}
// --8<-- [end:transform]

// --8<-- [start:app]
/// `"answers"` is the fallback, and what the generated document reports.
#[subscriber("questions", reply("answers"))]
async fn answer(question: &Question) -> Answer {
    Answer { id: question.id }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("answers", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(answer).out_reply(Publish).transform(ToInbox)
    })
}
// --8<-- [end:app]

// The scenarios are the tests below; the command in the header runs them.
fn main() {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{Answer, Question, app};

    #[tokio::test]
    async fn the_answer_goes_to_the_inbox_the_question_named() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();
        let mut headers = HeaderMap::new();
        headers.insert("reply-to", "inbox.7");
        broker
            .message(&Question { id: 7 })
            .to("questions")
            .with_headers(headers)
            .publish()
            .await?;

        broker
            .published::<Answer>("inbox.7")
            .assert_called_once()
            .with(&Answer { id: 7 });
        broker.published::<Answer>("answers").assert_not_called();
        Ok(())
    }

    #[tokio::test]
    async fn a_question_that_names_no_inbox_is_answered_on_the_fallback()
    -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();
        broker
            .message(&Question { id: 8 })
            .to("questions")
            .publish()
            .await?;

        broker
            .published::<Answer>("answers")
            .assert_called_once()
            .with(&Answer { id: 8 });
        Ok(())
    }
}
