use ruststream::prelude::*;
use serde::Deserialize;

// The mnemonic's typo guard, one direction: the serde infinitive on a payload view picks the
// codec lane, which cannot borrow - the program must fail instead of silently switching lanes.
#[derive(Deserialize)]
struct Frame<'a>(&'a [u8]);

struct Ingest;

impl Handle<Frame<'_>> for Ingest {
    async fn handle(
        &self,
        frame: &Frame<'_>,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let _ = frame.0.len();
        Ok(())
    }
}

fn main() {}
