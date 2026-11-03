//! A miner's loop against `TemplateSource` and `EventSource` only: build the
//! coinbase and header with `Template`'s helpers, find a nonce, submit, and
//! learn of new work from `wait_next` or a block event, whichever comes first.
use crate::next_event;
use bitcoin::{Amount, BlockHash, Script};
use bitcoin_node_api::{
    ApiResult, ChainSource, NodeEvent, Solution, Template, TemplateSource, WaitOptions,
};
use futures_core::Stream;
use std::time::Duration;

/// Tries every nonce; `None` if none meets the target (bump `extra_nonce`).
/// A CPU miner: only sensible on regtest.
pub fn solve(t: &Template, payout: &Script, extra_nonce: u32) -> Option<Solution> {
    let mut sol = Solution {
        version: t.version,
        time: t.cur_time.max(t.min_time),
        nonce: 0,
        coinbase: t.coinbase_tx(payout, &extra_nonce.to_le_bytes()),
    };
    let mut h = t.header(&sol);
    let target = h.target();
    loop {
        if target.is_met_by(h.block_hash()) {
            sol.nonce = h.nonce;
            return Some(sol);
        }
        h.nonce = h.nonce.checked_add(1)?;
    }
}

/// Solves `t` and submits it. Returns the new block's hash.
pub async fn mine<S: TemplateSource>(
    src: &S,
    t: &Template,
    payout: &Script,
) -> ApiResult<BlockHash> {
    let sol = (0..)
        .find_map(|extra_nonce| solve(t, payout, extra_nonce))
        .expect("some extra nonce works");
    src.submit_solution(t, &sol).await?;
    Ok(t.header(&sol).block_hash())
}

/// Why the current template is no longer the one to mine.
#[derive(Debug)]
pub enum NewWork {
    /// From `wait_next`: fees rose past the threshold, or the tip moved.
    Template(Template),
    /// A block event, and the tip is no longer the template's parent.
    Block(BlockHash),
    /// Events were lost; fetch a fresh template.
    Lagged,
    Timeout,
}

/// Waits for whichever comes first: a better template or a new block.
///
/// A block event carries only a hash, so it can't say whether it is newer
/// than `t`: our own just-mined blocks arrive late, too. So an event only
/// counts once `tip()` confirms that `t` is stale.
pub async fn next_work<S, E>(
    src: &S,
    t: &Template,
    events: &mut E,
    fee_threshold: Option<Amount>,
    timeout: Duration,
) -> ApiResult<NewWork>
where
    S: TemplateSource + ChainSource,
    E: Stream<Item = ApiResult<NodeEvent>> + Unpin,
{
    let opts = WaitOptions {
        timeout: Some(timeout),
        fee_threshold,
    };
    let wait = src.wait_next(t, opts);
    tokio::pin!(wait);
    loop {
        tokio::select! {
            next = &mut wait => {
                return Ok(next?.map_or(NewWork::Timeout, NewWork::Template));
            }
            ev = next_event(events) => match ev {
                Some(Ok(NodeEvent::BlockConnected(h))) if h != t.prev_hash => {
                    if src.tip().await?.hash != t.prev_hash {
                        return Ok(NewWork::Block(h));
                    }
                }
                Some(Ok(NodeEvent::Lagged)) => return Ok(NewWork::Lagged),
                Some(Ok(_)) => {}
                Some(Err(e)) => return Err(e),
                // No more events: `wait_next` alone decides.
                None => return Ok(wait.await?.map_or(NewWork::Timeout, NewWork::Template)),
            },
        }
    }
}
