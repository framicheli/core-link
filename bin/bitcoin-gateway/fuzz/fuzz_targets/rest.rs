//! Any HTTP request to the REST router: never panics, and never a 500
//! (the mock never fails, so a 500 would be the gateway's own bug).
#![no_main]
#[path = "common.rs"]
mod common;

use arbitrary::Arbitrary;
use axum::body::Body;
use axum::http::{Method, Request};
use http_body_util::BodyExt;
use libfuzzer_sys::fuzz_target;
use tower::ServiceExt;

const PREFIXES: &[&str] = &[
    "/v1/chain/tip",
    "/v1/chain/heights/",
    "/v1/blocks/",
    "/v1/tx/",
    "/v1/tx",
    "/v1/tx/test",
    "/v1/utxos/",
    "/v1/mempool/txids",
    "/v1/mempool/snapshot",
    "/v1/mempool/",
    "/v1/fees/estimate",
    "/v1/packages",
    "/v1/openapi.json",
    "/",
];

#[derive(Arbitrary, Debug)]
struct Input<'a> {
    method: u8,
    prefix: u8,
    tail: &'a str,
    query: Option<&'a str>,
    auth: u8,
    accept_binary: bool,
    content_type: u8,
    if_none_match: Option<&'a str>,
    body: &'a [u8],
}

fuzz_target!(|i: Input| {
    let h = common::harness();
    let method =
        [Method::GET, Method::POST, Method::HEAD, Method::PUT][i.method as usize % 4].clone();
    let mut uri = format!("{}{}", PREFIXES[i.prefix as usize % PREFIXES.len()], i.tail);
    if let Some(q) = i.query {
        uri = format!("{uri}?{q}");
    }
    let mut b = Request::builder().method(method).uri(uri);
    // 0..4: a valid key; 4: garbage; 5: another scheme; else none.
    match i.auth % 7 {
        n @ 0..4 => {
            b = b.header(
                "authorization",
                format!("Bearer {}", common::TOKENS[n as usize]),
            )
        }
        4 => b = b.header("authorization", "Bearer not-a-token"),
        5 => b = b.header("authorization", "Basic dTpw"),
        _ => {}
    }
    if i.accept_binary {
        b = b.header("accept", "application/octet-stream");
    }
    b = match i.content_type % 3 {
        0 => b.header("content-type", "application/json"),
        1 => b.header("content-type", "application/octet-stream"),
        _ => b,
    };
    if let Some(e) = i.if_none_match {
        b = b.header("if-none-match", e);
    }
    let Ok(req) = b.body(Body::from(i.body.to_vec())) else {
        return; // not a valid URI or header value: never reaches the router
    };
    let app = h.gw.http_router();
    h.rt.block_on(async {
        let resp = app.oneshot(req).await.unwrap();
        let status = resp.status();
        assert_ne!(status.as_u16(), 500, "{resp:?}");
        let sse = resp
            .headers()
            .get("content-type")
            .is_some_and(|c| c.as_bytes().starts_with(b"text/event-stream"));
        if !sse {
            let _ = resp.into_body().collect().await;
        }
    });
});
