//! Scope strings round-trip through `Display`; any method name and wallet
//! gets an answer from the permission table without panicking.
#![no_main]
use bitcoin_gateway::scopes::{Scope, method_area, required};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: (&str, &str, bool)| {
    let (s, wallet, on_wallet) = input;
    if let Some(scope) = Scope::parse(s) {
        let shown = scope.to_string();
        assert_eq!(
            Scope::parse(&shown),
            Some(scope.clone()),
            "{s:?} -> {shown:?}"
        );
        if let Some(w) = scope.wallet() {
            assert!(!w.is_empty());
        }
    }
    let _ = method_area(s);
    let _ = required(s, on_wallet.then_some(wallet));
});
