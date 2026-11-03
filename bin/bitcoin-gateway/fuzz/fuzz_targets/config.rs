//! Any text as a config: parsing never panics, and a config that parses
//! has keys that check (`Config::parse` already checked them).
#![no_main]
use bitcoin_gateway::config::Config;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Ok(src) = std::str::from_utf8(data) else {
        return;
    };
    if let Ok(c) = Config::parse(src) {
        let keys = c.key_specs(src).expect("parse checked the keys");
        assert_eq!(keys.len(), c.keys.len());
        for k in &keys {
            assert!(k.max_in_flight >= 1 && k.max_subscriptions >= 1);
        }
        let _ = c.wallet_scopes(src);
    }
});
