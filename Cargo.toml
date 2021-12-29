[package]
name = "nearcrowd"
version = "0.1.0"
authors = ["NEARCrowd <nearcrowd@protonmail.com>"]
edition = "2018"

[lib]
crate-type = ["cdylib", "rlib"]

[dependencies]
near-sdk = {version = "3.0.0-pre.3", git = "https://github.com/near/near-sdk-rs" }

[profile.release]
codegen-units = 1
# Tell `rustc` to optimize for small code size.
opt-level = "s"
lto = true
debug = false
panic = "abort"
overflow-checks = true

[dev-dependencies]
lazy_static = "1.4.0"
quickcheck = "0.9"
quickcheck_macros = "0.9"
log = "0.4"
rand = "0.7"
env_logger = { version = "0.7.1", default-features = false }
near-crypto = { git = "https://github.com/near/nearcore.git" }
near-primitives = { git = "https://github.com/near/nearcore.git" }
