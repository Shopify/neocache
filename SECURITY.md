# Security Policy

## Supported versions

`neocache` is in active development. Security fixes are made against the latest
released `0.x` minor version on crates.io and the `main` branch.

| Version | Supported          |
| ------- | ------------------ |
| `0.1.x` | :white_check_mark: |
| `< 0.1` | :x:                |

When a `1.0` is published, this policy will be revised to support the most
recent stable major version.

## Reporting a vulnerability

**Please do not report security vulnerabilities through public GitHub issues,
discussions, or pull requests.**

`neocache` uses `unsafe` Rust internally (via the `hashbrown` raw API and a
vendored reader-writer lock). Bugs in this crate can therefore cause memory
unsafety, data races, or undefined behaviour in downstream programs. We treat
any such report as a security issue.

To report a vulnerability, use **either** of the following private channels:

1. **GitHub private vulnerability reporting** — preferred. Open
   <https://github.com/Shopify/neocache/security/advisories/new> and submit a
   draft advisory. Only repository maintainers can see it.
2. **Email** — send a description to
   [`security@shopify.com`](mailto:security@shopify.com) with `neocache` in the
   subject line. PGP is available on request.

Please include, where possible:

- The version (or commit SHA) of `neocache` affected.
- A minimal reproducer (a Rust test or short program is ideal).
- The platform, Rust toolchain version, and feature flags used.
- The observed impact (e.g. crash, data race detected by ThreadSanitizer,
  out-of-bounds access reported by Miri, panic on a code path that should be
  total).
- Whether the issue requires `unsafe` from the caller, or is reachable from
  safe Rust only.
- Any suggested mitigation, if you have one.

## Disclosure timeline

We aim for the following response targets:

| Stage                                | Target                                      |
| ------------------------------------ | ------------------------------------------- |
| Acknowledgement of report            | Within **2 business days**                  |
| Initial triage and severity estimate | Within **5 business days**                  |
| Fix or mitigation in `main`          | Within **30 days** for High/Critical issues |
| Public disclosure & advisory         | After a fixed release is published          |

We coordinate disclosure with the reporter and will credit you in the published
advisory unless you ask otherwise.

## Out of scope

The following are not considered security vulnerabilities for this project:

- Performance pathologies that do not cause memory unsafety. Tail-latency
  surprises (for example, an `insert` blocking on a long S3-FIFO eviction
  sweep) should be reported as regular GitHub issues.
- Behaviour that requires a malicious `Hash`, `Eq`, `Clone`, or `Drop`
  implementation on user-supplied `K` or `V`. The crate documents that it
  trusts these traits.
- Hash-flooding attacks against a `NeoCache` constructed with a non-randomized
  hasher (e.g. `std::collections::hash_map::DefaultHasher` with a fixed seed).
  The default `ahash::RandomState` is randomized; substituting a deterministic
  hasher is an opt-in trade-off.
- Issues in third-party dependencies. Please report those upstream; we will
  bump the dependency once a fix is available.

## Hardening guidance for users

If you embed `neocache` in a service that handles untrusted input:

- Keep the default `ahash::RandomState` hasher unless you have a specific
  reason to substitute one.
- Set `cache_capacity` to a finite value sized to your memory budget; avoid
  `new_unbounded()` for caches keyed on attacker-controlled data.
- Do not hold `Ref` / `RefMut` guards across `.await` points or across calls
  that may re-enter the same shard. See `docs/usage-guide.md`.
