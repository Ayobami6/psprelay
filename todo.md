# TODO

## Routing & Webhooks

- [ ] Support deterministic DVA-to-product routing (replace disabled `broadcast-dva` fan-out) — see [#9](https://github.com/Ayobami6/psprelay/issues/9)
  - Implement a `(provider, account_number) -> product_name` registry or `account_reference` prefixing so DVA/bank-transfer webhooks route only to the owning product and fail closed to `unmatched` / DLQ otherwise.
