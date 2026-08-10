## Feature request administration

Set `FEATURE_REQUEST_ADMIN_ADDRESSES` to a comma-separated list of EVM wallet
addresses. Accounts authenticated with any address in this allowlist can move
community requests between `requested`, `planned`, `in_progress`, and
`released`.

Example:

```env
FEATURE_REQUEST_ADMIN_ADDRESSES=0x1234...,0xabcd...
```

## Redis cache and outbound queues

`REDIS_URL` enables the shared CSV cache, distributed single-flight, and
per-provider outbound API scheduling. If Redis is unavailable, the application
falls back to its bounded in-process cache and limiter, while readiness fails
when `REDIS_URL` is configured.

The Kubernetes Redis deployment is intentionally ephemeral: it has no volume,
RDB snapshots are disabled, and AOF is disabled. Cache and queue state may be
discarded safely on restart.

Provider defaults live in `outbound_queue.py`. Override individual limits with
`OUTBOUND_API_LIMITS_JSON`:

```env
OUTBOUND_API_LIMITS_JSON={"coinbase":{"requests":5,"period_seconds":1,"concurrency":2}}
```
