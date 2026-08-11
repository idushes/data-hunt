## Feature request administration

Set `FEATURE_REQUEST_ADMIN_ADDRESSES` to a comma-separated list of EVM wallet
addresses. Accounts authenticated with any address in this allowlist can move
community requests between `requested`, `planned`, `in_progress`, and
`released`.

Example:

```env
FEATURE_REQUEST_ADMIN_ADDRESSES=0x1234...,0xabcd...
```

The same admin wallet list protects `GET /admin/analytics` and
`GET /admin/analytics/queues`. Usage analytics
store only daily request counts grouped by internal account ID, source, and
response class. Wallet addresses, IP addresses, query parameters, formulas,
and credentials are not stored in analytics.

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

The admin queue endpoint reports live waiting and in-flight counts aggregated
across backend instances. Per-instance Redis counters expire automatically, so
an interrupted instance cannot leave stale activity in the dashboard.

## Polymarket positions

`GET /polymarket/positions.csv?address=0x...` reads the public Polymarket Data
API using the profile/proxy wallet address. It returns a stable
`{wallet}:portfolio` summary row followed by current market positions, including
outcomes, prices, value, PnL, and redeemable/mergeable flags. No Polymarket API
key is required. Market rows default to positions of at least one outcome token;
pass `size_threshold=0` to include dust. The portfolio summary remains the full
value. Responses use the shared 60-second CSV cache and the dedicated Polymarket
outbound queue.

## Pendle positions

`GET /pendle/positions.csv?address=0x...` reads Pendle's public cross-chain
portfolio endpoint and returns a stable `{wallet}:portfolio` summary followed
by PT, YT, LP, cross-chain PT, SY, and claimable-reward rows. Position values
are in USD; token balances and claimable amounts remain in the raw integer units
returned by Pendle. Pass `include_closed=true` to include closed markets. Pendle
may cache claimable reward amounts for up to 24 hours. No Pendle API key is
required. Responses use the shared 60-second CSV cache and a dedicated outbound
queue configured below Pendle's published free-tier limit.

## Uniswap V4 positions

`GET /uniswap/v4/positions.csv?address=0x...&chain_id=1` returns owned
Uniswap V4 NFT positions on Ethereum, Base, or Arbitrum. The export includes
token amounts, range status, pool and hook identifiers, USD value when one side
is a known stablecoin, and claimable fees calculated directly from V4
PoolManager state. Position discovery uses the public Blockscout NFT index and
ownership is verified onchain. Pass `include_closed=true` to include NFT
positions whose liquidity is zero. Responses use the shared 60-second CSV
cache and the existing per-provider RPC and Blockscout queues.

## Multi-wallet stablecoin balances

`GET /stablecoins/balances.csv` accepts up to 20 wallets per request across
EVM, Solana, and TRON. Pass multiple wallets in `address`, `wallet`, or
`tron_address` as comma-separated values. Duplicate addresses are removed
before the queued RPC requests are created. USDC and USDT remain the first
rows for backward compatibility, followed by up to 15 high-volume USD
stablecoins available on the selected network. The list is a fixed snapshot so
symbols, balance IDs, and short resource links do not change with market rank.

## Coinbase credential capsules

Coinbase credentials are accepted only by `POST /coinbase/capsule`, validated
as View-only, encrypted with AES-256-GCM, and returned without being persisted.
Balance URLs accept the encrypted capsule instead of raw credentials. Capsules
do not expire; they stop working when the Coinbase API key is revoked.

Configure a versioned keyring and keep old keys available for decryption during
rotation:

```env
COINBASE_CAPSULE_ACTIVE_KEY_ID=v2
COINBASE_CAPSULE_KEYS_JSON={"v1":"<base64url-32-byte-key>","v2":"<base64url-32-byte-key>"}
```

## Short value resources

`POST /value-resources` stores a credential-free description of one requested
cell and returns a reusable short ID. Identical normalized requests reuse one
database row and ID. Google Sheets can then import the value from `/v/{id}`.

Provider credentials are never accepted in the stored descriptor. Pass a
required readonly token or Coinbase capsule only as an additional query
parameter on `/v/{id}`. The legacy `/value?...` route remains available for
existing spreadsheets.

Coinbase accepts a required Main `capsule` and an optional separate
`intx_capsule` for Perpetuals/INTX portfolios. When `intx_capsule` is omitted,
the Main capsule is reused for backward compatibility. A missing INTX
permission no longer prevents Main balances from loading.

Signed-in users can request a revocable, non-expiring, read-only token from
`POST /web3/sheets-token`. The Sheets helper adds it as `auth_token` to new
short links and direct data requests. It cannot manage the user account. All
data routes require either this scoped token in `auth_token` or a login token
in the `Authorization` header. Anonymous data access is disabled. Requests
share a Redis-backed fixed-window limit of 120 per minute per authenticated
account. Configure this limit with `VALUE_RATE_LIMIT_AUTHENTICATED` and
`VALUE_RATE_LIMIT_WINDOW_SECONDS`.
