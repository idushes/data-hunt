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

## Google authentication

Set `GOOGLE_CLIENT_ID` to the OAuth 2.0 Web client ID used by Google Identity
Services on the frontend:

```env
GOOGLE_CLIENT_ID=1234567890-example.apps.googleusercontent.com
```

`POST /web3/google/login` accepts the Google ID credential, verifies its
signature and audience on the server, and returns the same Data Hunt session
token used by wallet login. Google accounts are keyed only by Google's stable
`sub` identifier; email addresses and Google access tokens are not stored.

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
API using the funder address: an existing profile proxy/Safe or a new deposit
wallet. It returns a stable
`{wallet}:portfolio` summary row, a stable `{wallet}:pusd` collateral-balance
row read directly from the official pUSD contract on Polygon, and current market
positions. The export includes outcomes, prices, value, PnL, redeemable/mergeable
flags, and `total_account_value_usd` across pUSD cash and market positions. No
Polymarket API key is required. Market rows default to positions of at least one
outcome token; pass `size_threshold=0` to include dust. Responses use the shared
60-second CSV cache and dedicated Polymarket and Polygon RPC queues.

## Pendle positions

`GET /pendle/positions.csv?address=0x...` reads Pendle's public cross-chain
portfolio endpoint and returns a stable `{wallet}:portfolio` summary followed
by PT, YT, LP, cross-chain PT, SY, and claimable-reward rows. Position values
are in USD; token balances and claimable amounts remain in the raw integer units
returned by Pendle. Pass `include_closed=true` to include closed markets. Pendle
may cache claimable reward amounts for up to 24 hours. No Pendle API key is
required. Responses use the shared 60-second CSV cache and a dedicated outbound
queue configured below Pendle's published free-tier limit.

## Compound III risk metrics

`GET /compound/positions.csv?address=0x...&chain_id=1` returns each active
Compound III base and collateral row with market-level risk metrics. LTV uses
raw collateral value; borrow and liquidation capacities weight every
collateral asset by the corresponding on-chain Compound factor. The same LTV,
capacity, liquidation usage, and health factor values are repeated on every row
from one Comet market so a Sheets lookup by `position_id` remains sufficient.

## Uniswap V4 positions

`GET /uniswap/v4/positions.csv?address=0x...&chain_id=1` returns owned
Uniswap V4 NFT positions on Ethereum, Base, or Arbitrum. The export includes
token amounts, range status, pool and hook identifiers, USD value when one side
is a known stablecoin, and claimable fees calculated directly from V4
PoolManager state. Position discovery uses the public Blockscout NFT index and
ownership is verified onchain. Pass `include_closed=true` to include NFT
positions whose liquidity is zero. Responses use the shared 60-second CSV
cache and the existing per-provider RPC and Blockscout queues.

## PancakeSwap V3 positions

`GET /pancakeswap/positions.csv?address=0x...&chain_id=56` returns owned
PancakeSwap V3 NFT liquidity positions on BNB Chain or Ethereum. The export
includes current token amounts, range status, USD value when one side is a
known stablecoin, and fees available to claim. Pass `include_closed=true` to
include positions with zero liquidity and no remaining fees. Responses use the
shared 60-second CSV cache and the chain-specific outbound RPC queue.

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

## Bybit account data

`GET /bybit/account.csv?capsule=...` returns a Unified Account summary,
non-zero asset balances, and open USDT/USDC linear and inverse derivative
positions. Create the encrypted access key with `POST /bybit/capsule`; only
Read-only Bybit keys are accepted. Raw credentials are validated and encrypted
in memory, never persisted, and the capsule is stored only in the user's
browser. Select `region` when the account uses a regional Bybit API endpoint.
Responses use the shared 60-second CSV cache and a dedicated outbound queue.

## Binance account data

`GET /binance/account.csv?capsule=...` returns non-zero Spot balances with
estimated USD values and, when available, USD-M Futures balances and open
positions. Create the encrypted access key with `POST /binance/capsule`; only
read-only Binance keys are accepted. Raw credentials are validated and
encrypted in memory, never persisted, and the capsule is stored only in the
user's browser. Responses use the shared CSV cache and Binance outbound queue.

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
