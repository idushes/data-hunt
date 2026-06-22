import asyncio
import base64
import unittest
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import HTTPException

from routers.solana import (
    GMTRADE_MARKET_DECIMALS,
    GMTRADE_PRICE_DECIMALS,
    KAMINO_FARM_USER_STATE_DISCRIMINATOR,
    KAMINO_FARMS_PROGRAM_ID,
    KAMINO_VAULT_PROGRAM_ID,
    KAMINO_VAULT_STATE_DISCRIMINATOR,
    SPL_TOKEN_2022_PROGRAM_ID,
    SPL_TOKEN_PROGRAM_ID,
    _base58_encode,
    _build_kamino_rows,
    _build_kamino_vault_token_positions,
    _build_gmtrade_perp_rows,
    _decode_gmtrade_perp_position,
    _derive_kamino_farm_user_state_address,
    _is_solana_address,
    _kamino_csv_cache,
    _gmtrade_csv_cache,
    _gmtrade_perp_csv_cache,
    _fetch_market_infos,
    _fetch_optional_lookup,
    _fetch_optional_positions,
    _fetch_token_accounts,
    _filter_positive_balance_items,
    _normalize_kamino_vault_name,
    _parse_kamino_farm_staked_shares,
    _parse_kamino_vault_state,
    _render_kamino_csv,
    _render_gmtrade_perp_csv,
    _rpc_request,
    get_kamino_csv,
    get_gmtrade_csv,
    get_gmtrade_perps_csv,
)


class FetchMarketInfosTest(unittest.IsolatedAsyncioTestCase):
    async def test_skips_graphql_query_without_mints(self):
        with patch("routers.solana._query_graphql", new_callable=AsyncMock) as query:
            result = await _fetch_market_infos(object(), [])

        self.assertEqual(result, {})
        query.assert_not_awaited()

    async def test_queries_only_requested_mints(self):
        with patch("routers.solana._query_graphql", new_callable=AsyncMock) as query:
            query.return_value = {
                "marketInfos": [
                    {"id": "mint-a", "name": "Market A"},
                    {"id": "mint-b", "name": "Market B"},
                ]
            }

            result = await _fetch_market_infos(object(), ["mint-a", "mint-b"])

        query.assert_awaited_once()
        gql_query = query.await_args.args[1]
        self.assertIn('where:{id_in:["mint-a","mint-b"]}', gql_query)
        self.assertEqual(result["mint-a"]["name"], "Market A")
        self.assertEqual(result["mint-b"]["name"], "Market B")


class PositiveBalanceItemsTest(unittest.TestCase):
    def test_filters_zero_empty_and_invalid_balances(self):
        result = _filter_positive_balance_items(
            [
                {"balance": "0", "id": "zero"},
                {"balance": None, "id": "missing"},
                {"balance": "not-a-number", "id": "invalid"},
                {"balance": "1", "id": "positive"},
            ]
        )

        self.assertEqual(result, [{"balance": "1", "id": "positive"}])


class SolanaAddressConstantsTest(unittest.TestCase):
    def test_token_program_ids_are_valid_solana_addresses(self):
        self.assertTrue(_is_solana_address(SPL_TOKEN_PROGRAM_ID))
        self.assertTrue(_is_solana_address(SPL_TOKEN_2022_PROGRAM_ID))


class TokenAccountsTest(unittest.IsolatedAsyncioTestCase):
    async def test_ignores_unsupported_token_2022_program_lookup(self):
        async def fetcher(_client, _wallet, token_program_id):
            if token_program_id == SPL_TOKEN_PROGRAM_ID:
                return [{"pubkey": "token-account"}]
            raise HTTPException(
                status_code=502, detail="unrecognized Token program id"
            )

        with patch("routers.solana._fetch_token_accounts_by_owner", fetcher):
            result = await _fetch_token_accounts(object(), "wallet")

        self.assertEqual(result, [{"pubkey": "token-account"}])


class OptionalPositionsTest(unittest.IsolatedAsyncioTestCase):
    async def test_returns_items_from_fetcher(self):
        fetcher = AsyncMock(return_value=[{"balance": "1"}])

        result = await _fetch_optional_positions(fetcher, object())

        self.assertEqual(result, [{"balance": "1"}])

    async def test_returns_empty_list_on_bad_gateway(self):
        fetcher = AsyncMock(side_effect=HTTPException(status_code=502))

        result = await _fetch_optional_positions(fetcher, object())

        self.assertEqual(result, [])

    async def test_returns_empty_list_on_timeout(self):
        async def slow_fetcher():
            await asyncio.sleep(0.05)
            return [{"balance": "1"}]

        with patch("routers.solana.OPTIONAL_POSITION_TIMEOUT", 0.001):
            result = await _fetch_optional_positions(slow_fetcher)

        self.assertEqual(result, [])

    async def test_reraises_non_bad_gateway_errors(self):
        fetcher = AsyncMock(side_effect=HTTPException(status_code=400))

        with self.assertRaises(HTTPException):
            await _fetch_optional_positions(fetcher, object())


class OptionalLookupTest(unittest.IsolatedAsyncioTestCase):
    async def test_returns_empty_dict_on_bad_gateway(self):
        fetcher = AsyncMock(side_effect=HTTPException(status_code=502))

        result = await _fetch_optional_lookup(fetcher, object())

        self.assertEqual(result, {})

    async def test_reraises_non_bad_gateway_errors(self):
        fetcher = AsyncMock(side_effect=HTTPException(status_code=400))

        with self.assertRaises(HTTPException):
            await _fetch_optional_lookup(fetcher, object())

    async def test_returns_empty_dict_on_timeout(self):
        async def slow_fetcher():
            await asyncio.sleep(0.05)
            return {"unexpected": True}

        with patch("routers.solana.OPTIONAL_LOOKUP_TIMEOUT", 0.001):
            result = await _fetch_optional_lookup(slow_fetcher)

        self.assertEqual(result, {})

    async def test_returns_empty_dict_on_httpx_errors(self):
        fetcher = AsyncMock(side_effect=httpx.ConnectError("network unavailable"))

        result = await _fetch_optional_lookup(fetcher, object())

        self.assertEqual(result, {})


class GmtradeCsvCacheTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        _gmtrade_csv_cache.clear()

    def tearDown(self):
        _gmtrade_csv_cache.clear()

    async def test_updates_cache_after_successful_refresh(self):
        content = "type,mint\nGM,mint-a\n"

        with patch(
            "routers.solana._build_gmtrade_csv_content", new_callable=AsyncMock
        ) as build:
            build.return_value = content
            response = await get_gmtrade_csv(" wallet-a ")

        build.assert_awaited_once_with("wallet-a")
        self.assertEqual(response.body.decode(), content)
        self.assertEqual(_gmtrade_csv_cache["wallet-a"], content)

    async def test_returns_cached_csv_when_refresh_fails(self):
        cached = "type,mint\nGM,cached-mint\n"
        _gmtrade_csv_cache["wallet-a"] = cached

        with patch(
            "routers.solana._build_gmtrade_csv_content", new_callable=AsyncMock
        ) as build:
            build.side_effect = HTTPException(status_code=502)
            response = await get_gmtrade_csv("wallet-a")

        self.assertEqual(response.body.decode(), cached)

    async def test_returns_empty_csv_when_refresh_fails_without_cache(self):
        with patch(
            "routers.solana._build_gmtrade_csv_content", new_callable=AsyncMock
        ) as build:
            build.side_effect = HTTPException(status_code=502)
            response = await get_gmtrade_csv("wallet-a")

        self.assertEqual(
            response.body.decode(),
            (
                "type,mint,name,balance,price_usd,value_usd,long_token_mint,"
                "short_token_mint,index_token_mint,updated_at\r\n"
            ),
        )


class KaminoCsvTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        _kamino_csv_cache.clear()

    def tearDown(self):
        _kamino_csv_cache.clear()

    def test_normalizes_kvault_metadata_name_to_resource_name(self):
        self.assertEqual(
            _normalize_kamino_vault_name("kVault PYUSD Sentora"),
            _normalize_kamino_vault_name("Sentora PYUSD"),
        )

    def test_builds_kvault_rows_with_metrics(self):
        rows = _build_kamino_rows(
            "11111111111111111111111111111111",
            [{"mint": "share-mint", "balance": Decimal("10")}],
            {
                "share-mint": {
                    "name": "kVault PYUSD Sentora",
                    "symbol": "kV-PYUSD",
                }
            },
            {
                "mainnet-beta": {
                    "vaults": {
                        "vault-address": {
                            "name": "Sentora PYUSD",
                            "tokenSymbol": "PYUSD",
                        }
                    }
                }
            },
            {
                "vault-address": {
                    "tokensPerShare": "1.0118",
                    "tokenPrice": "1",
                    "apy": "0.032",
                    "apy7d": "0.065",
                    "apy30d": "0.062",
                    "apy90d": "0.064",
                    "apyFarmRewards": "0.03",
                    "apyActual": "0.031",
                    "sharePrice": "1.0118",
                    "tokensAvailable": "100",
                    "tokensInvested": "147000000",
                }
            },
            "2026-06-21T00:00:00Z",
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["type"], "kVault")
        self.assertEqual(rows[0]["vault_address"], "vault-address")
        self.assertEqual(rows[0]["vault_name"], "Sentora PYUSD")
        self.assertEqual(rows[0]["share_symbol"], "kV-PYUSD")
        self.assertEqual(rows[0]["underlying_symbol"], "PYUSD")
        self.assertEqual(rows[0]["share_balance"], "10")
        self.assertEqual(rows[0]["underlying_amount"], "10.118")
        self.assertEqual(rows[0]["value_usd"], "10.118")
        self.assertEqual(rows[0]["apy"], "0.032")
        self.assertEqual(rows[0]["farm_rewards_apy"], "0.03")

    def test_derives_kamino_farm_user_state_address(self):
        self.assertEqual(
            _derive_kamino_farm_user_state_address(
                "8hznHD38esVyPps3hUcFahynwekYUfjn43PRz9n5PDZN",
                "4hgKXUgyETQVxEf1HXoDYHnoJXey37Y9Srkrp6kjwwDp",
            ),
            "9F8Jk9ujXkRFjdbr8nEsEHJmhndoX5nehFJWf3K2sf8s",
        )

    def test_parses_kamino_farm_staked_shares(self):
        data = bytearray(920)
        data[:8] = KAMINO_FARM_USER_STATE_DISCRIMINATOR
        active_stake_scaled = 27415311762906484421683123955
        data[408:424] = active_stake_scaled.to_bytes(16, "little")
        account_info = {
            "owner": KAMINO_FARMS_PROGRAM_ID,
            "data": [base64.b64encode(data).decode(), "base64"],
        }

        shares = _parse_kamino_farm_staked_shares(account_info, 6)

        self.assertEqual(shares, Decimal("27415.311762906484421683123955"))

    def test_parses_kamino_vault_state(self):
        data = bytearray(58728)
        data[:8] = KAMINO_VAULT_STATE_DISCRIMINATOR
        token_mint = bytes([1]) * 32
        shares_mint = bytes([2]) * 32
        vault_farm = bytes([3]) * 32
        first_loss_farm = bytes([4]) * 32
        data[80:112] = token_mint
        data[112:120] = (6).to_bytes(8, "little")
        data[184:216] = shares_mint
        data[216:224] = (6).to_bytes(8, "little")
        data[224:232] = (123).to_bytes(8, "little")
        data[232:240] = (456).to_bytes(8, "little")
        data[58528:58540] = b"Sentora PYUSD"
        data[58600:58632] = vault_farm
        data[58696:58728] = first_loss_farm
        account_info = {
            "owner": KAMINO_VAULT_PROGRAM_ID,
            "data": [base64.b64encode(data).decode(), "base64"],
        }

        state = _parse_kamino_vault_state("vault-address", account_info)

        self.assertIsNotNone(state)
        self.assertEqual(state["token_mint"], _base58_encode(token_mint))
        self.assertEqual(state["shares_mint"], _base58_encode(shares_mint))
        self.assertEqual(state["shares_mint_decimals"], 6)
        self.assertEqual(state["name"], "Sentora PYUSD")
        self.assertEqual(state["vault_farm"], _base58_encode(vault_farm))
        self.assertEqual(
            state["first_loss_capital_farm"], _base58_encode(first_loss_farm)
        )

    def test_builds_kamino_positions_from_staked_shares(self):
        positions = _build_kamino_vault_token_positions(
            [{"mint": "share-mint", "balance": Decimal("2.5")}],
            {
                "vault-address": {
                    "shares_mint": "share-mint",
                    "shares_mint_decimals": 6,
                }
            },
            {"vault-address": Decimal("7.5")},
        )

        self.assertEqual(positions, [
            {
                "mint": "share-mint",
                "balance": Decimal("10.0"),
                "unstaked_balance": Decimal("2.5"),
                "staked_balance": Decimal("7.5"),
                "vault_address": "vault-address",
            }
        ])

    def test_renders_empty_kamino_csv(self):
        self.assertEqual(
            _render_kamino_csv([]),
            (
                "type,wallet,vault_address,share_mint,vault_name,share_symbol,"
                "underlying_symbol,share_balance,underlying_amount,token_price_usd,"
                "value_usd,apy,apy_7d,apy_30d,apy_90d,farm_rewards_apy,"
                "actual_apy,share_price,tokens_per_share,tokens_available,"
                "tvl_tokens,updated_at,source_url\r\n"
            ),
        )

    async def test_kamino_endpoint_updates_cache_after_successful_refresh(self):
        content = "type,vault_name\nkVault,Sentora PYUSD\n"
        wallet = "11111111111111111111111111111111"

        with patch(
            "routers.solana._build_kamino_csv_content", new_callable=AsyncMock
        ) as build:
            build.return_value = content
            response = await get_kamino_csv(wallet)

        build.assert_awaited_once_with(wallet)
        self.assertEqual(response.body.decode(), content)
        self.assertEqual(_kamino_csv_cache[wallet], content)

    async def test_kamino_endpoint_returns_cached_csv_when_refresh_fails(self):
        wallet = "11111111111111111111111111111111"
        cached = "type,vault_name\nkVault,cached\n"
        _kamino_csv_cache[wallet] = cached

        with patch(
            "routers.solana._build_kamino_csv_content", new_callable=AsyncMock
        ) as build:
            build.side_effect = HTTPException(status_code=502)
            response = await get_kamino_csv(wallet)

        self.assertEqual(response.body.decode(), cached)


def _write_int(data: bytearray, offset: int, value: int, length: int, signed=False):
    data[offset : offset + length] = value.to_bytes(length, "little", signed=signed)


class GmtradePerpCsvTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        _gmtrade_perp_csv_cache.clear()

    def tearDown(self):
        _gmtrade_perp_csv_cache.clear()

    def test_decodes_open_perp_position_account(self):
        data = bytearray(296)
        owner = bytes([1]) * 32
        market_token = bytes([2]) * 32
        collateral_token = bytes([3]) * 32
        data[42] = 1
        data[56:88] = owner
        data[88:120] = market_token
        data[120:152] = collateral_token
        _write_int(data, 48, 1_700_000_000, 8, signed=True)
        _write_int(data, 152, 42, 8)
        _write_int(data, 160, 1_700_000_100, 8, signed=True)
        _write_int(data, 168, 123_456, 8)
        _write_int(data, 184, 2 * 10**8, 16)
        _write_int(data, 200, 20_000 * 10**6, 16)
        _write_int(data, 216, 100_000 * 10**GMTRADE_MARKET_DECIMALS, 16)

        decoded = _decode_gmtrade_perp_position("position-a", bytes(data))

        self.assertIsNotNone(decoded)
        self.assertEqual(decoded["position_address"], "position-a")
        self.assertEqual(decoded["side"], "long")
        self.assertEqual(decoded["owner"], _base58_encode(owner))
        self.assertEqual(decoded["market_token_mint"], _base58_encode(market_token))
        self.assertEqual(decoded["collateral_token_mint"], _base58_encode(collateral_token))
        self.assertEqual(decoded["raw_size_in_tokens"], 2 * 10**8)
        self.assertEqual(decoded["raw_size_usd"], 100_000 * 10**GMTRADE_MARKET_DECIMALS)

    def test_builds_perp_rows_with_estimated_values(self):
        market_token = _base58_encode(bytes([2]) * 32)
        collateral_token = _base58_encode(bytes([3]) * 32)
        index_token = _base58_encode(bytes([4]) * 32)
        positions = [
            {
                "position_address": "position-a",
                "side": "long",
                "owner": "wallet-a",
                "market_token_mint": market_token,
                "collateral_token_mint": collateral_token,
                "created_at": "1700000000",
                "increased_at": "1700000100",
                "decreased_at": "",
                "updated_at_slot": 123456,
                "trade_id": 42,
                "raw_size_in_tokens": 2 * 10**8,
                "raw_collateral_amount": 20_000 * 10**6,
                "raw_size_usd": 100_000 * 10**GMTRADE_MARKET_DECIMALS,
            }
        ]
        market_infos = {
            market_token: {
                "name": "BTC/USD",
                "indexTokenMint": index_token,
            }
        }
        token_decimals = {
            index_token: 8,
            collateral_token: 6,
        }
        tickers = {
            index_token: {
                "tokenSymbol": "BTC",
                "minPrice": str(50_000 * 10 ** (GMTRADE_PRICE_DECIMALS - 8)),
                "maxPrice": str(50_000 * 10 ** (GMTRADE_PRICE_DECIMALS - 8)),
            },
            collateral_token: {
                "tokenSymbol": "USDC",
                "minPrice": str(10 ** (GMTRADE_PRICE_DECIMALS - 6)),
                "maxPrice": str(10 ** (GMTRADE_PRICE_DECIMALS - 6)),
            },
        }

        rows = _build_gmtrade_perp_rows(
            positions, market_infos, token_decimals, tickers
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["market"], "BTC/USD")
        self.assertEqual(rows[0]["size_usd"], "100000")
        self.assertEqual(rows[0]["entry_price_usd"], "50000")
        self.assertEqual(rows[0]["mark_price_usd"], "50000")
        self.assertEqual(rows[0]["pnl_usd_estimated"], "0")
        self.assertEqual(rows[0]["collateral_usd"], "20000")
        self.assertEqual(rows[0]["leverage_estimated"], "5")

    def test_renders_empty_perp_csv(self):
        self.assertEqual(
            _render_gmtrade_perp_csv([]),
            (
                "position_address,market,side,size_usd,net_value_usd_estimated,"
                "collateral_usd,collateral_amount,collateral_symbol,entry_price_usd,"
                "mark_price_usd,pnl_usd_estimated,leverage_estimated,"
                "market_token_mint,index_token_mint,collateral_token_mint,owner,"
                "created_at,increased_at,decreased_at,updated_at_slot,trade_id,"
                "size_in_tokens,raw_size_usd,raw_collateral_amount\r\n"
            ),
        )

    async def test_perp_endpoint_updates_cache_after_successful_refresh(self):
        content = "position_address,market\nposition-a,BTC/USD\n"

        with patch(
            "routers.solana._build_gmtrade_perp_csv_content",
            new_callable=AsyncMock,
        ) as build:
            build.return_value = content
            response = await get_gmtrade_perps_csv(
                "11111111111111111111111111111111"
            )

        build.assert_awaited_once_with("11111111111111111111111111111111")
        self.assertEqual(response.body.decode(), content)
        self.assertEqual(
            _gmtrade_perp_csv_cache["11111111111111111111111111111111"],
            content,
        )

    async def test_perp_endpoint_returns_cached_csv_when_refresh_fails(self):
        cached = "position_address,market\nposition-a,BTC/USD\n"
        _gmtrade_perp_csv_cache["11111111111111111111111111111111"] = cached

        with patch(
            "routers.solana._build_gmtrade_perp_csv_content",
            new_callable=AsyncMock,
        ) as build:
            build.side_effect = HTTPException(status_code=502)
            response = await get_gmtrade_perps_csv(
                "11111111111111111111111111111111"
            )

        self.assertEqual(response.body.decode(), cached)

    async def test_rpc_request_sends_gmtrade_origin_header(self):
        seen_headers = {}

        def handler(request):
            seen_headers["origin"] = request.headers.get("origin")
            return httpx.Response(
                200,
                json={"jsonrpc": "2.0", "id": 1, "result": "ok"},
            )

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            result = await _rpc_request(client, "getHealth", [])

        self.assertEqual(result, "ok")
        self.assertEqual(seen_headers["origin"], "https://gmtrade.xyz")
