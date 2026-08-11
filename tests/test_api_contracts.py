import unittest
from pathlib import Path

from routers.aave import AAVE_CSV_HEADER
from routers.bybit import BYBIT_CSV_HEADER
from routers.coinbase import COINBASE_CSV_HEADER
from routers.compound import COMPOUND_CSV_HEADER
from routers.euler import EULER_CSV_HEADER
from routers.fluid import FLUID_CSV_HEADER
from routers.gmx import GMX_CSV_HEADER
from routers.jupiter import JUPITER_JLP_CSV_HEADER
from routers.lido import LIDO_CSV_HEADER
from routers.morpho import MORPHO_CSV_HEADER
from routers.pancakeswap import PANCAKESWAP_CSV_HEADER
from routers.pendle import PENDLE_CSV_HEADER
from routers.polymarket import POLYMARKET_CSV_HEADER
from routers.solana import (
    GMTRADE_CSV_HEADER,
    GMTRADE_PERP_CSV_HEADER,
    KAMINO_CSV_HEADER,
    KAMINO_PORTFOLIO_CSV_HEADER,
)
from routers.stablecoins import STABLECOIN_CSV_HEADER
from routers.stakedao import STAKEDAO_CSV_HEADER
from routers.uniswap import UNISWAP_CSV_HEADER
from routers.uniswap_v4 import UNISWAP_V4_CSV_HEADER
from routers.value import VALUE_SOURCES, ValueSource
from server import app


PUBLISHED_VALUE_SOURCES = {
    "paradex": ValueSource("/paradex/balance", "account"),
    "lighter": ValueSource("/lighter/balance", "account_index"),
    "hyperliquid": ValueSource("/hyperliquid/balance", "account"),
    "coinbase": ValueSource("/coinbase/balance", "id"),
    "bybit": ValueSource("/bybit/account.csv", "id"),
    "gmtrade-assets": ValueSource("/solana/gmtrade.csv", "mint"),
    "gmtrade-perps": ValueSource(
        "/solana/gmtrade-perps.csv", "position_address"
    ),
    "kamino-vaults": ValueSource("/solana/kamino.csv", "vault_address"),
    "kamino-positions": ValueSource(
        "/solana/kamino-positions.csv", "vault_address"
    ),
    "fluid": ValueSource("/fluid/positions.csv", "position_id"),
    "aave": ValueSource("/aave/positions.csv", "position_id"),
    "uniswap": ValueSource("/uniswap/positions.csv", "position_id"),
    "uniswap-v4": ValueSource("/uniswap/v4/positions.csv", "position_id"),
    "pancakeswap": ValueSource("/pancakeswap/positions.csv", "position_id"),
    "stablecoins": ValueSource("/stablecoins/balances.csv", "balance_id"),
    "stakedao": ValueSource("/stakedao/positions.csv", "position_id"),
    "morpho": ValueSource("/morpho/positions.csv", "position_id"),
    "compound": ValueSource("/compound/positions.csv", "position_id"),
    "euler": ValueSource("/euler/positions.csv", "position_id"),
    "lido": ValueSource("/lido/positions.csv", "position_id"),
    "jupiter-jlp": ValueSource("/jupiter/jlp.csv", "position_id"),
    "gmx": ValueSource("/gmx/positions.csv", "position_id"),
    "polymarket": ValueSource("/polymarket/positions.csv", "position_id"),
    "pendle": ValueSource("/pendle/positions.csv", "position_id"),
}

PUBLISHED_CSV_HEADERS = {
    "paradex": [
        "account",
        "account_value",
        "total_collateral",
        "free_collateral",
        "status",
        "settlement_asset",
        "updated_at",
    ],
    "lighter": [
        "account_index",
        "account_type",
        "l1_address",
        "total_asset_value",
        "cross_asset_value",
        "collateral",
        "available_balance",
        "status",
        "name",
    ],
    "hyperliquid": [
        "account",
        "account_type",
        "master",
        "name",
        "account_value",
        "withdrawable",
        "spot_usdc",
        "total_equity",
        "spot_balances",
        "time",
    ],
    "coinbase": COINBASE_CSV_HEADER,
    "bybit": BYBIT_CSV_HEADER,
    "gmtrade-assets": GMTRADE_CSV_HEADER,
    "gmtrade-perps": GMTRADE_PERP_CSV_HEADER,
    "kamino-vaults": KAMINO_CSV_HEADER,
    "kamino-positions": KAMINO_PORTFOLIO_CSV_HEADER,
    "fluid": FLUID_CSV_HEADER,
    "aave": AAVE_CSV_HEADER,
    "uniswap": UNISWAP_CSV_HEADER,
    "uniswap-v4": UNISWAP_V4_CSV_HEADER,
    "pancakeswap": PANCAKESWAP_CSV_HEADER,
    "stablecoins": STABLECOIN_CSV_HEADER,
    "stakedao": STAKEDAO_CSV_HEADER,
    "morpho": MORPHO_CSV_HEADER,
    "compound": COMPOUND_CSV_HEADER,
    "euler": EULER_CSV_HEADER,
    "lido": LIDO_CSV_HEADER,
    "jupiter-jlp": JUPITER_JLP_CSV_HEADER,
    "gmx": GMX_CSV_HEADER,
    "polymarket": POLYMARKET_CSV_HEADER,
    "pendle": PENDLE_CSV_HEADER,
}


class PublishedApiContractTest(unittest.TestCase):
    def test_docker_image_includes_credential_capsule_modules(self):
        dockerfile = Path("Dockerfile").read_text()

        self.assertIn("coinbase_capsule.py", dockerfile)
        self.assertIn("bybit_capsule.py", dockerfile)

    def test_value_source_aliases_and_paths_do_not_change_silently(self):
        self.assertEqual(VALUE_SOURCES, PUBLISHED_VALUE_SOURCES)

    def test_every_value_source_path_is_a_registered_get_route(self):
        get_paths = {
            route.path
            for route in app.routes
            if "GET" in (getattr(route, "methods", None) or set())
        }

        for source, config in PUBLISHED_VALUE_SOURCES.items():
            with self.subTest(source=source):
                self.assertIn(config.path, get_paths)

    def test_every_stable_key_is_present_once_in_the_csv_header(self):
        self.assertEqual(
            set(PUBLISHED_CSV_HEADERS), set(PUBLISHED_VALUE_SOURCES)
        )

        for source, config in PUBLISHED_VALUE_SOURCES.items():
            header = PUBLISHED_CSV_HEADERS[source]
            with self.subTest(source=source):
                self.assertEqual(
                    header.count(config.key_column),
                    1,
                    f"{source} must expose stable key {config.key_column}",
                )
                self.assertEqual(
                    len(header),
                    len(set(header)),
                    f"{source} CSV header contains duplicate columns",
                )

    def test_value_route_remains_public_and_registered(self):
        get_paths = {
            route.path
            for route in app.routes
            if "GET" in (getattr(route, "methods", None) or set())
        }
        self.assertIn("/value", get_paths)
        self.assertIn("/v/{resource_id}", get_paths)
        self.assertIn("/value-resources", app.openapi()["paths"])
        self.assertIn("post", app.openapi()["paths"]["/value-resources"])

    def test_coinbase_uses_capsules_instead_of_raw_credentials(self):
        schema = app.openapi()
        balance_parameters = schema["paths"]["/coinbase/balance"]["get"][
            "parameters"
        ]
        parameter_names = {parameter["name"] for parameter in balance_parameters}

        self.assertIn("capsule", parameter_names)
        self.assertNotIn("token", parameter_names)
        self.assertNotIn("key_name", parameter_names)
        self.assertNotIn("key_secret", parameter_names)
        self.assertIn("post", schema["paths"]["/coinbase/capsule"])

    def test_bybit_uses_capsules_instead_of_raw_credentials(self):
        schema = app.openapi()
        parameters = schema["paths"]["/bybit/account.csv"]["get"]["parameters"]
        parameter_names = {parameter["name"] for parameter in parameters}

        self.assertIn("capsule", parameter_names)
        self.assertNotIn("api_key", parameter_names)
        self.assertNotIn("api_secret", parameter_names)
        self.assertIn("post", schema["paths"]["/bybit/capsule"])


if __name__ == "__main__":
    unittest.main()
