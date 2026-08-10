import csv
import io
import unittest
from decimal import Decimal
from unittest.mock import AsyncMock, patch

from routers.jupiter import (
    _fetch_jlp_rows,
    _sum_token_accounts,
    get_jupiter_jlp_csv,
)


WALLET = "11111111111111111111111111111111"


class JupiterJlpTest(unittest.IsolatedAsyncioTestCase):
    def test_sums_jlp_token_accounts(self):
        result = {
            "value": [
                {
                    "account": {
                        "data": {
                            "parsed": {
                                "info": {"tokenAmount": {"amount": "1200000"}}
                            }
                        }
                    }
                },
                {
                    "account": {
                        "data": {
                            "parsed": {
                                "info": {"tokenAmount": {"amount": "300000"}}
                            }
                        }
                    }
                },
            ]
        }
        self.assertEqual(_sum_token_accounts(result), 1_500_000)

    async def test_combines_balance_with_jlp_info(self):
        with (
            patch(
                "routers.jupiter._fetch_jlp_balance",
                AsyncMock(return_value=Decimal("10")),
            ),
            patch(
                "routers.jupiter._fetch_jlp_info",
                AsyncMock(
                    return_value={
                        "jlpPriceUsdFormatted": "3.5",
                        "jlpAprPct": "6",
                        "jlpApyPct": "6.2",
                        "aumUsdFormatted": "1000",
                        "aumLimitUsdFormatted": "2000",
                        "jlpTotalSupplyFormatted": "300",
                        "jlpAprLastUpdatedTimestamp": "123",
                        "jlpRealizedFeeUsd": "2500000",
                    }
                ),
            ),
        ):
            rows = await _fetch_jlp_rows(object(), WALLET)

        self.assertEqual(rows[0]["value_usd"], "35")
        self.assertEqual(rows[0]["realized_fee_usd"], "2.5")

    async def test_route_returns_csv(self):
        row = {
            "wallet": WALLET,
            "chain_id": "solana-mainnet",
            "chain": "Solana",
            "protocol": "Jupiter Perps",
            "position_id": "jlp",
            "token_symbol": "JLP",
            "balance": "10",
        }
        with patch(
            "routers.jupiter._fetch_jlp_rows", AsyncMock(return_value=[row])
        ):
            response = await get_jupiter_jlp_csv(WALLET)

        parsed = list(csv.DictReader(io.StringIO(response.body.decode())))
        self.assertEqual(parsed[0]["balance"], "10")
        self.assertEqual(response.headers["cache-control"], "public, max-age=60")


if __name__ == "__main__":
    unittest.main()
