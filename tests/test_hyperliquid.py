import unittest
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException

from routers.hyperliquid import (
    _build_account_row,
    _sum_balance_field,
    get_hyperliquid_balance,
)


class HyperliquidBalanceRowsTest(unittest.TestCase):
    def test_total_equity_adds_perp_equity_without_double_counting_held_usdc(self):
        row = _build_account_row(
            "0x1111111111111111111111111111111111111111",
            "main",
            "0x1111111111111111111111111111111111111111",
            "",
            {
                "marginSummary": {"accountValue": "141.77615"},
                "withdrawable": "1.242624",
                "time": 123,
            },
            {
                "balances": [
                    {
                        "coin": "USDC",
                        "total": "1137.3254213",
                        "hold": "141.796684",
                    }
                ]
            },
        )

        self.assertEqual(row["spot_usdc"], "1137.3254213")
        self.assertEqual(row["total_equity"], "1137.3048873")

    def test_sum_balance_field_sums_decimal_strings(self):
        self.assertEqual(
            _sum_balance_field(
                [
                    {"total_equity": "1137.3048873"},
                    {"total_equity": "1005.838626"},
                    {"total_equity": "472.802963"},
                ],
                "total_equity",
            ),
            "2615.9464763",
        )


class HyperliquidBalanceRouteTest(unittest.IsolatedAsyncioTestCase):
    async def test_aggregate_returns_sum_for_selected_field(self):
        rows = [
            {
                "account": "0x1111111111111111111111111111111111111111",
                "total_equity": "1137.3048873",
            },
            {
                "account": "0x2222222222222222222222222222222222222222",
                "total_equity": "1005.838626",
            },
        ]

        with patch(
            "routers.hyperliquid._build_accounts_rows", AsyncMock(return_value=rows)
        ):
            response = await get_hyperliquid_balance(
                "0x1111111111111111111111111111111111111111",
                account=None,
                field="total_equity",
                aggregate=True,
            )

        self.assertEqual(response.body.decode(), "2143.1435133")

    async def test_rejects_account_and_aggregate_together(self):
        with self.assertRaises(HTTPException) as context:
            await get_hyperliquid_balance(
                "0x1111111111111111111111111111111111111111",
                account="0x2222222222222222222222222222222222222222",
                aggregate=True,
            )

        self.assertEqual(context.exception.status_code, 400)


if __name__ == "__main__":
    unittest.main()
