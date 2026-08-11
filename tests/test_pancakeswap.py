import unittest
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException

from routers.pancakeswap import (
    PANCAKESWAP_CHAINS,
    PANCAKESWAP_POOL_ABI,
    PANCAKESWAP_V3_FACTORY,
    PANCAKESWAP_V3_POSITION_MANAGER,
    _fetch_pancakeswap_rows,
    get_pancakeswap_positions_csv,
)


WALLET = "0x6272ab4f91e0df14acb6a2a311d817381210e339"


class PancakeSwapValidationTest(unittest.IsolatedAsyncioTestCase):
    def test_uses_official_v3_deployments(self):
        self.assertEqual(set(PANCAKESWAP_CHAINS), {1, 56})
        for chain in PANCAKESWAP_CHAINS.values():
            self.assertEqual(
                chain["position_manager"], PANCAKESWAP_V3_POSITION_MANAGER
            )
            self.assertEqual(chain["factory"], PANCAKESWAP_V3_FACTORY)
            self.assertIs(chain["pool_abi"], PANCAKESWAP_POOL_ABI)

        slot0_outputs = PANCAKESWAP_POOL_ABI[0]["outputs"]
        self.assertEqual(slot0_outputs[5]["type"], "uint32")

    async def test_delegates_to_shared_v3_reader(self):
        rows = [{"position_id": "56:manager:1"}]
        with patch(
            "routers.pancakeswap._fetch_v3_rows",
            AsyncMock(return_value=rows),
        ) as fetch:
            result = await _fetch_pancakeswap_rows(WALLET, 56, False)

        fetch.assert_awaited_once_with(
            WALLET,
            56,
            False,
            PANCAKESWAP_CHAINS,
            "PancakeSwap V3",
        )
        self.assertEqual(result, rows)

    async def test_rejects_unsupported_chain(self):
        with self.assertRaises(HTTPException) as context:
            await _fetch_pancakeswap_rows(WALLET, 999999, False)

        self.assertEqual(context.exception.status_code, 400)
        self.assertIn("Supported: 1, 56", context.exception.detail)

    async def test_route_returns_csv(self):
        rows = [
            {
                "wallet": WALLET,
                "chain_id": "56",
                "protocol": "PancakeSwap V3",
                "position_id": "56:manager:123",
                "token_id": "123",
                "liquidity": "42",
            }
        ]
        with patch(
            "routers.pancakeswap._fetch_pancakeswap_rows",
            AsyncMock(return_value=rows),
        ) as fetch:
            response = await get_pancakeswap_positions_csv(WALLET, 56, False)

        fetch.assert_awaited_once_with(WALLET, 56, False)
        self.assertEqual(response.media_type, "text/csv")
        self.assertEqual(response.headers["cache-control"], "public, max-age=60")
        self.assertIn("PancakeSwap V3", response.body.decode())


if __name__ == "__main__":
    unittest.main()
