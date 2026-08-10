import csv
import io
import unittest
from unittest.mock import AsyncMock, patch

from routers.lido import _amount, _fetch_lido_rows, get_lido_positions_csv


WALLET = "0x6272ab4f91e0df14acb6a2a311d817381210e339"


class LidoHelpersTest(unittest.TestCase):
    def test_formats_wei_amount(self):
        self.assertEqual(_amount(1_500_000_000_000_000_000), "1.5")


class LidoFetchTest(unittest.IsolatedAsyncioTestCase):
    async def test_parses_tokens_and_claimable_withdrawal(self):
        with (
            patch(
                "routers.lido._rpc_batch",
                AsyncMock(
                    side_effect=[
                        [(2 * 10**18,), (10**18,), ([7],)],
                        [
                            (1_200_000_000_000_000_000,),
                            (
                                [
                                    (
                                        3 * 10**18,
                                        0,
                                        WALLET,
                                        123,
                                        True,
                                        False,
                                    )
                                ],
                            ),
                        ],
                    ]
                ),
            ),
            patch("routers.lido._fetch_apr", AsyncMock(return_value="2.18")),
        ):
            rows = await _fetch_lido_rows(object(), WALLET)

        self.assertEqual(len(rows), 3)
        wrapped = next(row for row in rows if row["position_id"] == "wsteth")
        self.assertEqual(wrapped["steth_equivalent"], "1.2")
        withdrawal = next(
            row for row in rows if row["position_id"] == "withdrawal:7"
        )
        self.assertEqual(withdrawal["is_claimable"], "true")

    async def test_route_returns_csv(self):
        row = {
            "wallet": WALLET,
            "chain_id": "1",
            "chain": "Ethereum",
            "protocol": "Lido",
            "position_id": "steth",
            "position_type": "token",
            "token_symbol": "stETH",
            "amount": "2",
        }
        with patch(
            "routers.lido._fetch_lido_rows", AsyncMock(return_value=[row])
        ):
            response = await get_lido_positions_csv(WALLET)

        parsed = list(csv.DictReader(io.StringIO(response.body.decode())))
        self.assertEqual(parsed[0]["amount"], "2")
        self.assertEqual(response.headers["cache-control"], "public, max-age=60")


if __name__ == "__main__":
    unittest.main()
