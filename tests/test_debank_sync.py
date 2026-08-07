import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from routers.history import enrich_history_prices_for_account
from tasks import fetch_and_save_data


def query_result(items):
    result = MagicMock()
    result.filter.return_value = result
    result.all.return_value = items
    return result


def async_client_context(client):
    context = MagicMock()
    context.__aenter__ = AsyncMock(return_value=client)
    context.__aexit__ = AsyncMock(return_value=False)
    return context


class HistoryPriceEnrichmentTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.account = SimpleNamespace(
            id="account-1",
            balance=1,
            addresses=[SimpleNamespace(address="0xabc")],
        )
        self.history = SimpleNamespace(
            json={"receives": [{"token_id": "token-1", "amount": 1}]},
            time_at=1_786_000_000,
            chain="eth",
            prices_synced=False,
        )

    async def test_failed_price_request_remains_pending_for_retry(self):
        db = MagicMock()
        db.query.side_effect = [
            query_result([self.history]),
            query_result([]),
            query_result([]),
        ]
        client = AsyncMock()
        client.get.return_value = SimpleNamespace(status_code=403, text="forbidden")

        with (
            patch(
                "routers.history.httpx.AsyncClient",
                return_value=async_client_context(client),
            ),
            patch("routers.history.asyncio.sleep", new_callable=AsyncMock),
        ):
            result = await enrich_history_prices_for_account(self.account, db)

        self.assertEqual(result["status"], "partial_error")
        self.assertEqual(result["transactions_synced"], 0)
        self.assertEqual(result["transactions_pending"], 1)
        self.assertFalse(self.history.prices_synced)

    async def test_successful_price_request_marks_transaction_synced(self):
        db = MagicMock()
        db.query.side_effect = [
            query_result([self.history]),
            query_result([]),
            query_result([]),
        ]
        response = SimpleNamespace(
            status_code=200,
            json=lambda: {"price": 42.5},
        )
        client = AsyncMock()
        client.get.return_value = response

        with (
            patch(
                "routers.history.httpx.AsyncClient",
                return_value=async_client_context(client),
            ),
            patch("routers.history.asyncio.sleep", new_callable=AsyncMock),
        ):
            result = await enrich_history_prices_for_account(self.account, db)

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["transactions_synced"], 1)
        self.assertEqual(result["transactions_pending"], 0)
        self.assertTrue(self.history.prices_synced)
        db.execute.assert_called_once()


class ScheduledDebankSyncTest(unittest.IsolatedAsyncioTestCase):
    async def test_scheduler_refreshes_portfolio_history_and_prices(self):
        account = SimpleNamespace(
            id="account-1",
            addresses=[SimpleNamespace(address="0xabc")],
        )
        db = MagicMock()
        db.query.return_value.filter.return_value.all.return_value = [account]
        client = AsyncMock()

        fetch_protocols = AsyncMock(
            return_value={"address": "0xabc", "status": "success"}
        )
        fetch_tokens = AsyncMock(
            return_value={"address": "0xabc", "status": "success"}
        )
        sync_history = AsyncMock(
            return_value={
                "results": [
                    {
                        "address": "0xabc",
                        "status": "success",
                        "synced_count": 2,
                        "error": None,
                    }
                ]
            }
        )
        enrich_prices = AsyncMock(
            return_value={
                "status": "ok",
                "transactions_synced": 2,
                "transactions_pending": 0,
            }
        )

        with (
            patch("tasks.SessionLocal", return_value=db),
            patch(
                "tasks.httpx.AsyncClient",
                return_value=async_client_context(client),
            ),
            patch("utils.fetch_debank_complex_protocols", fetch_protocols),
            patch("utils.fetch_debank_token_list", fetch_tokens),
            patch("routers.history.sync_history_for_account", sync_history),
            patch(
                "routers.history.enrich_history_prices_for_account",
                enrich_prices,
            ),
        ):
            await fetch_and_save_data()

        fetch_protocols.assert_awaited_once()
        fetch_tokens.assert_awaited_once()
        sync_history.assert_awaited_once_with(account, db)
        enrich_prices.assert_awaited_once_with(account, db)
        db.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
