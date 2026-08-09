import unittest
from unittest.mock import patch

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import PlainTextResponse, Response
from fastapi.testclient import TestClient

from routers.value import VALUE_SOURCES, ValueSource, router


def _build_app() -> FastAPI:
    app = FastAPI()

    @app.get("/test.csv")
    async def test_csv(
        order: str = "normal",
        value: str = "20",
        duplicate: bool = False,
    ):
        rows = ["one,10", f"two,{value}"]
        if order == "reversed":
            rows.reverse()
        if duplicate:
            rows.append("two,30")
        return Response(
            content="id,amount\n" + "\n".join(rows) + "\n",
            media_type="text/csv",
        )

    @app.get("/plain")
    async def plain():
        return PlainTextResponse("42")

    @app.get("/failure")
    async def failure():
        raise HTTPException(status_code=418, detail="source failed")

    app.include_router(router)
    return app


class StableValueRouteTest(unittest.TestCase):
    def setUp(self):
        self.app = _build_app()
        self.sources = patch.dict(
            VALUE_SOURCES,
            {
                "test": ValueSource("/test.csv", "id"),
                "plain": ValueSource("/plain", "id"),
                "failure": ValueSource("/failure", "id"),
            },
        )
        self.sources.start()

    def tearDown(self):
        self.sources.stop()

    def test_returns_same_value_when_row_order_changes(self):
        with TestClient(self.app) as client:
            normal = client.get(
                "/value?source=test&key=two&column=amount&order=normal"
            )
            reversed_rows = client.get(
                "/value?source=test&key=two&column=amount&order=reversed"
            )

        self.assertEqual(normal.status_code, 200)
        self.assertEqual(reversed_rows.status_code, 200)
        self.assertEqual(normal.text.strip(), "20")
        self.assertEqual(reversed_rows.text.strip(), "20")
        self.assertTrue(normal.headers["content-type"].startswith("text/csv"))

    def test_forwards_source_query_parameters(self):
        with TestClient(self.app) as client:
            response = client.get(
                "/value?source=test&key=two&column=amount&value=123.45"
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.text.strip(), "123.45")

    def test_rejects_unknown_column(self):
        with TestClient(self.app) as client:
            response = client.get(
                "/value?source=test&key=two&column=missing"
            )

        self.assertEqual(response.status_code, 400)
        self.assertIn("was not found", response.json()["detail"])

    def test_reports_missing_key_without_falling_back_to_row_number(self):
        with TestClient(self.app) as client:
            response = client.get(
                "/value?source=test&key=missing&column=amount"
            )

        self.assertEqual(response.status_code, 404)
        self.assertIn("was not found", response.json()["detail"])

    def test_rejects_duplicate_stable_key(self):
        with TestClient(self.app) as client:
            response = client.get(
                "/value?source=test&key=two&column=amount&duplicate=true"
            )

        self.assertEqual(response.status_code, 409)
        self.assertIn("is not unique", response.json()["detail"])

    def test_tells_client_to_use_already_scalar_source_directly(self):
        with TestClient(self.app) as client:
            response = client.get(
                "/value?source=plain&key=one&column=amount"
            )

        self.assertEqual(response.status_code, 400)
        self.assertIn("Use that source URL directly", response.json()["detail"])

    def test_preserves_source_http_error(self):
        with TestClient(self.app) as client:
            response = client.get(
                "/value?source=failure&key=one&column=amount"
            )

        self.assertEqual(response.status_code, 418)
        self.assertEqual(response.json()["detail"], "source failed")


if __name__ == "__main__":
    unittest.main()
