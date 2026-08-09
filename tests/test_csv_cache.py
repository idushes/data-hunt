import unittest

from fastapi import FastAPI, Header
from fastapi.responses import PlainTextResponse, Response
from fastapi.testclient import TestClient

from csv_cache import CSVMemoryCacheMiddleware


def _build_app() -> tuple[FastAPI, dict[str, int]]:
    app = FastAPI()
    calls = {"csv": 0, "text": 0}
    app.add_middleware(CSVMemoryCacheMiddleware, ttl_seconds=60, max_entries=16)

    @app.get("/report.csv")
    async def report(
        value: str = "default",
        authorization: str | None = Header(None),
    ):
        calls["csv"] += 1
        return Response(
            content=f"value,authorization\n{value},{authorization or ''}\n",
            media_type="text/csv",
        )

    @app.get("/number")
    async def number():
        calls["text"] += 1
        return PlainTextResponse(str(calls["text"]))

    return app, calls


class CSVMemoryCacheMiddlewareTest(unittest.TestCase):
    def test_caches_successful_csv_response(self):
        app, calls = _build_app()

        with TestClient(app) as client:
            first = client.get("/report.csv?value=one")
            second = client.get("/report.csv?value=one")

        self.assertEqual(first.headers["x-csv-cache"], "MISS")
        self.assertEqual(second.headers["x-csv-cache"], "HIT")
        self.assertEqual(first.text, second.text)
        self.assertEqual(calls["csv"], 1)

    def test_auth_headers_have_separate_cache_entries(self):
        app, calls = _build_app()

        with TestClient(app) as client:
            first = client.get(
                "/report.csv", headers={"Authorization": "Bearer user-one"}
            )
            second = client.get(
                "/report.csv", headers={"Authorization": "Bearer user-two"}
            )
            repeated = client.get(
                "/report.csv", headers={"Authorization": "Bearer user-one"}
            )

        self.assertEqual(first.headers["x-csv-cache"], "MISS")
        self.assertEqual(second.headers["x-csv-cache"], "MISS")
        self.assertEqual(repeated.headers["x-csv-cache"], "HIT")
        self.assertNotEqual(first.text, second.text)
        self.assertEqual(calls["csv"], 2)

    def test_cache_buster_does_not_bypass_server_cache(self):
        app, calls = _build_app()

        with TestClient(app) as client:
            first = client.get("/report.csv?refresh=1")
            second = client.get("/report.csv?refresh=2")

        self.assertEqual(first.headers["x-csv-cache"], "MISS")
        self.assertEqual(second.headers["x-csv-cache"], "HIT")
        self.assertEqual(calls["csv"], 1)

    def test_does_not_cache_non_csv_response(self):
        app, calls = _build_app()

        with TestClient(app) as client:
            first = client.get("/number")
            second = client.get("/number")

        self.assertEqual(first.text, "1")
        self.assertEqual(second.text, "2")
        self.assertNotIn("x-csv-cache", second.headers)
        self.assertEqual(calls["text"], 2)


if __name__ == "__main__":
    unittest.main()
