import unittest

from routers.value import VALUE_SOURCES
from server import app


class RemovedLegacyRoutesTest(unittest.TestCase):
    def test_debank_and_history_routes_are_not_registered(self):
        registered_paths = {route.path for route in app.routes}
        removed_paths = {
            "/debank/all_complex_protocol_list",
            "/debank/all_token_list",
            "/debank/all_history",
            "/debank/history/readable",
            "/debank/enrich_prices",
            "/debt",
            "/stability",
            "/pool",
            "/wallet",
        }

        self.assertTrue(removed_paths.isdisjoint(registered_paths))

    def test_debank_backed_value_sources_are_not_registered(self):
        self.assertTrue(
            {"debt", "stability", "pool", "wallet"}.isdisjoint(VALUE_SOURCES)
        )


if __name__ == "__main__":
    unittest.main()
