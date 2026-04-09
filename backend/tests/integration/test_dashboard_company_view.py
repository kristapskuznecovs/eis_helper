import json
import unittest
from pathlib import Path

from dashboard.server import AnalyticsService, Filters


class FakeRepository:
    def __init__(self, rows, filters):
        self._rows = rows
        self._filters = filters

    def fetch_rows(self, filters):  # noqa: ARG002
        return list(self._rows)

    def available_filters(self):
        return dict(self._filters)


def make_row(procurement_id, *, buyer="Buyer A", region="Rīga", category="design", winner=None, winner_normalized=None, award=None, participants=None):
    return {
        "procurement_id": str(procurement_id),
        "procurement_url": f"https://example.test/{procurement_id}",
        "buyer_normalized": buyer,
        "purchaser_name": buyer,
        "planning_region": region,
        "category": category,
        "cpv_main": "45210000-2",
        "estimated_value_eur": 100.0,
        "procurement_winner": winner,
        "winner_normalized": winner_normalized,
        "procurement_winner_suggested_price_eur": award,
        "procurement_participants_json": json.dumps(participants or []),
    }


class DashboardCompanyViewTests(unittest.TestCase):
    def test_build_company_view_supports_multiple_selected_companies(self) -> None:
        rows = [
            make_row(
                1,
                winner="Alpha Build",
                winner_normalized="ALPHA BUILD",
                award=100.0,
                participants=[
                    {"name": "Alpha Build", "suggested_price_eur": 100.0},
                    {"name": "Gamma", "suggested_price_eur": 105.0},
                ],
            ),
            make_row(
                2,
                winner="Gamma",
                winner_normalized="GAMMA",
                award=95.0,
                participants=[
                    {"name": "Beta Construct", "suggested_price_eur": 98.0},
                    {"name": "Gamma", "suggested_price_eur": 95.0},
                ],
            ),
            make_row(
                3,
                winner="Beta Construct",
                winner_normalized="BETA CONSTRUCT",
                award=90.0,
                participants=[
                    {"name": "Alpha Build", "suggested_price_eur": 94.0},
                    {"name": "Beta Construct", "suggested_price_eur": 90.0},
                    {"name": "Gamma", "suggested_price_eur": 97.0},
                ],
            ),
        ]
        filters = {
            "years": [2025],
            "buyers": ["Buyer A"],
            "planning_regions": ["Rīga"],
            "categories": ["design"],
        }
        service = AnalyticsService(FakeRepository(rows, filters))

        result = service.build_company_view(Filters(), ["Alpha Build", "Beta Construct"])

        self.assertEqual(result["selected_companies"], ["Alpha Build", "Beta Construct"])
        self.assertIn("Alpha Build", result["selected_company"])
        self.assertIn("Beta Construct", result["selected_company"])
        self.assertEqual(result["summary"]["applications"], 3)
        self.assertEqual(result["summary"]["wins"], 2)
        self.assertEqual(result["summary"]["losses"], 1)
        self.assertEqual(result["summary"]["won_value_eur"], 190.0)
        self.assertEqual(result["buyers"]["best"][0]["bids"], 3)
        self.assertEqual(result["buyers"]["best"][0]["wins"], 2)


class CompanyViewStaticSmokeTests(unittest.TestCase):
    def test_gc_html_contains_multi_company_controls(self) -> None:
        html = Path("dashboard/static/gc.html").read_text(encoding="utf-8")
        self.assertIn('id="company-search"', html)
        self.assertIn('id="company-selected"', html)
        self.assertIn('id="company-options"', html)

    def test_gc_js_uses_multi_company_query(self) -> None:
        script = Path("dashboard/static/gc.js").read_text(encoding="utf-8")
        self.assertIn("state.companies", script)
        self.assertIn("query.append(key, item)", script)
        self.assertIn("selected_companies", script)


if __name__ == "__main__":
    unittest.main()
