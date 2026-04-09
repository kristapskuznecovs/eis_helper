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


def make_row(
    procurement_id,
    *,
    winner=None,
    winner_normalized=None,
    buyer="Buyer A",
    region="Rīga",
    category="design",
    award=None,
    estimate=None,
    participants_count=None,
    participants=None,
    evaluation_method=None,
):
    return {
        "procurement_id": str(procurement_id),
        "procurement_url": f"https://example.test/{procurement_id}",
        "procurement_winner": winner,
        "winner_normalized": winner_normalized,
        "purchaser_name": buyer,
        "planning_region": region,
        "category": category,
        "procurement_winner_suggested_price_eur": award,
        "estimated_value_eur": estimate,
        "procurement_participants_count": participants_count,
        "procurement_participants_json": json.dumps(participants) if participants is not None else None,
        "evaluation_method": evaluation_method,
    }


class DashboardRiskViewTests(unittest.TestCase):
    def test_build_risk_view_returns_filters_and_risk_aggregations(self) -> None:
        rows = [
            make_row(
                1,
                winner="Acme",
                winner_normalized="ACME",
                award=100.0,
                participants_count=4,
                participants=[
                    {"name": "SIA Acme", "suggested_price_eur": 100.0},
                    {"name": "Beta", "suggested_price_eur": 102.0},
                ],
                evaluation_method="lowest_price",
            ),
            make_row(
                2,
                winner="SIA Acme",
                winner_normalized="ACME",
                award=110.0,
                participants_count=4,
                participants=[
                    {"name": "Acme", "suggested_price_eur": 110.0},
                    {"name": "Beta", "suggested_price_eur": 111.0},
                ],
                evaluation_method="lowest_price",
            ),
            make_row(
                3,
                winner='SIA "ACME"',
                winner_normalized="ACME",
                award=120.0,
                participants_count=4,
                participants=[
                    {"name": 'SIA "ACME"', "suggested_price_eur": 120.0},
                    {"name": "Beta", "suggested_price_eur": 122.0},
                ],
                evaluation_method="lowest_price",
            ),
            make_row(
                4,
                winner="Acme",
                winner_normalized="ACME",
                award=120.0,
                estimate=100.0,
                participants_count=1,
            ),
            make_row(
                5,
                winner="Acme",
                winner_normalized="ACME",
                award=75.0,
                estimate=100.0,
                participants_count=3,
            ),
            make_row(
                6,
                winner="SafeCo",
                winner_normalized="SAFECO",
                buyer="Buyer Safe",
                region="Kurzeme",
                category="maintenance",
                award=55.0,
            ),
            make_row(
                7,
                winner="SafeCo",
                winner_normalized="SAFECO",
                buyer="Buyer Safe",
                region="Kurzeme",
                category="maintenance",
                award=60.0,
            ),
            make_row(
                8,
                winner="SafeCo",
                winner_normalized="SAFECO",
                buyer="Buyer Safe",
                region="Kurzeme",
                category="maintenance",
                award=65.0,
            ),
        ]
        filters = {
            "years": [2025],
            "buyers": ["Buyer A", "Buyer Safe"],
            "planning_regions": ["Kurzeme", "Rīga"],
            "categories": ["design", "maintenance"],
        }
        service = AnalyticsService(FakeRepository(rows, filters))

        result = service.build_risk_view(Filters())

        self.assertEqual(result["filters"], filters)
        self.assertEqual(result["summary"]["projects"], 8)
        self.assertEqual(result["summary"]["single_bidder_count"], 1)
        self.assertEqual(result["summary"]["low_competition_count"], 1)
        self.assertEqual(result["summary"]["with_estimate_count"], 2)
        self.assertEqual(result["summary"]["award_above_estimate_count"], 1)
        self.assertEqual(result["summary"]["award_above_estimate_10pct_count"], 1)
        self.assertEqual(result["summary"]["award_below_estimate_20pct_count"], 1)

        winners = {item["name"]: item for item in result["winners"]}
        self.assertIn("Acme", winners)
        self.assertIn("SafeCo", winners)
        self.assertEqual(winners["Acme"]["project_count"], 5)
        self.assertEqual(winners["Acme"]["risky_project_count"], 4)
        self.assertEqual(winners["Acme"]["single_bidder_wins"], 1)
        self.assertEqual(winners["Acme"]["above_estimate_wins"], 1)
        self.assertEqual(winners["SafeCo"]["risky_project_count"], 0)

        buyer_hotspots = result["buyers"]["risk_hotspots"]
        self.assertEqual(len(buyer_hotspots), 1)
        self.assertEqual(buyer_hotspots[0]["name"], "Buyer A")
        self.assertEqual(buyer_hotspots[0]["project_count"], 5)
        self.assertEqual(buyer_hotspots[0]["risky_project_count"], 4)

        self.assertEqual(result["buyers"]["single_bidder"][0]["name"], "Buyer A")
        self.assertEqual(result["buyers"]["concentration"][0]["top_winner"], "ACME")

        self.assertEqual(result["hotspots"]["regions"][0]["name"], "Rīga")
        self.assertEqual(result["hotspots"]["regions"][0]["risky_project_count"], 4)
        self.assertEqual(result["hotspots"]["categories"][0]["name"], "design")
        self.assertEqual(result["hotspots"]["categories"][0]["risky_project_count"], 4)

        self.assertEqual(len(result["pairs"]), 1)
        self.assertEqual(result["pairs"][0]["close_bid_count"], 3)
        self.assertEqual(result["pairs"][0]["meet_count"], 3)
        self.assertEqual(result["pairs"][0]["lowest_price_close_count"], 3)


class RiskViewStaticSmokeTests(unittest.TestCase):
    def test_risk_html_contains_new_sections(self) -> None:
        html = Path("dashboard/static/risk.html").read_text(encoding="utf-8")
        self.assertIn('id="risk-winners"', html)
        self.assertIn('id="risk-buyers-hotspots"', html)
        self.assertIn('id="risk-hotspots-regions"', html)
        self.assertIn('id="risk-hotspots-categories"', html)

    def test_risk_js_references_expanded_api_shape(self) -> None:
        script = Path("dashboard/static/risk.js").read_text(encoding="utf-8")
        self.assertIn("data.winners", script)
        self.assertIn("data.buyers.risk_hotspots", script)
        self.assertIn("data.hotspots.regions", script)
        self.assertIn("data.filters ||", script)


if __name__ == "__main__":
    unittest.main()
