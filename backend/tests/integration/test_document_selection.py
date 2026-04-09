import unittest

from lib.collector_outcomes import select_final_report_document


class DocumentSelectionTests(unittest.TestCase):
    def test_selects_opening_meeting_financial_summary(self) -> None:
        docs = [
            {
                "Id": 101,
                "TypeTitle": "Atvēršanas sanāksmes finanšu piedāvājumu kopsavilkums",
                "Title": "Iepirkuma Nr. DKNP 2023/44 finanšu piedāvājumu apkopojums",
            }
        ]

        selected = select_final_report_document(docs)
        self.assertIsNotNone(selected)
        self.assertEqual(selected["Id"], 101)

    def test_prefers_final_report_over_summary(self) -> None:
        docs = [
            {
                "Id": 201,
                "TypeTitle": "Atvēršanas sanāksmes finanšu piedāvājumu kopsavilkums",
                "Title": "Finanšu piedāvājumu apkopojums",
            },
            {
                "Id": 202,
                "TypeCode": "PRCFINSMR",
                "Title": "Noslēguma ziņojums",
            },
        ]

        selected = select_final_report_document(docs)
        self.assertIsNotNone(selected)
        self.assertEqual(selected["Id"], 202)


if __name__ == "__main__":
    unittest.main()
