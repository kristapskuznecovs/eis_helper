import os
import tempfile
import unittest
from pathlib import Path

import utils


class UtilsTests(unittest.TestCase):
    def test_slugify_and_normalize(self) -> None:
        self.assertEqual(utils.slugify("  Hello, World!  "), "hello_world")
        self.assertEqual(utils.normalize_text("Ēka Šķūnis"), "eka skunis")

    def test_render_prompt_template(self) -> None:
        template = "Hello {{NAME}}, id={{ID}}"
        rendered = utils.render_prompt_template(template, {"NAME": "Alice", "ID": "42"})
        self.assertEqual(rendered, "Hello Alice, id=42")

    def test_extract_js_array_and_captcha_and_csrf(self) -> None:
        html_text = """
        <script>
          var ActualDocuments_items = [{"Id":1},{"Id":2}];
        </script>
        """
        parsed = utils.extract_js_array(html_text, "ActualDocuments_items")
        self.assertEqual([row["Id"] for row in parsed], [1, 2])
        self.assertTrue(utils.is_captcha_page("Pārbaude pret robotiem"))
        self.assertFalse(utils.is_captcha_page("normal page"))
        csrf = utils.parse_csrf_token(
            '<input name="__RequestVerificationToken" type="hidden" value="token123">'
        )
        self.assertEqual(csrf, "token123")
        self.assertIsNone(utils.parse_csrf_token("<html></html>"))

    def test_load_dotenv_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            env_path = Path(tmpdir) / ".env"
            env_path.write_text("FOO=one\nBAR='two'\n", encoding="utf-8")
            os.environ.pop("FOO", None)
            os.environ["BAR"] = "keep"
            loaded = utils.load_dotenv_file(env_path, override=False)
            self.assertEqual(loaded, 1)
            self.assertEqual(os.environ["FOO"], "one")
            self.assertEqual(os.environ["BAR"], "keep")


if __name__ == "__main__":
    unittest.main()
