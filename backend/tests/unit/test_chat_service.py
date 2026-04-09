from __future__ import annotations

from types import SimpleNamespace

from app_template.modules.chat.service import (
    ChatService,
    detect_message_locale,
    fallback_quick_replies_for_locale,
    resolve_chat_locale,
)
from app_template.shared.i18n import set_locale


class _FakeCompletions:
    def __init__(self, content: str) -> None:
        self._content = content

    def create(self, **_: object) -> SimpleNamespace:
        return SimpleNamespace(
            choices=[SimpleNamespace(message=SimpleNamespace(content=self._content))]
        )


class _FakeClient:
    def __init__(self, content: str) -> None:
        self.chat = SimpleNamespace(completions=_FakeCompletions(content))


def _make_service(content: str) -> ChatService:
    service = object.__new__(ChatService)
    service.client = _FakeClient(content)
    service.model = "test-model"
    return service


def test_detect_message_locale_prefers_english_text() -> None:
    assert detect_message_locale("Looking for training services for schools next month") == "en"


def test_detect_message_locale_prefers_latvian_text() -> None:
    assert detect_message_locale("Meklēju apmācības pakalpojumus nākamajam mēnesim") == "lv"


def test_resolve_chat_locale_uses_detected_user_message_over_existing_chat_locale() -> None:
    messages = [
        {"role": "assistant", "content": "Kādas apmācības jūs meklējat?"},
        {"role": "user", "content": "Looking for management training"},
    ]
    assert resolve_chat_locale(messages, "lv", "lv") == "en"


def test_resolve_chat_locale_falls_back_to_existing_chat_locale_when_detection_is_ambiguous() -> None:
    messages = [{"role": "user", "content": "SAP"}]
    assert resolve_chat_locale(messages, "en", "lv") == "en"


def test_process_returns_question_with_fallback_quick_replies_in_chat_locale() -> None:
    set_locale("lv")
    service = _make_service('{"type":"question","message":"What training topic?","question_key":"keywords"}')

    result = service.process(
        messages=[{"role": "user", "content": "Looking for language training"}],
        chat_locale="lv",
    )

    assert result["type"] == "question"
    assert result["chat_locale"] == "en"
    assert result["quick_replies"] == fallback_quick_replies_for_locale("en")


def test_process_returns_search_ready_with_resolved_chat_locale() -> None:
    set_locale("en")
    service = _make_service(
        '{"type":"search_ready","message":"Meklēju iepirkumus.","filters":{"status":"open","keywords":[]}}'
    )

    result = service.process(
        messages=[{"role": "user", "content": "Meklēju IT apmācības"}],
        chat_locale="en",
    )

    assert result["type"] == "search_ready"
    assert result["chat_locale"] == "lv"
    assert result["filters"]["status"] == "open"
