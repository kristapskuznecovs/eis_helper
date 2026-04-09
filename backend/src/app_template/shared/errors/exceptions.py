from __future__ import annotations

from typing import Any


class AppError(Exception):
    def __init__(self, *, code: str, status_code: int = 400, params: dict[str, Any] | None = None) -> None:
        super().__init__(code)
        self.code = code
        self.status_code = status_code
        self.params = params or {}
