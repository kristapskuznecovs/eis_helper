from dataclasses import dataclass


@dataclass
class StoredFile:
    key: str
    url: str
    size_bytes: int
    content_type: str
    backend: str
