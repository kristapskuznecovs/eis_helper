class OCRService:
    def extract_text(self, content: bytes) -> str:
        raise NotImplementedError("Wire your OCR provider here.")
