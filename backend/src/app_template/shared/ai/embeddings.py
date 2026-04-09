class EmbeddingsService:
    def embed_text(self, text: str) -> list[float]:
        raise NotImplementedError("Wire your embeddings provider here.")
