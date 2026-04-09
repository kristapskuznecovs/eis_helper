# AI Pipeline Runbook

This template is designed for OCR, document parsing, structured extraction, and later embeddings/chat workflows.

## Where code goes

- `shared/ai/`:
  provider clients, OCR adapters, embeddings adapters, reusable prompts
- `modules/documents/`:
  upload, persistence, queue trigger, document lifecycle state
- `modules/extraction/`:
  parse structured information from OCR/text output
- `modules/<future-domain>/`:
  domain-specific interpretation of extracted data

## Flow

1. Upload enters through a module router.
2. Service stores the file through `shared/storage/`.
3. Service persists metadata in the database.
4. Service enqueues work through `shared/jobs/`.
5. Worker task delegates back into module service code.
6. OCR/parsing uses `shared/ai/` adapters, not provider SDKs directly.
7. Results are written back to module-owned tables.

## Hard rules

- Do not call OpenAI, Gemini, Anthropic, PaddleOCR, or similar SDKs from module routers.
- Do not hide prompts inside route handlers.
- Do not mix provider request/response schemas into business models.
- Keep OCR and model selection inside `shared/ai/`.

## When to add `tasks.py`

Add `tasks.py` when:

- work is slow enough to hurt request latency
- retries matter
- OCR/parsing can fail independently from upload

Skip `tasks.py` when:

- the module has no async work yet
- the logic is fast and purely synchronous
