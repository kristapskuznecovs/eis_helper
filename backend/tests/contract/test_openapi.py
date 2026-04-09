from app_template.main import app


def test_openapi_contains_core_routes() -> None:
    spec = app.openapi()
    paths = spec["paths"]
    assert "/health" in paths
    assert "/api/auth/login" in paths
    assert "/api/auth/register" in paths
    assert "/api/documents/upload" in paths
