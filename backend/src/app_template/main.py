from fastapi import FastAPI, HTTPException
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware

from app_template.lifespan import lifespan
from app_template.modules.auth.router import router as auth_router
from app_template.modules.chat.router import router as chat_router
from app_template.modules.documents.router import router as documents_router
from app_template.modules.extraction.router import router as dashboard_router
from app_template.modules.extraction.router_pg import router as dashboard_pg_router
from app_template.modules.users.router import router as users_router
from app_template.shared.errors.exceptions import AppError
from app_template.shared.errors.handlers import (
    app_error_handler,
    http_exception_handler,
    unhandled_exception_handler,
    validation_exception_handler,
)
from app_template.shared.i18n.middleware import i18n_middleware
from app_template.shared.logging.config import configure_logging
from app_template.shared.logging.middleware import logging_middleware
from app_template.settings import get_settings

configure_logging()
settings = get_settings()

app = FastAPI(title="App Template API", version="0.1.0", lifespan=lifespan)
app.add_middleware(BaseHTTPMiddleware, dispatch=i18n_middleware)
app.add_middleware(BaseHTTPMiddleware, dispatch=logging_middleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[settings.web_origin],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth_router)
app.include_router(users_router)
app.include_router(documents_router)
app.include_router(dashboard_router)
app.include_router(dashboard_pg_router)
app.include_router(chat_router)
app.add_exception_handler(AppError, app_error_handler)
app.add_exception_handler(HTTPException, http_exception_handler)
app.add_exception_handler(RequestValidationError, validation_exception_handler)
app.add_exception_handler(Exception, unhandled_exception_handler)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}
