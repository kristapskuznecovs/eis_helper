import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI

logger = logging.getLogger(__name__)


async def _nightly_sync_loop() -> None:
    """Runs EIS procurement sync once per day at 03:00 UTC."""
    from app_template.modules.chat.sync import EISSyncService
    from app_template.shared.db.session import SessionLocal

    while True:
        now = datetime.now(timezone.utc)
        # Next 03:00 UTC
        next_run = now.replace(hour=3, minute=0, second=0, microsecond=0)
        if next_run <= now:
            next_run = next_run.replace(day=next_run.day + 1)
        delay = (next_run - now).total_seconds()
        logger.info("EIS sync scheduled in %.0f seconds (at %s UTC)", delay, next_run.strftime("%H:%M"))

        await asyncio.sleep(delay)

        logger.info("EIS nightly sync starting")
        db = SessionLocal()
        try:
            result = EISSyncService().run(db=db, full=False)
            logger.info("EIS nightly sync done: %s", result)
        except Exception:
            logger.exception("EIS nightly sync failed")
        finally:
            db.close()


@asynccontextmanager
async def lifespan(_: FastAPI):
    task = asyncio.create_task(_nightly_sync_loop())
    try:
        yield
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
