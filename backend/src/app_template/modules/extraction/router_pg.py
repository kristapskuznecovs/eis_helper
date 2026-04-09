"""Dashboard analytics API router — Postgres backend."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Query

from app_template.settings import get_settings

from .eis_analytics import AnalyticsService, Filters, PostgresAnalyticsRepository

router = APIRouter(prefix="/api/dashboard-pg", tags=["dashboard-pg"])


def _get_service() -> AnalyticsService:
    database_url = get_settings().database_url
    dsn = database_url.replace("postgresql+psycopg://", "postgresql://")
    return AnalyticsService(PostgresAnalyticsRepository(dsn))


@router.get("")
def get_dashboard(
    year: Annotated[int | None, Query()] = None,
    planning_region: Annotated[str | None, Query()] = None,
    multi_lot: Annotated[bool | None, Query()] = None,
    buyer: Annotated[str | None, Query()] = None,
    category: Annotated[str | None, Query()] = None,
) -> dict:
    filters = Filters(year=year, planning_region=planning_region, multi_lot=multi_lot, buyer=buyer, category=category)
    return _get_service().build_dashboard(filters)


@router.get("/company")
def get_company(
    company: Annotated[list[str] | None, Query()] = None,
    year: Annotated[int | None, Query()] = None,
    planning_region: Annotated[str | None, Query()] = None,
    multi_lot: Annotated[bool | None, Query()] = None,
    buyer: Annotated[str | None, Query()] = None,
    category: Annotated[str | None, Query()] = None,
) -> dict:
    filters = Filters(year=year, planning_region=planning_region, multi_lot=multi_lot, buyer=buyer, category=category)
    return _get_service().build_company_view(filters, company or [])


@router.get("/purchaser")
def get_purchaser(
    purchaser: Annotated[str | None, Query()] = None,
    year: Annotated[int | None, Query()] = None,
    planning_region: Annotated[str | None, Query()] = None,
    multi_lot: Annotated[bool | None, Query()] = None,
    buyer: Annotated[str | None, Query()] = None,
    category: Annotated[str | None, Query()] = None,
) -> dict:
    filters = Filters(year=year, planning_region=planning_region, multi_lot=multi_lot, buyer=buyer, category=category)
    return _get_service().build_purchaser_view(filters, purchaser)


@router.get("/risk")
def get_risk(
    year: Annotated[int | None, Query()] = None,
    planning_region: Annotated[str | None, Query()] = None,
    multi_lot: Annotated[bool | None, Query()] = None,
    buyer: Annotated[str | None, Query()] = None,
    category: Annotated[str | None, Query()] = None,
) -> dict:
    filters = Filters(year=year, planning_region=planning_region, multi_lot=multi_lot, buyer=buyer, category=category)
    return _get_service().build_risk_view(filters)


@router.get("/projects")
def get_projects(
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
    year: Annotated[int | None, Query()] = None,
    planning_region: Annotated[str | None, Query()] = None,
    multi_lot: Annotated[bool | None, Query()] = None,
    buyer: Annotated[str | None, Query()] = None,
    category: Annotated[str | None, Query()] = None,
) -> dict:
    filters = Filters(year=year, planning_region=planning_region, multi_lot=multi_lot, buyer=buyer, category=category)
    return _get_service().list_projects(filters, limit=limit, offset=offset)
