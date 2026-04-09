"""Chat and search API endpoints for EIS tender finder."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app_template.shared.db.session import SessionLocal

from .service import ChatService, SearchService
from .sync import EISSyncService

router = APIRouter(prefix="/api", tags=["chat"])


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


class ChatMessageIn(BaseModel):
    role: str
    content: str


class MyCompanyContext(BaseModel):
    name: str
    cpv_prefixes: list[str]


class ChatRequest(BaseModel):
    messages: list[ChatMessageIn]
    my_company: MyCompanyContext | None = None


class SearchFilters(BaseModel):
    keywords: list[str] | None = None
    category: str | None = None
    cpv_prefixes: list[str] | None = None
    cpv_code: str | None = None
    planning_region: str | None = None
    status: str | None = None
    procedure_type: str | None = None
    subject_type: str | None = None
    value_min_eur: float | None = None
    value_max_eur: float | None = None
    deadline_days: int | None = None
    sort: str | None = None
    similar_companies: list[str] | None = None
    similar_company_cpv_prefixes: list[str] | None = None
    buyer: str | None = None
    pub_date_from: str | None = None
    pub_date_to: str | None = None


class SearchRequest(BaseModel):
    filters: SearchFilters


class CompanyCpvRequest(BaseModel):
    company_name: str
    reg_number: str | None = None


class CompanyResolveRequest(BaseModel):
    query: str


class CompanyCandidate(BaseModel):
    name: str
    wins: int
    participations: int


class CompanySuggestRequest(BaseModel):
    query: str


class CompanySuggestion(BaseModel):
    name: str
    reg_number: str | None


class ActivityItem(BaseModel):
    procurement_id: str
    title: str
    buyer: str
    cpv_main: str
    submission_deadline: str
    estimated_value_eur: float | None
    contract_value_eur: float | None
    eis_url: str
    status: str
    signed_date: str | None


class ActivityStats(BaseModel):
    total_participations: int
    total_wins: int
    win_rate: float
    total_won_value_eur: float


class MyActivityResponse(BaseModel):
    company: str
    participations: list[ActivityItem]
    wins: list[ActivityItem]
    stats: ActivityStats


@router.post("/chat")
def chat(request: ChatRequest) -> Any:
    service = ChatService()
    messages = [{"role": m.role, "content": m.content} for m in request.messages]
    return service.process(messages, my_company=request.my_company)


@router.post("/search")
def search(request: SearchRequest, db: Session = Depends(get_db)) -> Any:
    service = SearchService()
    return service.search(request.filters.model_dump(), db)


@router.get("/procedure-types")
def procedure_types(db: Session = Depends(get_db)) -> list[str]:
    from sqlalchemy import distinct

    from app_template.modules.chat.models import Procurement
    rows = db.query(distinct(Procurement.procedure_type)).filter(
        Procurement.procedure_type.isnot(None),
        Procurement.procedure_type != "",
    ).order_by(Procurement.procedure_type).all()
    return [r[0] for r in rows]


@router.post("/company-cpv")
def company_cpv(req: CompanyCpvRequest, db: Session = Depends(get_db)) -> Any:
    service = SearchService()
    return service.get_company_cpv_profile(req.company_name, db, reg_number=req.reg_number)


@router.post("/company-suggest")
def company_suggest(req: CompanySuggestRequest, db: Session = Depends(get_db)) -> list[CompanySuggestion]:
    service = SearchService()
    return [CompanySuggestion.model_validate(item) for item in service.suggest_companies(req.query, db)]


@router.post("/company-resolve")
def company_resolve(req: CompanyResolveRequest, db: Session = Depends(get_db)) -> list[CompanyCandidate]:
    service = SearchService()
    return [CompanyCandidate.model_validate(item) for item in service.resolve_company_candidates(req.query, db)]


@router.get("/my-activity")
def my_activity(company: str, reg_number: str | None = None, db: Session = Depends(get_db)) -> MyActivityResponse:
    service = SearchService()
    return MyActivityResponse.model_validate(service.get_my_activity(company, db, reg_number=reg_number))


@router.post("/sync")
def sync(full: bool = False, db: Session = Depends(get_db)) -> Any:
    """Trigger EIS procurement sync. Use ?full=true for initial backfill."""
    service = EISSyncService()
    return service.run(db=db, full=full)
