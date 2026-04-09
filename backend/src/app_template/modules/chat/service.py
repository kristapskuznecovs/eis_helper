"""Chat and search services for EIS tender finder."""

from __future__ import annotations

import json
import re
from datetime import date, timedelta
from typing import Any

from openai import OpenAI
from sqlalchemy import and_, or_, text
from sqlalchemy.orm import Session

from app_template.modules.chat.models import Procurement
from app_template.settings import get_settings
from app_template.shared.i18n import _, get_locale, set_locale

# Map English category names to CPV code prefixes
CATEGORY_CPV_MAP: dict[str, list[str]] = {
    "construction": ["45"],
    "it": ["72", "48"],
    "consulting": ["73", "79"],
    "training": ["80"],
    "engineering": ["71"],
    "healthcare": ["85", "33"],
    "supplies": ["34", "39", "44"],
}

# Latvian planning regions → location keywords used in delivery_location column
PLANNING_REGION_KEYWORDS: dict[str, list[str]] = {
    "Rīga": ["rīga", "jūrmala", "ādažu", "ķekavas", "mārupes", "olaines", "ropažu", "salaspils", "siguldas", "ogres"],
    "Vidzeme": ["alūksnes", "balvu", "cēsu", "gulbenes", "limbažu", "madonas", "smiltenes", "valkas", "valmieras", "saulkrastu"],
    "Kurzeme": ["dienvidkurzemes", "kuldīgas", "liepāja", "saldus", "talsu", "tukuma", "ventspils"],
    "Zemgale": ["aizkraukles", "bauskas", "dobeles", "jelgava", "jēkabpils", "jaunjelgava"],
    "Latgale": ["daugavpils", "krāslavas", "ludzas", "preiļu", "rēzekne", "augšdaugavas"],
    "Pierīga": ["ādažu", "ķekavas", "mārupes", "olaines", "ropažu", "salaspils"],
}

# Latvian procurement_status values that mean "open / announced"
OPEN_STATUSES = {"Izsludināts", "Pieteikumi/piedāvājumi atvērti"}
CLOSED_STATUSES = {"Līgums noslēgts", "Noslēgts", "Izbeigts", "Pārtraukts", "Lēmums pieņemts",
                   "Uzsākta līguma slēgšana"}
# Map English subject_type values to Latvian DB values
SUBJECT_TYPE_MAP = {
    "works": "Būvdarbi",
    "services": "Pakalpojums",
    "supplies": "Piegāde",
}

CHAT_LOCALES = {"lv", "en"}
LATVIAN_DIACRITICS_RE = re.compile(r"[āčēģīķļņšūž]")
TOKEN_RE = re.compile(r"[a-zA-Zāčēģīķļņšūž]+", re.IGNORECASE)
ENGLISH_HINTS = {
    "a", "an", "and", "any", "broaden", "building", "company", "construction", "deadline", "filter", "for",
    "found", "help", "i", "ignore", "in", "it", "keywords", "looking", "month", "need", "next", "no", "of",
    "only", "other", "over", "preference", "region", "remove", "search", "services", "software", "the", "these",
    "this", "training", "under", "weeks", "which", "with",
}
LATVIAN_HINTS = {
    "ar", "apmācības", "atradu", "būvniecība", "cits", "filtru", "ir", "jums", "kāda", "ko", "man", "meklēju",
    "mēnesis", "mēneša", "nākamais", "nākamās", "nav", "nedēļas", "noņemt", "pakalpojumi", "paplašināt",
    "reģiona", "reģions", "tikai", "un", "uzņēmums", "vairāk", "vēlos",
}


def detect_message_locale(text: str) -> str | None:
    normalized = text.strip().lower()
    if not normalized:
        return None
    if LATVIAN_DIACRITICS_RE.search(normalized):
        return "lv"

    tokens = TOKEN_RE.findall(normalized)
    if not tokens:
        return None

    english_score = sum(token in ENGLISH_HINTS for token in tokens)
    latvian_score = sum(token in LATVIAN_HINTS for token in tokens)

    if english_score >= 2 and english_score > latvian_score:
        return "en"
    if latvian_score >= 1 and latvian_score > english_score:
        return "lv"
    return None


def resolve_chat_locale(messages: list[dict[str, str]], chat_locale: str | None, request_locale: str) -> str:
    for message in reversed(messages):
        if message.get("role") != "user":
            continue
        detected = detect_message_locale(message.get("content", ""))
        if detected in CHAT_LOCALES:
            return detected
        break
    if chat_locale in CHAT_LOCALES:
        return chat_locale
    return request_locale if request_locale in CHAT_LOCALES else "lv"


def _translate_for_locale(locale: str, key: str) -> str:
    current_locale = get_locale()
    set_locale(locale)
    try:
        return _(key)
    finally:
        set_locale(current_locale)


def fallback_quick_replies_for_locale(locale: str) -> list[str]:
    return [
        _translate_for_locale(locale, "chat.fallback.quick_reply_yes"),
        _translate_for_locale(locale, "chat.fallback.quick_reply_no"),
        _translate_for_locale(locale, "chat.fallback.quick_reply_not_sure"),
        _translate_for_locale(locale, "chat.fallback.quick_reply_other"),
    ]


def build_system_prompt(chat_locale: str, company_context: str = "") -> str:
    response_language = "Latvian" if chat_locale == "lv" else "English"
    option_other = "Cits" if chat_locale == "lv" else "Other"
    contract_value_examples = (
        '["Līdz €20k", "€20k–€100k", "€100k–€500k", "Virs €500k", "Nav preferences", "Cits"]'
        if chat_locale == "lv"
        else '["Under €20k", "€20k–€100k", "€100k–€500k", "Over €500k", "No preference", "Other"]'
    )
    urgency_examples = (
        '["Nākamās 2 nedēļas", "Nākamais mēnesis", "Nākamie 3 mēneši", "Nav preferences", "Cits"]'
        if chat_locale == "lv"
        else '["Next 2 weeks", "Next month", "Next 3 months", "No preference", "Other"]'
    )
    tech_examples = (
        '[".NET / C#", "Java", "C / C++", "Python", "SAP / Oracle", "Cits"]'
        if chat_locale == "lv"
        else '[".NET / C#", "Java", "C / C++", "Python", "SAP / Oracle", "Other"]'
    )
    similar_company_question = (
        "Vai zināt konkurentu vai līdzīgu uzņēmumu, kura iepirkumu aktivitāti vēlaties atdarināt? "
        "Varu izmantot tā CPV kategorijas, lai paplašinātu meklēšanu."
        if chat_locale == "lv"
        else "Do you know a competitor or similar company whose tender activity you'd like to match? "
        "I can use their CPV categories to broaden your search."
    )
    similar_company_replies = (
        '["Jā, nosaukšu uzņēmumu", "Nē, paldies", "Ko tas nozīmē?"]'
        if chat_locale == "lv"
        else '["Yes, I\'ll name one", "No thanks", "What does this mean?"]'
    )

    return f"""You are a procurement assistant helping users find relevant public tenders on the Latvian EIS (Elektroniskā iepirkumu sistēma) system.

Your job is to ask targeted questions one at a time to collect the following filter information:
- category: what industry/type (construction, it, consulting, training, engineering, healthcare, supplies)
- planning_region: which Latvian region or "All Latvia" (Rīga, Vidzeme, Kurzeme, Zemgale, Latgale, Pierīga)
- value_min_eur / value_max_eur: contract size range in EUR
- deadline_days: how many days until submission deadline (urgency)
- subject_type: works, services, or supplies
- keywords: specific technologies, materials, specializations (e.g. "C++", "SAP", "facade insulation")

IMPORTANT: Always search for OPEN tenders only (status: "open"). Never ask the user about tender status — it is always open. Always set deadline_days to at least 14 (2 weeks) by default unless the user says otherwise.

CRITICAL: Every question response MUST include "quick_replies" with 3-6 short clickable options AND always include "{option_other}" as the last option. Never omit quick_replies. Examples:
- Contract value question → {contract_value_examples}
- Urgency question → {urgency_examples}
- Tech stack question → {tech_examples}
- cpv_prefixes: CPV code prefixes (e.g. "45" for construction, "72" for IT)

Rules:
1. Ask EXACTLY ONE question per response. Never ask multiple questions at once.
2. Skip a question only if the user has EXPLICITLY answered it in the conversation. Do not infer or assume.
3. You MUST ask ALL of these questions (in order, skip only if explicitly answered):
   a. Industry-specific specialization (for IT: exact tech stack/platforms; for construction: work type; for consulting: domain; etc.)
   b. Contract value range (always ask — never assume)
   c. Urgency / deadline preference (default 2 weeks if skipped)
   d. Similar company: ask "{similar_company_question}" quick_replies: {similar_company_replies}
      - If user names a company, set similar_companies: ["CompanyName"] in filters.
      - If user already mentioned a company in their first message (e.g. "similar to RBS"), extract it immediately, set similar_companies, and skip asking.
   Only after all 4 are answered (or explicitly skipped by user), return search_ready.
4. Ask INDUSTRY-SPECIFIC follow-ups:
   - IT: ask about tech stack (C++, .NET, Java, Python, SAP, Oracle, Linux, etc.)
   - Construction: ask what type (renovation, new buildings, roads, facades, networks)
   - Consulting: ask domain (management, legal, financial, HR, EU funds, environmental)
   - Training: ask topics (IT skills, language, management, safety, vocational)
   - Engineering: ask discipline (building design, civil, MEP, environmental, roads)
5. For post-search refinements ("remove region filter", "broader results"), return search_ready immediately with updated filters.
6. Be concise and friendly. Questions should be short.
7. Write every human-readable string in {response_language}. Keep JSON keys and enum values exactly as specified in English.{company_context}

Always respond in valid JSON with this exact schema (no extra text, no markdown):
{{
  "type": "question",
  "message": "<question text>",
  "question_key": "<snake_case identifier for this question>",
  "quick_replies": ["<option1>", "<option2>", ...],
  "filter_summary": {{ "<key>": "<value extracted so far>", ... }}
}}
OR:
{{
  "type": "search_ready",
  "message": "<brief confirmation like 'Looking for IT tenders in Riga...'>",
  "filters": {{
    "keywords": ["<kw1>", ...],
    "category": "<category or null>",
    "cpv_prefixes": ["<prefix>", ...],
    "cpv_code": "<exact CPV or null>",
    "planning_region": "<region or null>",
    "status": "open",
    "procedure_type": "<type or null>",
    "subject_type": "<works|services|supplies or null>",
    "value_min_eur": <number or null>,
    "value_max_eur": <number or null>,
    "deadline_days": <number or null>,
    "sort": "<relevance|date_desc|deadline|value_desc or null>",
    "similar_companies": ["<company name>", ...] or null
  }}
}}"""


class ChatService:
    def __init__(self) -> None:
        settings = get_settings()
        self.client = OpenAI(api_key=settings.openai_api_key)
        self.model = settings.ai_model

    def process(
        self,
        messages: list[dict[str, str]],
        my_company: Any = None,
        chat_locale: str | None = None,
    ) -> dict[str, Any]:
        request_locale = get_locale()
        resolved_chat_locale = resolve_chat_locale(messages, chat_locale, request_locale)
        company_context = ""
        if my_company and my_company.cpv_prefixes:
            prefixes_str = ", ".join(my_company.cpv_prefixes)
            company_context = (
                f"\n\nUSER CONTEXT: The user's company is '{my_company.name}'. "
                f"Their known CPV activity prefixes are: [{prefixes_str}]. "
                "Use these as the base for cpv_prefixes and similar_company_cpv_prefixes in filters. "
                "Do NOT ask the 'similar company' question — skip it unless the user explicitly names competitor companies themselves. "
                "If the user does name companies (e.g. 'similar to Turiba, RTU'), extract them into similar_companies as usual. "
                "You may briefly acknowledge their company context in your first response."
            )
        openai_messages = [
            {
                "role": "system",
                "content": build_system_prompt(resolved_chat_locale, company_context),
            }
        ]
        for m in messages:
            openai_messages.append({"role": m["role"], "content": m["content"]})

        response = self.client.chat.completions.create(
            model=self.model,
            messages=openai_messages,  # type: ignore[arg-type]
            response_format={"type": "json_object"},
            temperature=0.3,
        )
        content = response.choices[0].message.content or "{}"
        result = json.loads(content)

        # Guarantee quick_replies are always present for question responses
        if result.get("type") == "question" and not result.get("quick_replies"):
            result["quick_replies"] = fallback_quick_replies_for_locale(resolved_chat_locale)

        result["chat_locale"] = resolved_chat_locale

        return result


CPV_CATEGORY_MAP_LV: dict[str, str] = {
    "03": "Lauksaimniecība", "09": "Enerģija", "14": "Izrakteņi", "15": "Pārtika",
    "22": "Drukātie materiāli", "30": "IT tehnika", "31": "Elektrotehnika",
    "32": "Radio un sakari", "33": "Medicīnas aprīkojums", "34": "Transports",
    "35": "Drošība", "38": "Laboratorijas iekārtas", "39": "Mēbeles",
    "42": "Ražošanas iekārtas", "44": "Celtniecības materiāli", "45": "Celtniecība",
    "48": "Programmatūra", "50": "Uzturēšana un remonts", "55": "Ēdināšana",
    "60": "Transports (pakalpojumi)", "63": "Loģistika", "64": "Sakaru pakalpojumi",
    "65": "Komunālie pakalpojumi", "66": "Finanšu pakalpojumi", "70": "Nekustamais īpašums",
    "71": "Inženierpakalpojumi", "72": "IT pakalpojumi", "73": "Pētniecība",
    "75": "Valsts pārvalde", "77": "Lauksaimniecības pakalpojumi", "79": "Konsultācijas", "80": "Izglītība",
    "85": "Veselības aprūpe", "90": "Vides pakalpojumi", "92": "Kultūra",
    "98": "Citi pakalpojumi",
}


class SearchService:
    def get_my_activity(self, company_name: str, db: Session, reg_number: str | None = None) -> dict[str, Any]:
        def sql_date_expr(column: str) -> str:
            return f"""CASE
                WHEN {column} ~ '^\\d{{4}}-\\d{{2}}-\\d{{2}}$' THEN TO_DATE({column}, 'YYYY-MM-DD')
                WHEN {column} ~ '^\\d{{2}}\\.\\d{{2}}\\.\\d{{4}}$' THEN TO_DATE({column}, 'DD.MM.YYYY')
                ELSE NULL
            END"""

        # Prefer exact reg_number match — catches all name variants for the same legal entity
        if reg_number:
            participant_where = "cp.participant_reg_number = :reg"
            winner_where = "cr.winner_reg_number = :reg"
            stats_participant_where = "participant_reg_number = :reg"
            stats_winner_where = "winner_reg_number = :reg"
            params: dict[str, Any] = {"reg": reg_number}
        else:
            participant_where = "cp.participant_name ILIKE :pattern"
            winner_where = "cr.winner_name ILIKE :pattern"
            stats_participant_where = "participant_name ILIKE :pattern"
            stats_winner_where = "winner_name ILIKE :pattern"
            params = {"pattern": f"%{company_name}%"}

        participation_sort_date_sql = f"""
            COALESCE(
                {sql_date_expr("NULLIF(p.submission_deadline, '')")},
                {sql_date_expr("NULLIF(p.publication_date, '')")}
            )
        """
        win_sort_date_sql = sql_date_expr("NULLIF(cr.contract_signed_date, '')")
        win_fallback_publication_sort_sql = sql_date_expr("NULLIF(p.publication_date, '')")

        participation_sql = text(f"""
            SELECT
                participation.procurement_id,
                participation.title,
                participation.buyer,
                participation.cpv_main,
                participation.submission_deadline,
                participation.estimated_value_eur,
                participation.eis_url,
                participation.status,
                participation.contract_value_eur,
                participation.signed_date
            FROM (
                SELECT DISTINCT ON (p.procurement_id)
                    p.procurement_id,
                    p.title,
                    p.buyer,
                    p.cpv_main,
                    p.submission_deadline,
                    p.estimated_value_eur,
                    p.eis_url,
                    p.status,
                    NULL::numeric AS contract_value_eur,
                    NULL::text AS signed_date,
                    {participation_sort_date_sql} AS sort_date
                FROM ckan_participants cp
                JOIN procurements p ON p.procurement_id = cp.procurement_id
                WHERE {participant_where}
                ORDER BY
                    p.procurement_id,
                    {participation_sort_date_sql} DESC NULLS LAST
            ) AS participation
            ORDER BY participation.sort_date DESC NULLS LAST, participation.procurement_id DESC
            LIMIT 20
        """)

        wins_sql = text(f"""
            SELECT
                wins.procurement_id,
                wins.title,
                wins.buyer,
                wins.cpv_main,
                wins.submission_deadline,
                wins.estimated_value_eur,
                wins.eis_url,
                wins.status,
                wins.contract_value_eur,
                wins.signed_date
            FROM (
                SELECT DISTINCT ON (p.procurement_id)
                    p.procurement_id,
                    p.title,
                    p.buyer,
                    p.cpv_main,
                    p.submission_deadline,
                    p.estimated_value_eur,
                    p.eis_url,
                    p.status,
                    cr.contract_value_eur,
                    cr.contract_signed_date::text AS signed_date,
                    {win_sort_date_sql} AS sort_signed_date
                FROM ckan_results cr
                JOIN procurements p ON p.procurement_id = cr.procurement_id
                WHERE {winner_where}
                ORDER BY
                    p.procurement_id,
                    {win_sort_date_sql} DESC NULLS LAST,
                    {win_fallback_publication_sort_sql} DESC NULLS LAST
            ) AS wins
            ORDER BY wins.sort_signed_date DESC NULLS LAST, wins.procurement_id DESC
            LIMIT 20
        """)

        stats_sql = text(f"""
            WITH matched_participations AS (
                SELECT DISTINCT procurement_id
                FROM ckan_participants
                WHERE {stats_participant_where}
            ),
            matched_wins AS (
                SELECT DISTINCT procurement_id
                FROM ckan_results
                WHERE {stats_winner_where}
            ),
            matched_activity AS (
                SELECT procurement_id FROM matched_participations
                UNION
                SELECT procurement_id FROM matched_wins
            )
            SELECT
                (SELECT COUNT(*) FROM matched_activity)       AS total_contracts,
                (SELECT COUNT(*) FROM matched_participations) AS total_bids,
                (SELECT COUNT(*) FROM matched_wins)           AS total_wins,
                (SELECT COALESCE(SUM(contract_value_eur), 0) FROM ckan_results
                 WHERE {stats_winner_where}
                   AND contract_value_eur IS NOT NULL) AS total_won_value
        """)

        def row_to_item(row: Any) -> dict[str, Any]:
            return {
                "procurement_id": row.procurement_id or "",
                "title": row.title or "",
                "buyer": row.buyer or "",
                "cpv_main": row.cpv_main or "",
                "submission_deadline": row.submission_deadline or "",
                "estimated_value_eur": row.estimated_value_eur,
                "contract_value_eur": float(row.contract_value_eur) if row.contract_value_eur is not None else None,
                "eis_url": row.eis_url or "",
                "status": row.status or "",
                "signed_date": str(row.signed_date) if row.signed_date else None,
            }

        participations = [row_to_item(r) for r in db.execute(participation_sql, params)]
        wins = [row_to_item(r) for r in db.execute(wins_sql, params)]
        counts = db.execute(stats_sql, params).one()

        total_contracts = int(counts.total_contracts)
        total_bids = int(counts.total_bids)
        total_w = int(counts.total_wins)
        win_rate = round(total_w / total_contracts, 3) if total_contracts > 0 else 0.0

        return {
            "company": company_name,
            "participations": participations,
            "wins": wins,
            "stats": {
                "total_contracts": total_contracts,
                "total_participations": total_bids,
                "total_bids": total_bids,
                "total_wins": total_w,
                "win_rate": win_rate,
                "total_won_value_eur": float(counts.total_won_value),
            },
        }

    def get_company_cpv_profile(self, company_name: str, db: Session, reg_number: str | None = None) -> dict[str, Any]:
        if reg_number:
            participant_where = "cp.participant_reg_number = :reg"
            winner_where = "cr.winner_reg_number = :reg"
            params: dict[str, Any] = {"reg": reg_number}
        else:
            # Short names (≤4 chars) must match as whole word to avoid false positives
            name = company_name.strip()
            pattern = f"% {name} %" if len(name) <= 4 else f"%{name}%"
            participant_where = "cp.participant_name ILIKE :pattern"
            winner_where = "cr.winner_name ILIKE :pattern"
            params = {"pattern": pattern}

        participant_sql = text(f"""
            SELECT LEFT(p.cpv_main, 2) AS cpv_prefix, COUNT(*) AS cnt
            FROM ckan_participants cp
            JOIN procurements p ON p.procurement_id = cp.procurement_id
            WHERE {participant_where}
              AND p.cpv_main IS NOT NULL AND p.cpv_main != ''
            GROUP BY cpv_prefix
        """)

        winner_sql = text(f"""
            SELECT LEFT(p.cpv_main, 2) AS cpv_prefix, COUNT(*) AS cnt
            FROM ckan_results cr
            JOIN procurements p ON p.procurement_id = cr.procurement_id
            WHERE {winner_where}
              AND p.cpv_main IS NOT NULL AND p.cpv_main != ''
            GROUP BY cpv_prefix
        """)

        # Wins weighted 3x — they reflect what the company delivers.
        # Participations included with weight 1 as a weaker signal.
        combined: dict[str, int] = {}
        for row in db.execute(participant_sql, params):
            combined[row.cpv_prefix] = combined.get(row.cpv_prefix, 0) + row.cnt
        for row in db.execute(winner_sql, params):
            combined[row.cpv_prefix] = combined.get(row.cpv_prefix, 0) + row.cnt * 3

        top = sorted(combined.items(), key=lambda x: -x[1])[:5]
        cpv_prefixes = [prefix for prefix, _ in top]
        cpv_labels = {prefix: CPV_CATEGORY_MAP_LV.get(prefix, f"CPV {prefix}xxx") for prefix in cpv_prefixes}
        match_count = sum(cnt for _, cnt in top)

        return {
            "company": company_name,
            "cpv_prefixes": cpv_prefixes,
            "cpv_labels": cpv_labels,
            "match_count": match_count,
        }

    def suggest_companies(self, query: str, db: Session) -> list[dict[str, Any]]:
        """Typeahead suggestions from ckan_participants + ckan_results, deduped by name."""
        if not query or len(query.strip()) < 2:
            return []
        pattern = f"%{query.strip()}%"
        sql = text("""
            SELECT name, reg_number
            FROM (
                SELECT participant_name AS name,
                       participant_reg_number AS reg_number,
                       COUNT(*) AS cnt
                FROM ckan_participants
                WHERE participant_name ILIKE :pattern
                  AND participant_name NOT LIKE '=%%'
                GROUP BY participant_name, participant_reg_number
                UNION ALL
                SELECT winner_name AS name,
                       winner_reg_number AS reg_number,
                       COUNT(*) AS cnt
                FROM ckan_results
                WHERE winner_name ILIKE :pattern
                  AND winner_name NOT LIKE '=%%'
                GROUP BY winner_name, winner_reg_number
            ) combined
            GROUP BY name, reg_number
            ORDER BY SUM(cnt) DESC
            LIMIT 8
        """)
        rows = db.execute(sql, {"pattern": pattern}).fetchall()
        return [
            {"name": r.name, "reg_number": r.reg_number or None}
            for r in rows
        ]

    def resolve_company_candidates(self, query: str, db: Session) -> list[dict[str, Any]]:
        """Return distinct company names from CKAN data matching the query, with win/participation counts."""
        name = query.strip()
        if len(name) <= 4:
            pattern = f"% {name} %"
        else:
            pattern = f"%{name}%"

        sql = text("""
            WITH winners AS (
                SELECT winner_name AS name, COUNT(*) AS wins
                FROM ckan_results
                WHERE winner_name ILIKE :pattern
                GROUP BY winner_name
            ),
            participants AS (
                SELECT participant_name AS name, COUNT(*) AS participations
                FROM ckan_participants
                WHERE participant_name ILIKE :pattern
                GROUP BY participant_name
            ),
            combined AS (
                SELECT COALESCE(w.name, p.name) AS name,
                       COALESCE(w.wins, 0) AS wins,
                       COALESCE(p.participations, 0) AS participations
                FROM winners w
                FULL OUTER JOIN participants p ON LOWER(w.name) = LOWER(p.name)
            )
            SELECT name, wins, participations
            FROM combined
            ORDER BY wins DESC, participations DESC
            LIMIT 8
        """)
        rows = db.execute(sql, {"pattern": pattern}).fetchall()
        return [{"name": r.name, "wins": r.wins, "participations": r.participations} for r in rows]

    def search(self, filters: dict[str, Any], db: Session) -> dict[str, Any]:
        category = filters.get("category")
        cpv_prefixes: list[str] = list(filters.get("cpv_prefixes") or [])
        # Merge in any prefixes resolved from similar companies
        for prefix in filters.get("similar_company_cpv_prefixes") or []:
            if prefix not in cpv_prefixes:
                cpv_prefixes.append(prefix)
        if category and category in CATEGORY_CPV_MAP:
            for prefix in CATEGORY_CPV_MAP[category]:
                if prefix not in cpv_prefixes:
                    cpv_prefixes.append(prefix)

        keywords: list[str] = filters.get("keywords") or []
        planning_region = filters.get("planning_region")
        status = filters.get("status") or "open"  # always default to open
        subject_type = filters.get("subject_type")
        value_min = filters.get("value_min_eur")
        value_max = filters.get("value_max_eur")
        deadline_days = filters.get("deadline_days")
        sort = filters.get("sort") or "date_desc"
        cpv_code = filters.get("cpv_code")
        buyer = filters.get("buyer")

        q = db.query(Procurement)

        # CPV filter (from category or explicit prefixes)
        if cpv_prefixes:
            q = q.filter(or_(*[Procurement.cpv_main.like(f"{p}%") for p in cpv_prefixes]))

        # Exact CPV code
        if cpv_code:
            q = q.filter(Procurement.cpv_main.like(f"{cpv_code}%"))

        # Keywords in title
        if keywords:
            q = q.filter(or_(*[Procurement.title.ilike(f"%{kw}%") for kw in keywords]))

        # Buyer / issuer
        if buyer:
            q = q.filter(Procurement.buyer.ilike(f"%{buyer}%"))

        # Region
        if planning_region and planning_region not in ("All Latvia", "all"):
            region_kws = PLANNING_REGION_KEYWORDS.get(planning_region, [planning_region.lower()])
            q = q.filter(or_(*[Procurement.region.ilike(f"%{rk}%") for rk in region_kws]))

        # Status
        if status == "open":
            q = q.filter(Procurement.status.in_(OPEN_STATUSES))
            # Always exclude stale open tenders whose deadline has already passed
            q = q.filter(
                or_(
                    Procurement.submission_deadline == "",
                    Procurement.submission_deadline.is_(None),
                    text("TO_DATE(NULLIF(submission_deadline, ''), 'DD.MM.YYYY') >= CURRENT_DATE"),
                )
            )
        elif status == "closed":
            q = q.filter(Procurement.status.in_(CLOSED_STATUSES))
        elif status and status != "all":
            # Exact Latvian status value passed directly
            q = q.filter(Procurement.status == status)

        # Subject type (Latvian values in DB)
        if subject_type and subject_type in SUBJECT_TYPE_MAP:
            q = q.filter(Procurement.subject_type == SUBJECT_TYPE_MAP[subject_type])

        # Value range
        if value_min is not None:
            q = q.filter(Procurement.estimated_value_eur >= value_min)
        if value_max is not None:
            q = q.filter(Procurement.estimated_value_eur <= value_max)

        # Deadline window — submission_deadline stored as DD.MM.YYYY, so cast via TO_DATE
        if deadline_days:
            today = date.today()
            cutoff = today + timedelta(days=deadline_days)
            q = q.filter(
                and_(
                    text("TO_DATE(NULLIF(submission_deadline, ''), 'DD.MM.YYYY') >= :today").bindparams(today=today),
                    text("TO_DATE(NULLIF(submission_deadline, ''), 'DD.MM.YYYY') <= :cutoff").bindparams(cutoff=cutoff),
                )
            )

        # Procedure type
        procedure_type = filters.get("procedure_type")
        if procedure_type:
            q = q.filter(Procurement.procedure_type == procedure_type)

        # Publication date range
        pub_date_from = filters.get("pub_date_from")
        pub_date_to = filters.get("pub_date_to")
        if pub_date_from:
            q = q.filter(Procurement.publication_date >= pub_date_from)
        if pub_date_to:
            q = q.filter(Procurement.publication_date <= pub_date_to)

        # Sort
        if sort == "deadline":
            q = q.order_by(text("TO_DATE(NULLIF(submission_deadline, ''), 'DD.MM.YYYY') ASC NULLS LAST"))
        elif sort == "value_desc":
            q = q.order_by(Procurement.estimated_value_eur.desc().nulls_last())
        else:
            q = q.order_by(Procurement.publication_date.desc())

        rows = q.limit(50).all()

        # Build match reason
        match_parts: list[str] = []
        similar_cpv = filters.get("similar_company_cpv_prefixes") or []
        similar_cos = filters.get("similar_companies") or []
        if cpv_prefixes:
            base_cpv = [p for p in cpv_prefixes if p not in similar_cpv]
            if base_cpv:
                match_parts.append(_("search.match.cpv", value=", ".join(base_cpv)))
        if similar_cos and similar_cpv:
            match_parts.append(f"similar to {', '.join(similar_cos)} (CPV {', '.join(similar_cpv)})")
        if keywords:
            match_parts.append(_("search.match.keywords", value=", ".join(keywords)))
        if planning_region and planning_region not in ("All Latvia", "all"):
            match_parts.append(_("search.match.region", value=planning_region))
        match_reason = _("search.match.prefix", parts="; ".join(match_parts)) if match_parts else _("search.match.all")

        results = [
            {
                "procurement_id": row.procurement_id,
                "title": row.title or "",
                "buyer": row.buyer or "",
                "region": row.region or "",
                "cpv_main": row.cpv_main or "",
                "estimated_value_eur": row.estimated_value_eur,
                "publication_date": row.publication_date or "",
                "submission_deadline": row.submission_deadline or "",
                "status": row.status or "",
                "procedure_type": row.procedure_type or "",
                "eis_url": row.eis_url or "",
                "match_reason": match_reason,
            }
            for row in rows
        ]

        return {
            "query": "",
            "interpreted_profile": {
                "category": category or "",
                "cpv_prefixes": cpv_prefixes,
                "region": planning_region or "",
                "keywords": keywords,
            },
            "filters": filters,
            "results": results,
            "total_count": len(results),
        }
