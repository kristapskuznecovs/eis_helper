# Data Sources & API Reference

Reference for external APIs and open data sources used in this project.

---

## 1. EIS Data Portal (data.gov.lv)

Latvia's open data portal hosts EIS (Elektronisko iepirkumu sistēma) procurement data as CKAN datasets.

**Base URL:** `https://data.gov.lv/dati/api/3/action/`

**Endpoints used:**

| Action | Purpose |
|--------|---------|
| `package_show?id=<dataset>` | List resources in a dataset (get resource IDs) |
| `datastore_search_sql?sql=<SQL>` | Run SQL against a resource table |

### 1.1 Contract Results — `iepirkumu-rezultatu-datu-grupa`

One CSV resource per year. Contains awarded contracts (winners, amounts).

| Year | Resource ID |
|------|-------------|
| 2018 | `cecd0be7-c8e0-451a-8314-f1d806db3bc1` |
| 2019 | `1d37ba16-4d7b-4c1e-9650-1ee9b6a32666` |
| 2020 | `abf811a3-26e8-48c2-bc86-e9b74ca0b385` |
| 2021 | `a1342945-ce4b-480b-abb5-b74d43c41534` |
| 2022 | `97a7c410-60c0-4d08-b554-4d1abb9092da` |
| 2023 | `71f88053-97c1-4928-93c3-8d83d714f27f` |
| 2024 | `3a02a1a7-0322-4c0d-9700-8af9832f0f91` |
| 2025 | `79b34e1c-8989-4984-816a-8e8f92b701f3` |
| 2026 | `c4007411-1ba5-40f3-9b50-f25881c93f51` |

**Key columns:**

| Column | Notes |
|--------|-------|
| `Iepirkuma_ID` | Integer procurement ID (links to announcements table) |
| `Iepirkuma_nosaukums` | Procurement title |
| `Pasutitaja_nosaukums` | Purchaser name |
| `Uzvaretaja_nosaukums` | Winner name |
| `Uzvaretaja_registracijas_numurs` | Winner registration number |
| `Aktuala_liguma_summa` | Current contract value (may be amended; use MAX per ID for dedup) |
| `Sakotneja_liguma_summa` | Original contract value |
| `Liguma_dok_noslegsanas_datums` | Contract signing date |

> **Note:** This table has **no CPV code**. Join with the announcements table on `Iepirkuma_ID` to get CPV.

> **Gotcha:** `Iepirkuma_ID` in results is an integer; in announcements it is stored as `="12345"` (Excel CSV escape). Strip `="` prefix and trailing `"` before comparing.

**Deduplication:** The same `Iepirkuma_ID` appears multiple times when contracts are amended. Keep the row with the highest `Aktuala_liguma_summa` per `(Iepirkuma_ID, Uzvaretaja_nosaukums)` pair.

---

### 1.2 Announcements — `izsludinato-iepirkumu-datu-grupa`

One CSV resource per year. Contains procurement announcements with CPV codes and planned values. **This is the only table with CPV codes.**

| Year | Resource ID |
|------|-------------|
| 2020 | `e6038531-e12d-4bc7-8357-89e37d7de476` |
| 2021 | `6bd2e287-c494-4ddc-9525-802d88872edc` |
| 2022 | `0c4d15c4-cf59-4239-ba76-de4bbf48d824` |
| 2023 | `b8927a5d-1274-4262-bccc-16d21abcf4a3` |
| 2024 | `d7204b7f-0767-472e-b1c7-b85816992885` |
| 2025 | `42739f4f-d625-46a7-9668-ef2e0376e421` |

**Key columns:**

| Column | Notes |
|--------|-------|
| `Iepirkuma_ID` | Stored as `="12345"` — strip Excel prefix before use |
| `CPV_kods_galvenais_prieksmets` | Main CPV code, also Excel-escaped: `="45000000-7 Būvdarbi"` |
| `CPV_kodi_papildus_prieksmeti` | Additional CPV codes |
| `Iepirkuma_izsludinasanas_datums` | Announcement date |
| `Planota_ligumcena` | Estimated contract value |

**CPV filter for construction:** `WHERE "CPV_kods_galvenais_prieksmets" LIKE '="45%'`

---

### 1.3 Example Query Pattern

```python
import urllib.request, json, urllib.parse

BASE = 'https://data.gov.lv/dati/api/3/action/datastore_search_sql'

def fetch_sql(sql):
    params = urllib.parse.urlencode({'sql': sql})
    with urllib.request.urlopen(f'{BASE}?{params}', timeout=60) as r:
        return json.loads(r.read())['result']['records']

def strip_excel(v):
    """Strip Excel CSV escape: '=\"12345\"' -> '12345'"""
    if isinstance(v, str) and v.startswith('="'):
        return v[2:].rstrip('"')
    return str(v) if v is not None else ''

# Find construction procurement IDs from announcements
ann_rows = fetch_sql(f'''
    SELECT "Iepirkuma_ID"
    FROM "e6038531-e12d-4bc7-8357-89e37d7de476"
    WHERE "CPV_kods_galvenais_prieksmets" LIKE '="45%'
''')
ids = {int(strip_excel(r['Iepirkuma_ID'])) for r in ann_rows}

# Fetch winner data from results
result_rows = fetch_sql(f'''
    SELECT "Iepirkuma_ID", "Uzvaretaja_nosaukums", "Uzvaretaja_registracijas_numurs",
           "Aktuala_liguma_summa"
    FROM "abf811a3-26e8-48c2-bc86-e9b74ca0b385"
    WHERE "Uzvaretaja_nosaukums" IS NOT NULL
    LIMIT 50000
''')
# Filter in Python (IN clause with thousands of IDs hits 409 errors)
matched = [r for r in result_rows if r['Iepirkuma_ID'] in ids]
```

> **API limit:** The `datastore_search_sql` endpoint returns max ~32–50k rows. Use `LIMIT n OFFSET m` for pagination. Avoid `IN (...)` with large ID lists — causes HTTP 409 errors. Filter in Python instead.

---

### 1.3 Offer Openings / Participants — `iepirkumu-piedavajumu-atversanu-datu-grupa`

One CSV per year (2016–2026). Contains all bidders/participants at the moment of bid opening.

**Key columns:** `Iepirkuma_ID`, `Pretendenta_nosaukums` (participant name), `Pretendenta_registracijas_numurs`, `Pretendenta_piedavajuma_iesniegsanas_datums`, `CPV_kods_galvenais_prieksmets`, `Pasutitaja_nosaukums`

**Postgres table:** `ckan_participants` — unique on `(procurement_id, participant_reg_number)`

| Year | Resource ID |
|------|-------------|
| 2016 | `8e77cc9e-554e-4bfb-8772-9dc0b7d24608` |
| 2017 | `7733be61-bca2-4ae8-8577-d948058df6c0` |
| 2018 | `e40819ee-3a84-4205-b64e-4c67263ac237` |
| 2019 | `7f23e4c6-9bee-4552-ba0c-a05be1f6ac62` |
| 2020 | `eb1ddcf9-e358-4ceb-a4d7-e406a0a60d7e` |
| 2021 | `25883190-97ef-45a1-9b89-d15cc418a644` |
| 2022 | `4b9317d7-8495-4621-966d-48e00639e2cb` |
| 2023 | `7f4f7e75-8207-4ab1-9470-bcd0112653e9` |
| 2024 | `0bba780e-5ae3-4701-ab16-8f804d5a3e57` |
| 2025 | `4540cc38-0f5f-42a9-9749-3896c3da4488` |
| 2026 | `a45acd54-2e8f-4fb5-b757-31654e875563` |

---

### 1.4 Amendments — `iepirkumu-grozijumu-datu-grupa`

One CSV per year (2016–2026). Contains procurement amendments (deadline changes, scope changes, etc).

**Key columns:** `Iepirkuma_ID`, `Grozijumu_datums` (amendment date), `Iepirkuma_statuss`, `Piedavajumu_iesniegsanas_datumlaiks`

**Postgres table:** `ckan_amendments` — full refresh per year (truncate + reload)

| Year | Resource ID |
|------|-------------|
| 2016 | `792209b1-1465-41f9-b6e7-93878722c249` |
| 2017 | `92e512ed-013b-4757-ba19-56662630f6e6` |
| 2018 | `a1e67b0e-704a-4ca1-96dc-39c87d35c04e` |
| 2019 | `d5e7494f-6f7f-42a9-9683-3c855f3d00a1` |
| 2020 | `3438f0eb-d7d2-4d0c-a1f2-e1d2420c848f` |
| 2021 | `ec6da601-c7f6-466e-9bcb-4efea45dab36` |
| 2022 | `4ad302df-c832-48e5-bd67-89d470ae43b3` |
| 2023 | `25b0c081-49d9-4bea-9e5f-ecac3573d6cb` |
| 2024 | `f8e2e074-e29e-409f-8fec-5d22c7b0bcd1` |
| 2025 | `f7b96bf6-2af8-446a-a6ee-6022601fef7c` |
| 2026 | `d9e4d487-1f1a-4b37-9735-e411708d7288` |

---

### 1.5 Purchase Orders — `pirkuma-pasutijumu-datu-grupa`

One CSV per year (2010–2026). Catalog-based purchase orders placed via EIS (not tender-based).

**Key columns:** `Pasutijuma_Nr`, `Pasutitajs` (buyer), `Piegadatajs` (supplier), `Pasutitas_preces_nosaukums` (item), `Summa_bez_PVN` (amount ex-VAT)

**Postgres table:** `ckan_purchase_orders` — full refresh per year

| Year | Resource ID |
|------|-------------|
| 2010 | `a8f9c39c-5a68-4848-a6c6-57625f205c45` |
| 2020 | `d31fbf99-2708-4558-9c83-d16e69a70e08` |
| 2023 | `d89f4745-77b0-47e4-9e8b-ec494dc3ad1b` |
| 2024 | `11f08c38-50f7-47f3-a700-4f60cd09d943` |
| 2025 | `226db975-8dc7-4f59-9f92-773ef9b58739` |
| 2026 | `a63a9503-d7f1-4840-8e53-0346ed0513a9` |

> Full list in `fetch_ckan_raw.py` → `RESOURCE_IDS["purchase_orders"]`

---

### 1.6 Deliveries — `piegazu-datu-grupa`

One CSV per year (2010–2026). Confirmed deliveries against purchase orders.

**Key columns:** `PasutijumaNr`, `Piegadatajs` (supplier), `Summa_bez_PVN`, `Piegades_adrese`, `Kval_apstipr_dat` (quality approval date)

**Postgres table:** `ckan_deliveries` — full refresh per year

> Full resource ID list in `fetch_ckan_raw.py` → `RESOURCE_IDS["deliveries"]`

---

### 1.7 Buyers Registry — `pasutitaju-datu-grupa`

Single file. Master list of all organizations registered as buyers in EIS.

**Key columns:** `Organizacija`, `RegNr`, `Blokets` (blocked), `Dzests` (deleted)

**Postgres table:** `ckan_buyers` — upsert on `reg_number`

**Resource ID:** `08a09865-b831-462a-a5ce-226f9293ff3e`

---

### 1.8 Postgres Views

| View | Description |
|------|-------------|
| `v_procurement_winners` | `procurements` joined with `ckan_results` |
| `v_procurement_participants` | `procurements` joined with `ckan_participants` |
| `v_procurement_full` | participants + winner flag per row |
| `v_org_activity` | per-org summary: won count, total EUR, participated count |

### 1.9 Sync Script

```bash
# Sync all datasets, 2020–2026
python -m app_template.modules.extraction.fetch_ckan_raw

# Sync specific datasets
python -m app_template.modules.extraction.fetch_ckan_raw --datasets results participants

# Dry run
python -m app_template.modules.extraction.fetch_ckan_raw --dry-run
```

---

## 2. UR Open Data (Uzņēmumu reģistrs)

Latvia's Company Register open data, published via the same CKAN portal.

### 2.1 Company Addresses — CKAN API

**Dataset:** search for `ur-` packages on `data.gov.lv`

**Used in:** `pipelines/find_company_groups.py` — fetches registered address per company reg number to detect sister companies sharing an address.

```python
# Fetch company address from UR CKAN datastore
sql = f'''
    SELECT "registration_number", "legal_address"
    FROM "<ur_resource_id>"
    WHERE "registration_number" = '{reg_no}'
'''
```

Rate limit: add ~0.3s delay between requests.

### 2.2 Beneficial Owners CSV

**File:** downloaded once from UR open data portal.
**Used in:** `pipelines/find_company_groups.py` — matches companies sharing a beneficial owner (UBO) name to detect holding structures.

**Saved to:** `company_ubos` table in the local SQLite DB.

---

## 3. CPV Code Reference

CPV (Common Procurement Vocabulary) codes used in Latvian public procurement.

### Construction & Engineering

| CPV Prefix | Category |
|------------|----------|
| **45xxxxxx** | **Construction works** (all physical building/civil works) |
| 45000000-7 | Construction works (general) |
| 45100000-8 | Site preparation |
| 45200000-9 | Civil engineering |
| 45230000-8 | Road construction |
| 45300000-0 | Building installation works |
| 45400000-1 | Building completion works |

### Engineering & Consulting Services (NOT construction)

| CPV Prefix | Category |
|------------|----------|
| **71xxxxxx** | **Architectural, construction & engineering services** |
| 71000000-8 | Architectural, construction, engineering & inspection (broad umbrella) |
| 71200000-0 | Architectural services |
| 71242000-6 | Project & design preparation, cost estimates |
| 71247000-1 | Supervision of building work |
| 71310000-4 | Engineering consultancy |
| 71319000-7 | Expert witness services |
| 71320000-7 | Engineering design services |
| 71322000-1 | Engineering design for civil works |
| 71520000-9 | Construction supervision services |

> **Key distinction:** A company winning only 71xxx codes is an **engineering consultancy** (supervisor, designer, expert), not a contractor. Example: Firma L4 (reg 40003236001) — 204 contracts 2020-2025, ~€58M, exclusively 71xxx. They supervise Rail Baltica, LVC, LDZ projects.

### Other relevant codes seen in this dataset

| CPV | Description |
|-----|-------------|
| 90910000-9 | Cleaning services |
| 50100000-6 | Vehicle repair & maintenance |
| 48xxxxxx | Software (sometimes mislabelled on construction contracts in EIS) |

> **Data quality note:** EIS CPV codes are sometimes wrong. Example: a BKUS hospital construction contract showed CPV 48xxxxxx (software). Always verify against procurement title when in doubt.

---

## 4. Local SQLite Database

**Path:** `database/eis_procurement_records.sqlite`

### Core tables

#### `procurement_records`
Main table. Each row is one procurement lot/record extracted from EIS.
Coverage: **2023–2025** (fetch 2020–2022 via data portal API if needed).

Key columns: `procurement_id`, `year`, `purchaser_name`, `cpv_main`, `procurement_winner`, `procurement_winner_registration_no`, `procurement_winner_suggested_price_eur`, `winner_company_id`.

#### `companies`
Canonical company index built by `pipelines/build_company_index.py`.

| Column | Description |
|--------|-------------|
| `id` | Internal company ID |
| `canonical_name` | Best display name chosen from aliases |
| `registration_no` | 11-digit Latvian reg number (UNIQUE, NULL if unknown) |
| `source` | `reg_no` / `reg_no_suspect` / `fuzzy_match` / `standalone` |
| `match_confidence` | 0–100 fuzzy score (NULL for reg_no rows) |
| `aliases_json` | JSON array of all raw name strings seen |

**Stats:** ~1197 companies, 98% of winner records linked.
**Suspect reg numbers:** 15 flagged — LLM extraction errors where a reg number bled across unrelated companies in the same document.

#### `company_groups`
Corporate group layer above companies. Sister companies / holding structures.

| Column | Description |
|--------|-------------|
| `id` | Group ID |
| `name` | Display name |
| `source` | `same_owner` / `same_address` / `suspected` / `manual` |
| `notes` | Evidence / rationale |

`companies.group_id` → FK to `company_groups.id`.

**Stats:** ~13 confirmed groups as of 2026-03-23. Built by `pipelines/find_company_groups.py`.

#### `company_ubos`
Beneficial owner data from UR open data.

#### `ur_cache`
Cached UR API responses (company addresses) to avoid repeat lookups.

### Rebuild commands

```bash
# Build/refresh canonical company index
PYTHONPATH=. python3 pipelines/build_company_index.py

# Detect corporate groups (uses UR open data)
PYTHONPATH=. python3 pipelines/find_company_groups.py

# Dry-run (no writes)
PYTHONPATH=. python3 pipelines/build_company_index.py --dry-run
```

---

## 5. Notable Companies & Findings

### CBS IGATE Group (`company_groups` source: `same_owner`)
Multiple SIA entities under the Igate/CBS brand sharing the same registered owner per lursoft.lv:
- Ceļu būvniecības sabiedrība "IGATE" SIA
- CBS Igate SIA (and variants)

Combined: ~65 wins, ~€86.9M across 2023–2025.

### UPB AS (reg 42103000187)
Major general contractor. **19 contracts 2020–2025, €63.5M total.**

| Year | Contracts | €M |
|------|-----------|-----|
| 2020 | 3 | 5.2 |
| 2021 | 4 | 3.2 |
| 2022 | 5 | 14.6 |
| 2023 | 4 | 27.5 |
| 2024 | 2 | 2.6 |
| 2025 | 1 | 10.4 |

CPV breakdown: mostly **45000000-7** (general construction) and **45210000-2** (building works).
Notable contracts: Daugavas stadions €12.4M (2023), Katastrofu pārvaldības centri €10.4M (2025),
RSU farmācijas €9.7M (2022), BKUS rekonstrukcija €8.6M (2023), Olaines siltums €6.5M (2023).

**Why UPB was missing from the local dashboard:** The local DB covers only 2023–2025 announcements,
but UPB's biggest contracts (BKUS 2023, RSU 2022) were announced before that window or had CPV
codes in EIS that were incorrect (e.g. one BKUS contract appeared as CPV 48xxxxxx / software in EIS
even though it was construction). See section 6 for the full explanation.

### Firma L4 (reg 40003236001)
Engineering consultancy (NACE 71.12), **not** a construction contractor.
204 contracts 2020–2025, ~€58.4M total.
Top clients: Latvijas Valsts ceļi, RB Rail, Latvijas dzelzceļš.
All contracts under CPV 71xxx (engineering/supervision/expert services).

### Binders (road construction)
Top winner 2020–2022 by value: 35 contracts, €105.6M.

### Saldus ceļinieks
2020–2022: 48 contracts, €102.8M.

---

## 6. Why Companies May Be Missing from the Dashboard

The local SQLite DB (`procurement_records`) covers only **2023–2025 announcements** (fetched via
the pipeline). Companies underrepresented or absent may be missing for several reasons:

### Reason 1: Contracts announced before 2023
The local DB's `year` column reflects the announcement year. A contract signed in 2023 but
*announced* in 2022 will not appear. To get full history use the data portal API directly
(see section 1).

### Reason 2: Wrong CPV code in EIS
EIS allows purchasers to enter any CPV code. Some construction contracts are filed under
non-construction CPV codes:

| Example | Contract | CPV in EIS | Correct CPV |
|---------|----------|------------|-------------|
| BKUS (UPB) | Hospital building reconstruction | 48xxxxxx (Software) | 45210000-2 |
| RSU farmācijas (UPB) | University lab construction | 45000000-7 | 45000000-7 ✓ |

The pipeline filters by `cpv_main LIKE '45%'` so any contract filed under a non-45 CPV will be
excluded even if it is genuine construction work.

### Reason 3: Company acts as subcontractor
A company may participate as a subcontractor (listed in `subcontractors_json`) rather than prime
winner. Subcontractors are not counted in winner statistics. Example: Baltex Group appears as
subcontractor but not as prime winner.

### Reason 4: LLM extraction errors (reg number bleed)
The pipeline extracts winner names and registration numbers from PDF reports using an LLM.
Sometimes a registration number from one section of a document is assigned to the wrong company.
These are flagged as `source = 'reg_no_suspect'` in the `companies` table (15 cases found).

### How to cross-check
Always use the data portal API (section 1) for authoritative totals on any specific company.
Query by `Uzvaretaja_registracijas_numurs` (exact reg number) across all year resources.
