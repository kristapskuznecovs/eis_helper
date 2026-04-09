import { TenderResult, SearchResponse } from "@/lib/types/tender";

const mockResults: TenderResult[] = [
  {
    procurement_id: "IEP-2026-1042",
    title: "School renovation works in Riga Central District",
    buyer: "Riga City Council",
    region: "Riga",
    cpv_main: "45000000-7",
    estimated_value_eur: 250000,
    publication_date: "2026-04-01",
    submission_deadline: "2026-05-01",
    status: "Open",
    procedure_type: "Open procedure",
    eis_url: "https://www.eis.gov.lv/EKEIS/Supplier/Procurement/1042",
    match_reason: "Matched by CPV 45 and keyword \"renovation\""
  },
  {
    procurement_id: "IEP-2026-1098",
    title: "Reconstruction of municipal building facade",
    buyer: "Riga Municipality",
    region: "Riga",
    cpv_main: "45443000-4",
    estimated_value_eur: 180000,
    publication_date: "2026-03-28",
    submission_deadline: "2026-04-28",
    status: "Open",
    procedure_type: "Open procedure",
    eis_url: "https://www.eis.gov.lv/EKEIS/Supplier/Procurement/1098",
    match_reason: "Matched by CPV 45 and region Riga"
  },
  {
    procurement_id: "IEP-2026-0987",
    title: "Road surface repair works for the Vidzeme highway section",
    buyer: "Latvian State Roads",
    region: "Riga region",
    cpv_main: "45233142-6",
    estimated_value_eur: 520000,
    publication_date: "2026-03-20",
    submission_deadline: "2026-04-20",
    status: "Open",
    procedure_type: "Restricted procedure",
    eis_url: "https://www.eis.gov.lv/EKEIS/Supplier/Procurement/0987",
    match_reason: "Matched by construction category and road works keyword"
  },
  {
    procurement_id: "IEP-2026-1155",
    title: "Kindergarten insulation and roof replacement",
    buyer: "Jurmala City Council",
    region: "Jurmala",
    cpv_main: "45321000-3",
    estimated_value_eur: 95000,
    publication_date: "2026-04-05",
    submission_deadline: "2026-05-10",
    status: "Open",
    procedure_type: "Open procedure",
    eis_url: "https://www.eis.gov.lv/EKEIS/Supplier/Procurement/1155",
    match_reason: "Matched by CPV 45 and keyword \"renovation\""
  },
  {
    procurement_id: "IEP-2026-0876",
    title: "Public library interior renovation",
    buyer: "Riga Central Library",
    region: "Riga",
    cpv_main: "45400000-1",
    estimated_value_eur: 140000,
    publication_date: "2026-03-15",
    submission_deadline: "2026-04-15",
    status: "Closed",
    procedure_type: "Open procedure",
    eis_url: "https://www.eis.gov.lv/EKEIS/Supplier/Procurement/0876",
    match_reason: "Matched by region Riga and construction category"
  }
];

export function getMockSearchResponse(query: string): SearchResponse {
  return {
    query,
    interpreted_profile: {
      category: "construction",
      cpv_prefixes: ["45"],
      region: "Riga",
      keywords: ["construction", "renovation"],
    },
    filters: {
      planning_region: "Riga",
      cpv_prefixes: ["45"],
      keywords: ["construction", "renovation"],
      publication_date_from: "2026-01-01",
      status: "open",
    },
    results: mockResults,
    total_count: mockResults.length,
  };
}
