import { getApiBaseUrl, getLocaleHeaders } from "@/lib/api";
import type { ChatRequest, ChatResponse, SearchRequest, SearchResponse } from "@/lib/types/tender";

export async function postChat(request: ChatRequest, locale: string, sessionId?: string | null): Promise<ChatResponse> {
  const headers: Record<string, string> = { "Content-Type": "application/json", ...getLocaleHeaders(locale) };
  if (sessionId) headers["X-Chat-Session"] = sessionId;
  const res = await fetch(`${getApiBaseUrl()}/api/chat`, {
    method: "POST",
    headers,
    body: JSON.stringify(request),
  });
  if (!res.ok) throw new Error(`Chat request failed: ${res.status}`);
  return res.json();
}

export async function postSearch(request: SearchRequest, locale: string): Promise<SearchResponse> {
  const res = await fetch(`${getApiBaseUrl()}/api/search`, {
    method: "POST",
    headers: { "Content-Type": "application/json", ...getLocaleHeaders(locale) },
    body: JSON.stringify(request),
  });
  if (!res.ok) throw new Error(`Search request failed: ${res.status}`);
  return res.json();
}

export async function suggestCompanies(query: string, locale: string): Promise<
  { name: string; reg_number: string | null }[]
> {
  if (query.trim().length < 2) return [];
  const res = await fetch(`${getApiBaseUrl()}/api/company-suggest`, {
    method: "POST",
    headers: { "Content-Type": "application/json", ...getLocaleHeaders(locale) },
    body: JSON.stringify({ query }),
  });
  if (!res.ok) return [];
  return res.json();
}

export async function fetchProcedureTypes(): Promise<string[]> {
  const res = await fetch(`${getApiBaseUrl()}/api/procedure-types`);
  if (!res.ok) return [];
  return res.json();
}

export async function resolveCompanyCandidates(query: string, locale: string): Promise<
  { name: string; wins: number; participations: number }[]
> {
  const res = await fetch(`${getApiBaseUrl()}/api/company-resolve`, {
    method: "POST",
    headers: { "Content-Type": "application/json", ...getLocaleHeaders(locale) },
    body: JSON.stringify({ query }),
  });
  if (!res.ok) throw new Error("Company resolve failed");
  return res.json();
}

export async function resolveCompanyCpv(companyName: string, locale: string, regNumber?: string | null): Promise<{
  company: string;
  cpv_prefixes: string[];
  cpv_labels: Record<string, string>;
  match_count: number;
}> {
  const res = await fetch(`${getApiBaseUrl()}/api/company-cpv`, {
    method: "POST",
    headers: { "Content-Type": "application/json", ...getLocaleHeaders(locale) },
    body: JSON.stringify({ company_name: companyName, reg_number: regNumber ?? null }),
  });
  if (!res.ok) throw new Error("Company CPV lookup failed");
  return res.json();
}
