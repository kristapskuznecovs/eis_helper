import { getApiBaseUrl, getLocaleHeaders } from "@/lib/api";
import type { CompanyData, DashboardData, ProjectsData, PurchaserData, RiskData } from "@/lib/types/dashboard";

function buildUrl(path: string, params: Record<string, string | string[] | number | boolean | null | undefined>): string {
  const base = getApiBaseUrl();
  const parts: string[] = [];
  for (const [key, value] of Object.entries(params)) {
    if (value === null || value === undefined) continue;
    if (Array.isArray(value)) {
      value.forEach((v) => parts.push(`${encodeURIComponent(key)}=${encodeURIComponent(String(v))}`));
    } else {
      parts.push(`${encodeURIComponent(key)}=${encodeURIComponent(String(value))}`);
    }
  }
  const qs = parts.length ? `?${parts.join("&")}` : "";
  return `${base}/api/dashboard${path}${qs}`;
}

export async function fetchDashboard(params: {
  year?: number | null;
  planning_region?: string | null;
  multi_lot?: boolean | null;
  buyer?: string | null;
  category?: string | null;
}, locale: string): Promise<DashboardData> {
  const res = await fetch(buildUrl("", params), { headers: getLocaleHeaders(locale) });
  if (!res.ok) throw new Error(`Dashboard fetch failed: ${res.status}`);
  return res.json();
}

export async function fetchCompany(params: {
  company?: string[];
  year?: number | null;
  planning_region?: string | null;
  multi_lot?: boolean | null;
  buyer?: string | null;
  category?: string | null;
}, locale: string): Promise<CompanyData> {
  const res = await fetch(buildUrl("/company", params), { headers: getLocaleHeaders(locale) });
  if (!res.ok) throw new Error(`Company fetch failed: ${res.status}`);
  return res.json();
}

export async function fetchPurchaser(params: {
  purchaser?: string | null;
  year?: number | null;
  planning_region?: string | null;
  multi_lot?: boolean | null;
  buyer?: string | null;
  category?: string | null;
}, locale: string): Promise<PurchaserData> {
  const res = await fetch(buildUrl("/purchaser", params), { headers: getLocaleHeaders(locale) });
  if (!res.ok) throw new Error(`Purchaser fetch failed: ${res.status}`);
  return res.json();
}

export async function fetchRisk(params: {
  year?: number | null;
  planning_region?: string | null;
  multi_lot?: boolean | null;
  buyer?: string | null;
  category?: string | null;
}, locale: string): Promise<RiskData> {
  const res = await fetch(buildUrl("/risk", params), { headers: getLocaleHeaders(locale) });
  if (!res.ok) throw new Error(`Risk fetch failed: ${res.status}`);
  return res.json();
}

export async function fetchProjects(params: {
  limit?: number;
  offset?: number;
  year?: number | null;
  planning_region?: string | null;
  multi_lot?: boolean | null;
  buyer?: string | null;
  category?: string | null;
}, locale: string): Promise<ProjectsData> {
  const res = await fetch(buildUrl("/projects", params), { headers: getLocaleHeaders(locale) });
  if (!res.ok) throw new Error(`Projects fetch failed: ${res.status}`);
  return res.json();
}

// ---------------------------------------------------------------------------
// Postgres dashboard API (same shape, different base path)
// ---------------------------------------------------------------------------

function buildPgUrl(path: string, params: Record<string, string | string[] | number | boolean | null | undefined>): string {
  const base = getApiBaseUrl();
  const parts: string[] = [];
  for (const [key, value] of Object.entries(params)) {
    if (value === null || value === undefined) continue;
    if (Array.isArray(value)) {
      value.forEach((v) => parts.push(`${encodeURIComponent(key)}=${encodeURIComponent(String(v))}`));
    } else {
      parts.push(`${encodeURIComponent(key)}=${encodeURIComponent(String(value))}`);
    }
  }
  const qs = parts.length ? `?${parts.join("&")}` : "";
  return `${base}/api/dashboard-pg${path}${qs}`;
}

export async function fetchDashboardPg(params: {
  year?: number | null;
  planning_region?: string | null;
  multi_lot?: boolean | null;
  buyer?: string | null;
  category?: string | null;
}, locale: string): Promise<DashboardData> {
  const res = await fetch(buildPgUrl("", params), { headers: getLocaleHeaders(locale) });
  if (!res.ok) throw new Error(`Dashboard-pg fetch failed: ${res.status}`);
  return res.json();
}

export async function fetchCompanyPg(params: {
  company?: string[];
  year?: number | null;
  planning_region?: string | null;
  multi_lot?: boolean | null;
  buyer?: string | null;
  category?: string | null;
}, locale: string): Promise<CompanyData> {
  const res = await fetch(buildPgUrl("/company", params), { headers: getLocaleHeaders(locale) });
  if (!res.ok) throw new Error(`Company-pg fetch failed: ${res.status}`);
  return res.json();
}

export async function fetchPurchaserPg(params: {
  purchaser?: string | null;
  year?: number | null;
  planning_region?: string | null;
  multi_lot?: boolean | null;
  buyer?: string | null;
  category?: string | null;
}, locale: string): Promise<PurchaserData> {
  const res = await fetch(buildPgUrl("/purchaser", params), { headers: getLocaleHeaders(locale) });
  if (!res.ok) throw new Error(`Purchaser-pg fetch failed: ${res.status}`);
  return res.json();
}

export async function fetchRiskPg(params: {
  year?: number | null;
  planning_region?: string | null;
  multi_lot?: boolean | null;
  buyer?: string | null;
  category?: string | null;
}, locale: string): Promise<RiskData> {
  const res = await fetch(buildPgUrl("/risk", params), { headers: getLocaleHeaders(locale) });
  if (!res.ok) throw new Error(`Risk-pg fetch failed: ${res.status}`);
  return res.json();
}

export async function fetchProjectsPg(params: {
  limit?: number;
  offset?: number;
  year?: number | null;
  planning_region?: string | null;
  multi_lot?: boolean | null;
  buyer?: string | null;
  category?: string | null;
}, locale: string): Promise<ProjectsData> {
  const res = await fetch(buildPgUrl("/projects", params), { headers: getLocaleHeaders(locale) });
  if (!res.ok) throw new Error(`Projects-pg fetch failed: ${res.status}`);
  return res.json();
}
