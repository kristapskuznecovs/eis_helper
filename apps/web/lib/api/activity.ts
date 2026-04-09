import { getApiBaseUrl } from "@/lib/api";
import type { MyActivityResponse } from "@/lib/types/tender";

export async function fetchMyActivity(company: string, reg_number?: string | null): Promise<MyActivityResponse> {
  const params = new URLSearchParams({ company });
  if (reg_number) params.set("reg_number", reg_number);
  const res = await fetch(`${getApiBaseUrl()}/api/my-activity?${params}`);
  if (!res.ok) throw new Error(`Activity request failed: ${res.status}`);
  return res.json();
}
