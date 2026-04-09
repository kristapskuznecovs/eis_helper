"use client";

import { useEffect, useState } from "react";
import { postSearch } from "@/lib/api/chat";
import type { SavedFilter } from "@/lib/types/tender";

const CHECKED_KEY = "eis_filters_last_checked";
const CHECK_INTERVAL_MS = 23 * 60 * 60 * 1000; // 23 hours

export function useFilterNewCounts(
  filters: SavedFilter[],
  locale: string,
): { counts: Record<string, number>; totalNew: number } {
  const [counts, setCounts] = useState<Record<string, number>>({});

  useEffect(() => {
    if (filters.length === 0) return;

    const lastChecked = localStorage.getItem(CHECKED_KEY);
    const stale = !lastChecked || Date.now() - Number(lastChecked) > CHECK_INTERVAL_MS;
    if (!stale) return;

    Promise.all(
      filters.map(async (f) => {
        // baseline: last run date, or filter creation date (ISO date prefix "YYYY-MM-DD")
        const baseline = f.last_seen_pub_date ?? f.created_at.slice(0, 10);
        try {
          const resp = await postSearch({ filters: f.filters }, locale);
          const newCount = resp.results.filter(
            (r) => r.publication_date && r.publication_date > baseline,
          ).length;
          return [f.id, newCount] as const;
        } catch {
          return [f.id, 0] as const;
        }
      }),
    ).then((entries) => {
      setCounts(Object.fromEntries(entries));
      localStorage.setItem(CHECKED_KEY, String(Date.now()));
    });
  }, [filters, locale]);

  const totalNew = Object.values(counts).reduce((a, b) => a + b, 0);
  return { counts, totalNew };
}
