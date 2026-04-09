"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import type { ExtractedFilters, SavedFilter } from "@/lib/types/tender";

const STORAGE_KEY = "eis_saved_filters";

const generateId = () => `${Date.now()}-${Math.random().toString(36).slice(2)}`;

function readStorage(): SavedFilter[] {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    return raw ? JSON.parse(raw) : [];
  } catch {
    return [];
  }
}

export function useSavedFilters() {
  const [filters, setFilters] = useState<SavedFilter[]>([]);
  const hasHydrated = useRef(false);

  useEffect(() => {
    setFilters(readStorage());
  }, []);

  useEffect(() => {
    if (!hasHydrated.current) {
      hasHydrated.current = true;
      return;
    }

    try {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(filters));
    } catch {
      // ignore
    }
  }, [filters]);

  const save = useCallback((name: string, filterValues: ExtractedFilters): SavedFilter => {
    const entry: SavedFilter = {
      id: generateId(),
      name: name.trim(),
      filters: filterValues,
      created_at: new Date().toISOString(),
    };
    setFilters((prev) => [entry, ...prev]);
    return entry;
  }, []);

  const remove = useCallback((id: string) => {
    setFilters((prev) => prev.filter((f) => f.id !== id));
  }, []);

  const update = useCallback((id: string, partial: Partial<Pick<SavedFilter, "name" | "filters">>) => {
    setFilters((prev) => prev.map((f) => (f.id === id ? { ...f, ...partial } : f)));
  }, []);

  const markSeen = useCallback((id: string, newestPubDate: string) => {
    setFilters((prev) => prev.map((f) => (f.id === id ? { ...f, last_seen_pub_date: newestPubDate } : f)));
  }, []);

  const clear = useCallback(() => {
    setFilters([]);
  }, []);

  return { filters, save, remove, update, markSeen, clear };
}
