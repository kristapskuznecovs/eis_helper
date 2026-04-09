"use client";

import { useCallback, useEffect, useState } from "react";
import { TenderResult } from "@/lib/types/tender";

const STORAGE_KEY = "eis_bookmarked_tenders";

export function useBookmarks() {
  const [bookmarks, setBookmarks] = useState<TenderResult[]>(() => {
    try {
      const stored = localStorage.getItem(STORAGE_KEY);
      return stored ? JSON.parse(stored) : [];
    } catch {
      return [];
    }
  });

  useEffect(() => {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(bookmarks));
  }, [bookmarks]);

  const toggle = useCallback((tender: TenderResult) => {
    setBookmarks((prev) => {
      const exists = prev.some((item) => item.procurement_id === tender.procurement_id);
      return exists
        ? prev.filter((item) => item.procurement_id !== tender.procurement_id)
        : [...prev, tender];
    });
  }, []);

  const isBookmarked = useCallback(
    (id: string) => bookmarks.some((item) => item.procurement_id === id),
    [bookmarks],
  );

  const remove = useCallback((id: string) => {
    setBookmarks((prev) => prev.filter((item) => item.procurement_id !== id));
  }, []);

  const clear = useCallback(() => {
    setBookmarks([]);
  }, []);

  return { bookmarks, toggle, isBookmarked, remove, clear };
}
