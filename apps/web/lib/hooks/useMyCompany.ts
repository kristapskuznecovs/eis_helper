"use client";

import { useCallback, useEffect, useRef, useState } from "react";

export interface MyCompany {
  name: string;
  reg_number: string | null;
}

const STORAGE_KEY = "eis_my_company";

function readStorage(): MyCompany | null {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return null;
    // Support legacy plain-string format
    const parsed = JSON.parse(raw);
    if (typeof parsed === "string") return { name: parsed, reg_number: null };
    return parsed as MyCompany;
  } catch {
    return null;
  }
}

export function useMyCompany() {
  const [company, setCompanyState] = useState<MyCompany | null>(null);
  const hasHydrated = useRef(false);

  useEffect(() => {
    setCompanyState(readStorage());
  }, []);

  useEffect(() => {
    if (!hasHydrated.current) {
      hasHydrated.current = true;
      return;
    }
    try {
      if (company) {
        localStorage.setItem(STORAGE_KEY, JSON.stringify(company));
      } else {
        localStorage.removeItem(STORAGE_KEY);
      }
    } catch {
      // ignore
    }
  }, [company]);

  const setCompany = useCallback((name: string, reg_number: string | null = null) => {
    setCompanyState({ name: name.trim(), reg_number: reg_number ?? null });
  }, []);

  const clearCompany = useCallback(() => {
    setCompanyState(null);
  }, []);

  return { company, setCompany, clearCompany };
}
