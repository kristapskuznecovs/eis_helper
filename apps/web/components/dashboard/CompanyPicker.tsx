"use client";

import { useState } from "react";
import { useTranslations } from "next-intl";

interface CompanyPickerProps {
  companies: string[];
  selected: string[];
  onChange: (selected: string[]) => void;
}

export function CompanyPicker({ companies, selected, onChange }: CompanyPickerProps) {
  const t = useTranslations("dashboard.filters");
  const [search, setSearch] = useState("");

  const toggle = (name: string) => {
    onChange(selected.includes(name) ? selected.filter((company) => company !== name) : [...selected, name]);
  };

  const filtered = companies.filter((company) => !search || company.toLowerCase().includes(search.toLowerCase()));
  // Selected companies always pinned to top, then remaining filtered (excluding already-shown selected)
  const visible = [...selected, ...filtered.filter((name) => !selected.includes(name))].slice(0, 100 + selected.length);

  return (
    <div className="rounded-2xl border border-border/40 bg-card p-5 shadow-card">
      <label className="mb-2 block text-[11px] font-semibold uppercase tracking-wide text-muted-foreground/70">
        {t("company")}
      </label>

      <div className="mb-3 flex gap-2">
        <input
          type="text"
          value={search}
          onChange={(e) => { setSearch(e.target.value); }}
          placeholder={t("companyPlaceholder")}
          className="h-9 flex-1 rounded-lg border-0 bg-secondary/60 px-3 text-[14px] text-foreground placeholder:text-muted-foreground/40 focus:outline-none focus:ring-2 focus:ring-primary/30"
        />
      </div>

      <div className="flex max-h-32 flex-wrap gap-1.5 overflow-y-auto">
        {visible.map((name) => (
          <button
            key={name}
            type="button"
            onClick={() => toggle(name)}
            className={`rounded-full px-2.5 py-1 text-[12px] font-medium transition-colors ${
              selected.includes(name)
                ? "bg-primary text-primary-foreground"
                : "bg-secondary/60 text-foreground hover:bg-secondary"
            }`}
          >
            {name}
          </button>
        ))}
        {filtered.length === 0 && search && (
          <p className="py-1 text-[12px] text-muted-foreground/50">{t("companyNoMatches")}</p>
        )}
      </div>

      {selected.length > 0 && (
        <p className="mt-2 text-[12px] text-muted-foreground/60">
          {t("companySelected", { value: selected.join(" · ") })}
        </p>
      )}
    </div>
  );
}
