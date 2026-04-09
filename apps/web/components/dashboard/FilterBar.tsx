"use client";

import { useTranslations } from "next-intl";
import type { DashboardFilters } from "@/lib/types/dashboard";

interface FilterBarProps {
  filters: DashboardFilters;
  year: string;
  region: string;
  category: string;
  multiLot: string;
  onYear: (v: string) => void;
  onRegion: (v: string) => void;
  onCategory: (v: string) => void;
  onMultiLot: (v: string) => void;
  onReset: () => void;
}

const selectClass = "h-9 rounded-lg border-0 bg-secondary/60 px-3 text-[13px] text-foreground focus:outline-none focus:ring-2 focus:ring-primary/30";

export function FilterBar({ filters, year, region, category, multiLot, onYear, onRegion, onCategory, onMultiLot, onReset }: FilterBarProps) {
  const t = useTranslations("dashboard.filters");
  const common = useTranslations("common");

  return (
    <div className="flex flex-wrap items-end gap-3 rounded-2xl border border-border/40 bg-card p-4 shadow-card">
      <div className="flex flex-col gap-1">
        <label className="text-[11px] font-semibold uppercase tracking-wide text-muted-foreground/70">{t("year")}</label>
        <select className={selectClass} value={year} onChange={(e) => onYear(e.target.value)}>
          <option value="">{t("all")}</option>
          {filters.years.map((y) => <option key={y} value={y}>{y}</option>)}
        </select>
      </div>
      <div className="flex flex-col gap-1">
        <label className="text-[11px] font-semibold uppercase tracking-wide text-muted-foreground/70">{t("region")}</label>
        <select className={selectClass} value={region} onChange={(e) => onRegion(e.target.value)}>
          <option value="">{t("all")}</option>
          {filters.planning_regions.map((r) => <option key={r} value={r}>{r}</option>)}
        </select>
      </div>
      <div className="flex flex-col gap-1">
        <label className="text-[11px] font-semibold uppercase tracking-wide text-muted-foreground/70">{t("category")}</label>
        <select className={selectClass} value={category} onChange={(e) => onCategory(e.target.value)}>
          <option value="">{t("allFem")}</option>
          {filters.categories.map((c) => <option key={c} value={c}>{c}</option>)}
        </select>
      </div>
      <div className="flex flex-col gap-1">
        <label className="text-[11px] font-semibold uppercase tracking-wide text-muted-foreground/70">{t("multiLot")}</label>
        <select className={selectClass} value={multiLot} onChange={(e) => onMultiLot(e.target.value)}>
          <option value="">{t("all")}</option>
          <option value="true">{t("withLots")}</option>
          <option value="false">{t("withoutLots")}</option>
        </select>
      </div>
      <button
        type="button"
        onClick={onReset}
        className="h-9 rounded-lg px-4 text-[13px] font-medium text-muted-foreground/70 transition-colors hover:bg-secondary hover:text-foreground"
      >
        {common("reset")}
      </button>
    </div>
  );
}
