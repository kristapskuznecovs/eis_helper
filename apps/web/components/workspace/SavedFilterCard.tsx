"use client";

import { Edit2, Play, Trash2 } from "lucide-react";
import { useTranslations, useLocale } from "next-intl";
import type { SavedFilter } from "@/lib/types/tender";

interface SavedFilterCardProps {
  filter: SavedFilter;
  newCount?: number;
  onRun: () => void;
  onEdit: () => void;
  onDelete: () => void;
}

const buildSummary = (f: SavedFilter["filters"]): string => {
  const parts: string[] = [];
  if (f.category) parts.push(f.category);
  if (f.planning_region) parts.push(f.planning_region);
  if (f.keywords?.length) parts.push(f.keywords.slice(0, 2).join(", "));
  if (f.value_min_eur) parts.push(`≥€${(f.value_min_eur / 1000).toFixed(0)}k`);
  if (f.value_max_eur) parts.push(`≤€${(f.value_max_eur / 1000).toFixed(0)}k`);
  if (f.deadline_days) parts.push(`${f.deadline_days}d deadline`);
  if (f.subject_type) parts.push(f.subject_type);
  return parts.join(" · ") || "";
};

export function SavedFilterCard({ filter, newCount = 0, onRun, onEdit, onDelete }: SavedFilterCardProps) {
  const t = useTranslations("my.filters");
  const locale = useLocale();
  const formattedDate = new Date(filter.created_at).toLocaleDateString(locale, { day: "numeric", month: "short", year: "numeric" });

  return (
    <div className="flex items-center justify-between gap-4 rounded-2xl border border-border/40 bg-card p-4 shadow-card">
      <div className="min-w-0 flex-1">
        <div className="flex items-center gap-2">
          <p className="truncate text-[14px] font-semibold text-foreground">{filter.name}</p>
          {newCount > 0 && (
            <span className="shrink-0 rounded-full bg-primary/15 px-2 py-0.5 text-[11px] font-semibold text-primary">
              {newCount} jauni
            </span>
          )}
        </div>
        <p className="mt-0.5 truncate text-[12px] text-muted-foreground/60">
          {buildSummary(filter.filters)}
        </p>
        <p className="mt-1 text-[11px] text-muted-foreground/40">
          {t("savedOn", { date: formattedDate })}
        </p>
      </div>
      <div className="flex shrink-0 items-center gap-1">
        <button
          type="button"
          onClick={onRun}
          title={t("runTitle")}
          className="flex items-center gap-1.5 rounded-xl bg-primary/10 px-3 py-1.5 text-[12px] font-medium text-primary transition-colors hover:bg-primary/20"
        >
          <Play className="h-3 w-3" />
          {t("run")}
        </button>
        <button
          type="button"
          onClick={onEdit}
          title={t("editTitle")}
          className="rounded-xl p-2 text-muted-foreground/50 transition-colors hover:bg-accent hover:text-foreground"
        >
          <Edit2 className="h-3.5 w-3.5" />
        </button>
        <button
          type="button"
          onClick={onDelete}
          title={t("deleteTitle")}
          className="rounded-xl p-2 text-muted-foreground/50 transition-colors hover:bg-red-50 hover:text-red-500"
        >
          <Trash2 className="h-3.5 w-3.5" />
        </button>
      </div>
    </div>
  );
}
