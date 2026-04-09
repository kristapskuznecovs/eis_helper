"use client";

import { ExternalLink } from "lucide-react";
import { useTranslations, useLocale } from "next-intl";
import { cpvLabel } from "@/lib/cpv";
import type { ActivityItem } from "@/lib/types/tender";

interface ActivityCardProps {
  item: ActivityItem;
  variant: "participation" | "win";
}

const formatValue = (value?: number, locale?: string) => {
  if (!value) return null;
  return new Intl.NumberFormat(locale ?? "en-GB", {
    style: "currency",
    currency: "EUR",
    maximumFractionDigits: 0,
  }).format(value);
};

const formatDate = (value?: string, locale?: string) => {
  if (!value) return "—";
  const m = value.match(/^(\d{2})\.(\d{2})\.(\d{4})$/);
  const d = m ? new Date(+m[3], +m[2] - 1, +m[1]) : new Date(value);
  if (isNaN(d.getTime())) return value;
  return d.toLocaleDateString(locale ?? "en-GB", { day: "numeric", month: "short", year: "numeric" });
};

export function ActivityCard({ item, variant }: ActivityCardProps) {
  const t = useTranslations("my.activity.card");
  const locale = useLocale();
  const isWin = variant === "win";
  const value = isWin ? item.contract_value_eur : item.estimated_value_eur;
  const dateLabel = isWin ? t("signed") : t("deadline");
  const dateValue = isWin ? item.signed_date : item.submission_deadline;
  const prefix = item.cpv_main?.slice(0, 2) ?? "";

  return (
    <div className={`relative rounded-2xl border bg-card p-4 shadow-card transition-colors hover:border-primary/20 ${isWin ? "border-l-4 border-l-match border-border/40" : "border-border/40"}`}>
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0 flex-1 space-y-1.5">
          <p className="line-clamp-2 text-[13px] font-semibold leading-snug text-foreground">
            {item.title || "—"}
          </p>
          <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-[12px] text-muted-foreground">
            <span>{item.buyer || "—"}</span>
            {prefix && (
              <span className="font-mono">
                {prefix} — {cpvLabel(prefix)}
              </span>
            )}
          </div>
          <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-[12px] text-muted-foreground/70">
            <span>{dateLabel}: {formatDate(dateValue, locale)}</span>
            {value ? <span className="font-semibold text-foreground/80">{formatValue(value, locale)}</span> : null}
          </div>
        </div>
        {item.eis_url && (
          <a
            href={item.eis_url}
            target="_blank"
            rel="noopener noreferrer"
            className="shrink-0 rounded-xl p-1.5 text-muted-foreground/40 transition-colors hover:bg-accent hover:text-primary"
            title="View on EIS"
          >
            <ExternalLink className="h-3.5 w-3.5" />
          </a>
        )}
      </div>
    </div>
  );
}
