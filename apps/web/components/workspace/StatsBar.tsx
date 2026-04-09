"use client";

import { useTranslations, useLocale } from "next-intl";
import type { ActivityStats } from "@/lib/types/tender";

interface StatsBarProps {
  stats: ActivityStats;
}

export function StatsBar({ stats }: StatsBarProps) {
  const t = useTranslations("my.activity.stats");
  const locale = useLocale();

  const formatEur = (value: number) =>
    new Intl.NumberFormat(locale, {
      style: "currency",
      currency: "EUR",
      maximumFractionDigits: 0,
      notation: value >= 1_000_000 ? "compact" : "standard",
    }).format(value);

  const winRatePct = Math.min(stats.win_rate * 100, 100).toFixed(0);

  const items = [
    { label: t("participated"), value: stats.total_participations.toLocaleString(locale) },
    { label: t("won"), value: stats.total_wins.toLocaleString(locale) },
    { label: t("winRate"), value: `${winRatePct}%` },
    { label: t("totalWon"), value: formatEur(stats.total_won_value_eur) },
  ];

  return (
    <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
      {items.map(({ label, value }) => (
        <div key={label} className="rounded-2xl bg-secondary/50 p-3 text-center">
          <p className="tabular-nums text-[20px] font-bold text-foreground">{value}</p>
          <p className="text-[12px] text-muted-foreground/70">{label}</p>
        </div>
      ))}
    </div>
  );
}
