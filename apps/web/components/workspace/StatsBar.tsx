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

  const winRatePct = (stats.win_rate * 100).toFixed(0);
  const directAwards = stats.total_contracts - stats.total_bids;

  const items = [
    {
      label: t("totalContracts"),
      value: stats.total_contracts.toLocaleString(locale),
      sub: t("bidsSub", { bids: stats.total_bids, direct: directAwards }),
    },
    { label: t("won"), value: stats.total_wins.toLocaleString(locale), sub: null },
    { label: t("winRate"), value: `${winRatePct}%`, sub: null },
    { label: t("totalWon"), value: formatEur(stats.total_won_value_eur), sub: null },
  ];

  return (
    <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
      {items.map(({ label, value, sub }) => (
        <div key={label} className="rounded-2xl bg-secondary/50 p-3 text-center">
          <p className="tabular-nums text-[20px] font-bold text-foreground">{value}</p>
          <p className="text-[12px] text-muted-foreground/70">{label}</p>
          {sub && <p className="mt-0.5 text-[10px] text-muted-foreground/40">{sub}</p>}
        </div>
      ))}
    </div>
  );
}
