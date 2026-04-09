"use client";

import { useCallback, useEffect, useState } from "react";
import { useLocale, useTranslations } from "next-intl";
import { FilterBar } from "@/components/dashboard/FilterBar";
import { RankTable } from "@/components/dashboard/RankTable";
import { StatCard } from "@/components/dashboard/StatCard";
import { fetchRisk } from "@/lib/api/dashboard";
import type { RiskData } from "@/lib/types/dashboard";

const pct = (v: number | null | undefined) => (v != null ? `${v}%` : "—");

export default function RiskPage() {
  const locale = useLocale();
  const t = useTranslations("dashboard");
  const [data, setData] = useState<RiskData | null>(null);
  const [loading, setLoading] = useState(true);
  const [year, setYear] = useState("");
  const [region, setRegion] = useState("");
  const [category, setCategory] = useState("");
  const [multiLot, setMultiLot] = useState("");

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const result = await fetchRisk({
        year: year ? Number(year) : null,
        planning_region: region || null,
        category: category || null,
        multi_lot: multiLot === "true" ? true : multiLot === "false" ? false : null,
      }, locale);
      setData(result);
    } finally {
      setLoading(false);
    }
  }, [year, region, category, multiLot, locale]);

  useEffect(() => { load(); }, [load]);

  const reset = () => { setYear(""); setRegion(""); setCategory(""); setMultiLot(""); };

  if (!data && loading) return <div className="py-24 text-center text-[14px] text-muted-foreground">{t("loading")}</div>;
  if (!data) return null;

  const { summary, winners, buyers, hotspots, pairs } = data;

  return (
    <div className="space-y-6">
      <FilterBar
        filters={data.filters}
        year={year} region={region} category={category} multiLot={multiLot}
        onYear={setYear} onRegion={setRegion} onCategory={setCategory} onMultiLot={setMultiLot}
        onReset={reset}
      />

      {loading ? <div className="py-4 text-center text-[13px] text-muted-foreground/60">{t("updating")}</div> : null}

      <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
        <StatCard label="Projekti" value={summary.projects} />
        <StatCard label="1 pretendents" value={`${summary.single_bidder_count} (${pct(summary.single_bidder_share_pct)})`} />
        <StatCard label="Zema konkurence ≤2" value={`${summary.low_competition_count} (${pct(summary.low_competition_share_pct)})`} />
        <StatCard label="Cena virs tāmes" value={pct(summary.award_above_estimate_share_pct)} sub={`${summary.award_above_estimate_count} proj.`} />
        <StatCard label="Cena virs tāmes >10%" value={pct(summary.award_above_estimate_10pct_share_pct)} />
        <StatCard label="Cena zem tāmes >20%" value={pct(summary.award_below_estimate_20pct_share_pct)} />
      </div>

      <RankTable
        title="Riskantākie uzvarētāji"
        description="Uzvarētāji ar augstu riskanto projektu īpatsvaru (min. 3 projekti)"
        rows={winners}
        getValue={(r) => `${pct(r["risky_project_share_pct"] as number | null)} riskanti · ${r["single_bidder_wins"]} viens pret.`}
      />

      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
        <RankTable
          title="Pasūtītāji — 1 pretendents"
          description="Augsts viena pretendenta iepirkumu īpatsvars"
          rows={buyers.single_bidder}
          getValue={(r) => `${pct(r["single_bidder_share_pct"] as number | undefined)} (${r["single_bidder_count"]}/${r["project_count"]})`}
        />
        <RankTable
          title="Pasūtītāji — koncentrācija"
          description="Lielākā uzvarētāja daļa no iepirkumiem"
          rows={buyers.concentration}
          getValue={(r) => `${pct(r["top_winner_share_pct"] as number | undefined)} → ${r["top_winner"]}`}
        />
      </div>

      <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
        <RankTable
          title="Riska karstpunkti — Pasūtītāji"
          rows={buyers.risk_hotspots}
          getValue={(r) => `${pct(r["risky_project_share_pct"] as number | null)} riskanti`}
        />
        <RankTable
          title="Riska karstpunkti — Reģioni"
          rows={hotspots.regions}
          getValue={(r) => `${pct(r["risky_project_share_pct"] as number | null)} riskanti`}
        />
        <RankTable
          title="Riska karstpunkti — Kategorijas"
          rows={hotspots.categories}
          getValue={(r) => `${pct(r["risky_project_share_pct"] as number | null)} riskanti`}
        />
      </div>

      <div className="rounded-2xl border border-border/40 bg-card p-5 shadow-card">
        <h3 className="mb-3 text-[15px] font-semibold text-foreground">Atkārtotas tuvas cenu pāri</h3>
        <p className="mb-3 text-[12px] text-muted-foreground/70">Pretendenti, kas regulāri iesniedz ļoti tuvas cenas (&le;3% starpība, vismaz 3 reizes).</p>
        <div className="space-y-1.5">
          {pairs.map((pair, i) => (
            <div key={i} className="flex items-center justify-between gap-3 rounded-lg px-2 py-1.5 hover:bg-secondary/50">
              <span className="text-[13px] text-foreground">{pair.name}</span>
              <div className="shrink-0 text-right text-[12px] text-muted-foreground">
                <span className="font-semibold text-foreground">{pair.close_bid_count}×</span> tuvu / {pair.meet_count}× kopā
                {pair.lowest_price_close_count > 0 && <span className="ml-2 text-destructive/70">{pair.lowest_price_close_count}× zemākā cena</span>}
              </div>
            </div>
          ))}
          {pairs.length === 0 && <p className="py-4 text-center text-[13px] text-muted-foreground/50">Nav datu</p>}
        </div>
      </div>
    </div>
  );
}
