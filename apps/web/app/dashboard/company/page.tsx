"use client";

import { useCallback, useEffect, useState } from "react";
import { useLocale, useTranslations } from "next-intl";
import { CompanyPicker } from "@/components/dashboard/CompanyPicker";
import { FilterBar } from "@/components/dashboard/FilterBar";
import { RankTable } from "@/components/dashboard/RankTable";
import { StatCard } from "@/components/dashboard/StatCard";
import { fetchCompany } from "@/lib/api/dashboard";
import { formatCurrency } from "@/lib/format";
import type { CompanyData } from "@/lib/types/dashboard";

const pct = (v: number | null | undefined) => (v != null ? `${v}%` : "—");

export default function CompanyPage() {
  const locale = useLocale();
  const t = useTranslations("dashboard");
  const [data, setData] = useState<CompanyData | null>(null);
  const [loading, setLoading] = useState(true);
  const [selected, setSelected] = useState<string[]>([]);
  const [year, setYear] = useState("");
  const [region, setRegion] = useState("");
  const [category, setCategory] = useState("");
  const [multiLot, setMultiLot] = useState("");

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const result = await fetchCompany({
        company: selected,
        year: year ? Number(year) : null,
        planning_region: region || null,
        category: category || null,
        multi_lot: multiLot === "true" ? true : multiLot === "false" ? false : null,
      }, locale);
      setData(result);
    } finally {
      setLoading(false);
    }
  }, [selected, year, region, category, multiLot, locale]);

  useEffect(() => { load(); }, [load]);

  const reset = () => { setYear(""); setRegion(""); setCategory(""); setMultiLot(""); setSelected([]); };

  if (!data && loading) return <div className="py-24 text-center text-[14px] text-muted-foreground">{t("loading")}</div>;
  if (!data) return null;

  const { summary, our_fit, competitors, buyers: buyerData } = data;

  return (
    <div className="space-y-6">
      <CompanyPicker
        companies={data.companies}
        selected={selected}
        onChange={setSelected}
      />

      <FilterBar
        filters={data.filters}
        year={year} region={region} category={category} multiLot={multiLot}
        onYear={setYear} onRegion={setRegion} onCategory={setCategory} onMultiLot={setMultiLot}
        onReset={reset}
      />

      {loading ? <div className="py-4 text-center text-[13px] text-muted-foreground/60">{t("updating")}</div> : null}

      {!summary ? (
        <div className="py-16 text-center text-[14px] text-muted-foreground">{t("selectCompany")}</div>
      ) : (
        <>
          <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
            <StatCard label="Pieteikumi" value={summary.applications} />
            <StatCard label="Uzvaras" value={`${summary.wins} (${pct(summary.win_rate_pct)})`} />
            <StatCard label="Uzvarēts (EUR)" value={formatCurrency(summary.won_value_eur, locale)} />
            <StatCard label="Tuvas zaudēšanas ≤3%" value={summary.close_losses_3pct} />
            <StatCard label="Vid. zaudēšanas starpība" value={summary.avg_losing_gap_pct != null ? `${summary.avg_losing_gap_pct}%` : "—"} />
            <StatCard label="Vid. uzvaras rezerve" value={summary.avg_winning_margin_pct != null ? `${summary.avg_winning_margin_pct}%` : "—"} />
            <StatCard label="Pasūtītāji" value={summary.buyers_count} />
            <StatCard label="Kategorijas" value={summary.categories_count} />
          </div>

          {our_fit && (
            <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
              <RankTable title="Mūsu fit — Pasūtītāji" rows={our_fit.buyers} getValue={(r) => `${r["count"]} pieteik.`} />
              <RankTable title="Labākie segmenti" rows={our_fit.segments.map((s) => ({ name: `${s.category} · ${s.region}`, project_count: s.bids, awarded_sum_eur: undefined as unknown as number, win_rate_pct: s.win_rate_pct }))} getValue={(r) => `${pct(r["win_rate_pct"] as number | null)} uzv.`} />
            </div>
          )}

          {competitors && (
            <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
              <RankTable title="Konkurenti" description="Ar ko satiekamies visbiežāk" rows={competitors.met_most} getValue={(r) => `${r["meet_count"]}× sast.`} />
              <RankTable title="Kas mūs pārspēj" rows={competitors.beat_us} getValue={(r) => `${r["beat_us"]}× uzv.`} />
              <RankTable title="Ko pārspējam" rows={competitors.we_beat} getValue={(r) => `${r["we_beat"]}× uzv.`} />
            </div>
          )}

          {buyerData && (
            <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
              <RankTable title="Labākie pasūtītāji" description="Augstāks uzvaru īpatsvars" rows={buyerData.best} getValue={(r) => `${pct(r["win_rate_pct"] as number | null)} (${r["wins"]}/${r["bids"]})`} />
              <RankTable title="Mērķa pasūtītāji" description="Zemāka koncentrācija" rows={buyerData.targets} getValue={(r) => `konc. ${r["market_concentration_pct"] != null ? `${r["market_concentration_pct"]}%` : "—"}`} />
            </div>
          )}
        </>
      )}
    </div>
  );
}
