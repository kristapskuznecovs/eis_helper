"use client";

import { useCallback, useEffect, useState } from "react";
import { useLocale, useTranslations } from "next-intl";
import { FilterBar } from "@/components/dashboard/FilterBar";
import { RankTable } from "@/components/dashboard/RankTable";
import { StatCard } from "@/components/dashboard/StatCard";
import { fetchPurchaserPg } from "@/lib/api/dashboard";
import { formatCurrency } from "@/lib/format";
import type { PurchaserData } from "@/lib/types/dashboard";

const pct = (v: number | null | undefined) => (v != null ? `${v}%` : "—");

const selectClass = "h-9 rounded-lg border-0 bg-secondary/60 px-3 text-[13px] text-foreground focus:outline-none focus:ring-2 focus:ring-primary/30";

export default function PurchaserPgPage() {
  const locale = useLocale();
  const t = useTranslations("dashboard");
  const [data, setData] = useState<PurchaserData | null>(null);
  const [loading, setLoading] = useState(true);
  const [purchaser, setPurchaser] = useState("");
  const [year, setYear] = useState("");
  const [region, setRegion] = useState("");
  const [category, setCategory] = useState("");
  const [multiLot, setMultiLot] = useState("");

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const result = await fetchPurchaserPg({
        purchaser: purchaser || null,
        year: year ? Number(year) : null,
        planning_region: region || null,
        category: category || null,
        multi_lot: multiLot === "true" ? true : multiLot === "false" ? false : null,
      }, locale);
      setData(result);
      if (!purchaser && result.selected_purchaser) {
        setPurchaser(result.selected_purchaser);
      }
    } finally {
      setLoading(false);
    }
  }, [purchaser, year, region, category, multiLot, locale]);

  useEffect(() => { load(); }, [load]);

  const reset = () => { setYear(""); setRegion(""); setCategory(""); setMultiLot(""); setPurchaser(""); };

  if (!data && loading) return <div className="py-24 text-center text-[14px] text-muted-foreground">{t("loading")}</div>;
  if (!data) return null;

  const { summary, fit, suppliers, segments, market_context } = data;

  return (
    <div className="space-y-6">
      <div className="rounded-2xl border border-border/40 bg-card p-5 shadow-card">
        <label className="mb-2 block text-[11px] font-semibold uppercase tracking-wide text-muted-foreground/70">Pasūtītājs</label>
        <select className={`${selectClass} w-full`} value={purchaser} onChange={(e) => setPurchaser(e.target.value)}>
          <option value="">Izvēlies pasūtītāju</option>
          {data.purchasers.map((p) => <option key={p} value={p}>{p}</option>)}
        </select>
      </div>

      <FilterBar
        filters={data.filters}
        year={year} region={region} category={category} multiLot={multiLot}
        onYear={setYear} onRegion={setRegion} onCategory={setCategory} onMultiLot={setMultiLot}
        onReset={reset}
      />

      {loading ? <div className="py-4 text-center text-[13px] text-muted-foreground/60">{t("updating")}</div> : null}

      {!summary ? (
        <div className="py-16 text-center text-[14px] text-muted-foreground">{t("selectPurchaser")}</div>
      ) : (
        <>
          <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
            <StatCard label="Projekti" value={summary.projects} />
            <StatCard label="Kopā līgumcena" value={formatCurrency(summary.awarded_sum_eur, locale)} />
            <StatCard label="Piegādātāji" value={summary.suppliers_count} />
            <StatCard label="Vid. pretendenti" value={summary.avg_competitors ?? "—"} />
            <StatCard label="1 pretendents" value={pct(summary.single_bidder_share_pct)} />
            <StatCard label="Top piegādātāja daļa" value={pct(summary.top_supplier_share_pct)} />
            <StatCard label="Vid. lēmuma kavēšanās" value={summary.avg_decision_lag_days != null ? `${summary.avg_decision_lag_days} d.` : "—"} />
          </div>

          {suppliers && (
            <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
              <RankTable title="Top piegādātāji" description="Pēc līgumcenas" rows={suppliers.top_winners} getValue={(r) => `${formatCurrency(r["awarded_sum_eur"] as number, locale)} (${r["win_share_pct"]}%)`} />
              <RankTable title="Biežākie pretendenti" rows={suppliers.frequent_bidders} getValue={(r) => `${r["project_count"]} pieteik. · ${r["win_count"]} uzv.`} />
            </div>
          )}

          {fit && (
            <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
              <RankTable title="Kategorijas" rows={fit.categories} getValue={(r) => `${r["count"]}`} />
              <RankTable title="Reģioni" rows={fit.regions} getValue={(r) => `${r["count"]}`} />
              <RankTable title="Gadi" rows={fit.years} getValue={(r) => `${r["count"]} proj.`} />
            </div>
          )}

          {segments && (
            <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
              <RankTable title="Lielākie segmenti" rows={segments.biggest.map((s) => ({ name: `${s.category} · ${s.region}`, awarded_sum_eur: s.awarded_sum_eur, project_count: s.projects }))} />
              <RankTable title="Atvērtie segmenti" description="Zema koncentrācija, vieta ieiet" rows={segments.open.map((s) => ({ name: `${s.category} · ${s.region}`, project_count: s.projects }))} getValue={(r) => `${r["project_count"]} proj.`} />
            </div>
          )}

          {market_context && (
            <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
              <RankTable
                title={`Top pasūtītāji — ${market_context.dominant_region ?? "reģions"}`}
                rows={market_context.top_purchasers_region}
                getValue={(r) => `${formatCurrency(r["awarded_sum_eur"] as number, locale)}${r["is_selected"] ? " ◀" : ""}`}
              />
              <RankTable
                title={`Top uzņēmumi — ${market_context.dominant_region ?? "reģions"}`}
                rows={market_context.top_companies_region}
                getValue={(r) => formatCurrency(r["awarded_sum_eur"] as number, locale)}
              />
            </div>
          )}
        </>
      )}
    </div>
  );
}
