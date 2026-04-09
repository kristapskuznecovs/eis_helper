"use client";

import { useCallback, useEffect, useState } from "react";
import { useLocale, useTranslations } from "next-intl";
import { FilterBar } from "@/components/dashboard/FilterBar";
import { RankTable } from "@/components/dashboard/RankTable";
import { StatCard } from "@/components/dashboard/StatCard";
import { fetchDashboardPg } from "@/lib/api/dashboard";
import { formatCurrency } from "@/lib/format";
import type { DashboardData } from "@/lib/types/dashboard";

const pct = (value: number | null) => (value != null ? `${value}%` : "—");

export default function DashboardPgPage() {
  const locale = useLocale();
  const t = useTranslations();
  const isEn = locale === "en";
  const [data, setData] = useState<DashboardData | null>(null);
  const [loading, setLoading] = useState(true);
  const [year, setYear] = useState("");
  const [region, setRegion] = useState("");
  const [category, setCategory] = useState("");
  const [multiLot, setMultiLot] = useState("");

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const result = await fetchDashboardPg({
        year: year ? Number(year) : null,
        planning_region: region || null,
        category: category || null,
        multi_lot: multiLot === "true" ? true : multiLot === "false" ? false : null,
      }, locale);
      setData(result);
    } finally {
      setLoading(false);
    }
  }, [category, locale, multiLot, region, year]);

  useEffect(() => { void load(); }, [load]);

  const reset = () => { setYear(""); setRegion(""); setCategory(""); setMultiLot(""); };

  if (!data && loading) {
    return <div className="py-24 text-center text-[14px] text-muted-foreground">{t("dashboard.loading")}</div>;
  }
  if (!data) return null;

  const labels = isEn ? {
    years: "Years",
    year: "Year",
    projects: "Projects",
    awarded: "Awarded",
    estimated: "Estimated",
    multiLot: "Multi-lot",
    topWinners: "Winners",
    topWinnersDesc: "Top 10 by awarded value",
    topBuyers: "Buyers",
    topBuyersDesc: "Top 10 by awarded value",
    regions: "Regions",
    regionsDesc: "Derived from delivery location",
    bidders: "Bidders",
    biddersDesc: "Most active by applications",
    biggestWinners: "Biggest winners",
    biggestWinnersDesc: "Highest win rate",
    biggestLosers: "Biggest losers",
    biggestLosersDesc: "Many applications, no wins",
    closeLosses: "Close losing bids",
    closeLossesDesc: "Often lose by small margin",
    buyerConcentration: "Buyer concentration",
    buyerConcentrationDesc: "Top winner share",
    multiLotTitle: "Multi-lot procurements",
    share: "Share",
    knownLots: "Known lots",
    avgLots: "Avg. lots/project",
    dataQuality: "Data quality"
  } : {
    years: "Gadi",
    year: "Gads",
    projects: "Projekti",
    awarded: "Līgumcena",
    estimated: "Paredzamā",
    multiLot: "Vairāku daļu",
    topWinners: "Uzvarētāji",
    topWinnersDesc: "Top 10 pēc līgumcenas",
    topBuyers: "Pasūtītāji",
    topBuyersDesc: "Top 10 pēc noslēgtās summas",
    regions: "Reģioni",
    regionsDesc: "Atvasināti no piegādes vietas",
    bidders: "Pretendenti",
    biddersDesc: "Aktīvākie pēc pieteikumu skaita",
    biggestWinners: "Lielākie uzvarētāji",
    biggestWinnersDesc: "Augstākais uzvaru īpatsvars",
    biggestLosers: "Lielākie zaudētāji",
    biggestLosersDesc: "Daudzi pieteikumi, 0 uzvaras",
    closeLosses: "Tuvu zaudētās cenas",
    closeLossesDesc: "Bieži zaudē ar mazu starpību",
    buyerConcentration: "Pasūtītāju koncentrācija",
    buyerConcentrationDesc: "Lielākā uzvarētāja daļa",
    multiLotTitle: "Vairāku daļu iepirkumi",
    share: "Īpatsvars",
    knownLots: "Zināmie loti",
    avgLots: "Vid. loti/projekts",
    dataQuality: "Datu kvalitāte"
  };

  const { overview, market_concentration, yearly_series, top_winners, top_buyers, top_regions, bidder_leaderboard, biggest_winners, biggest_losers, close_losses, buyer_concentration, multi_lot: ml, data_quality } = data;

  return (
    <div className="space-y-6">
      <FilterBar
        filters={data.filters}
        year={year} region={region} category={category} multiLot={multiLot}
        onYear={setYear} onRegion={setRegion} onCategory={setCategory} onMultiLot={setMultiLot}
        onReset={reset}
      />

      {loading ? <div className="py-8 text-center text-[13px] text-muted-foreground/60">{t("dashboard.updating")}</div> : null}

      <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
        <StatCard label={t("dashboard.statLabels.projects")} value={overview.projects_in_scope} />
        <StatCard label={t("dashboard.statLabels.totalAwarded")} value={formatCurrency(overview.total_awarded_sum_eur, locale)} />
        <StatCard label={t("dashboard.statLabels.winners")} value={overview.winners_count} />
        <StatCard label={t("dashboard.statLabels.buyers")} value={overview.buyers_count} />
        <StatCard label={t("dashboard.statLabels.averageBidders")} value={overview.average_bidders ?? "—"} />
        <StatCard label={t("dashboard.statLabels.singleBidder")} value={pct(overview.single_bidder_share_pct)} />
        <StatCard label={t("dashboard.statLabels.multiLot")} value={pct(overview.multi_lot_share_pct)} />
        <StatCard label={t("dashboard.statLabels.top5Share")} value={pct(market_concentration.top5_awarded_share_pct)} sub={market_concentration.hhi != null ? `HHI: ${market_concentration.hhi}` : undefined} />
      </div>

      <div className="rounded-2xl border border-border/40 bg-card p-5 shadow-card">
        <h3 className="mb-3 text-[15px] font-semibold text-foreground">{labels.years}</h3>
        <div className="overflow-x-auto">
          <table className="w-full text-[13px]">
            <thead>
              <tr className="border-b border-border/40 text-left text-muted-foreground/60">
                <th className="pb-2 font-medium">{labels.year}</th>
                <th className="pb-2 font-medium text-right">{labels.projects}</th>
                <th className="pb-2 font-medium text-right">{labels.awarded}</th>
                <th className="pb-2 font-medium text-right">{labels.estimated}</th>
                <th className="pb-2 font-medium text-right">{labels.multiLot}</th>
              </tr>
            </thead>
            <tbody>
              {yearly_series.map((series) => (
                <tr key={series.year} className="border-b border-border/20 hover:bg-secondary/30">
                  <td className="py-1.5 font-semibold">{series.year}</td>
                  <td className="py-1.5 text-right tabular-nums">{series.projects}</td>
                  <td className="py-1.5 text-right tabular-nums">{formatCurrency(series.awarded_sum_eur, locale)}</td>
                  <td className="py-1.5 text-right tabular-nums">{formatCurrency(series.estimated_sum_eur, locale)}</td>
                  <td className="py-1.5 text-right tabular-nums">{pct(series.multi_lot_share_pct)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
        <RankTable title={labels.topWinners} description={labels.topWinnersDesc} rows={top_winners} />
        <RankTable title={labels.topBuyers} description={labels.topBuyersDesc} rows={top_buyers} />
        <RankTable title={labels.regions} description={labels.regionsDesc} rows={top_regions} />
      </div>

      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
        <RankTable
          title={labels.bidders}
          description={labels.biddersDesc}
          rows={bidder_leaderboard}
          getValue={(row) => `${row["applications"]} ${t("dashboard.tableLabels.applicationsShort")} · ${row["wins"]} ${t("dashboard.tableLabels.winsShort")}`}
        />
        <RankTable
          title={labels.biggestWinners}
          description={labels.biggestWinnersDesc}
          rows={biggest_winners}
          getValue={(row) => `${row["win_rate_pct"]}% (${row["wins"]}/${row["applications"]})`}
        />
      </div>

      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
        <RankTable
          title={labels.biggestLosers}
          description={labels.biggestLosersDesc}
          rows={biggest_losers}
          getValue={(row) => `${row["applications"]} ${t("dashboard.tableLabels.applicationsShort")}`}
        />
        <RankTable
          title={labels.closeLosses}
          description={labels.closeLossesDesc}
          rows={close_losses}
          getValue={(row) => `${row["close_losses_3pct"]}× ≤3%`}
        />
      </div>

      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
        <RankTable
          title={labels.buyerConcentration}
          description={labels.buyerConcentrationDesc}
          rows={buyer_concentration}
          getValue={(row) => `${row["top_winner_share_pct"]}% → ${row["top_winner"]}`}
        />
        <div className="rounded-2xl border border-border/40 bg-card p-5 shadow-card">
          <h3 className="mb-3 text-[15px] font-semibold text-foreground">{labels.multiLotTitle}</h3>
          <div className="space-y-2 text-[13px]">
            <div className="flex justify-between"><span className="text-muted-foreground">{labels.projects}</span><span className="font-semibold tabular-nums">{ml.projects}</span></div>
            <div className="flex justify-between"><span className="text-muted-foreground">{labels.share}</span><span className="font-semibold tabular-nums">{pct(ml.share_pct)}</span></div>
            <div className="flex justify-between"><span className="text-muted-foreground">{labels.knownLots}</span><span className="font-semibold tabular-nums">{ml.known_total_lots}</span></div>
            <div className="flex justify-between"><span className="text-muted-foreground">{labels.avgLots}</span><span className="font-semibold tabular-nums">{ml.average_lots_per_multi_lot_project ?? "—"}</span></div>
          </div>
        </div>
      </div>

      <div className="rounded-2xl border border-border/40 bg-card p-5 shadow-card">
        <h3 className="mb-3 text-[15px] font-semibold text-foreground">{labels.dataQuality}</h3>
        <div className="grid grid-cols-2 gap-2 sm:grid-cols-4">
          {data_quality.map((item) => (
            <div key={item.field} className="rounded-xl bg-secondary/40 p-3">
              <p className="text-[11px] text-muted-foreground/70">{item.label}</p>
              <p className="mt-1 text-[18px] font-bold tabular-nums text-foreground">{item.coverage_pct ?? "—"}%</p>
              <p className="text-[11px] text-muted-foreground/50">{item.missing_count} {t("dashboard.tableLabels.missing")}</p>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
