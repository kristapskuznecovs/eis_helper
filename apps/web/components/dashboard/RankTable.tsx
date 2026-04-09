import { useLocale, useTranslations } from "next-intl";
import { formatCurrency } from "@/lib/format";

interface BaseRankRow {
  name: string;
  project_count?: number;
  awarded_sum_eur?: number;
  count?: number;
}

interface RankTableProps<T extends BaseRankRow> {
  title: string;
  description?: string;
  rows: T[];
  valueLabel?: string;
  getValue?: (row: T) => string;
}

export function RankTable<T extends BaseRankRow>({ title, description, rows, valueLabel = "Amount", getValue }: RankTableProps<T>) {
  const locale = useLocale();
  const t = useTranslations("dashboard.tableLabels");
  const common = useTranslations("common");

  return (
    <div className="rounded-2xl border border-border/40 bg-card p-5 shadow-card">
      <div className="mb-3">
        <h3 className="text-[15px] font-semibold text-foreground">{title}</h3>
        {description ? <p className="mt-0.5 text-[12px] text-muted-foreground/70">{description}</p> : null}
      </div>
      <div className="space-y-1.5">
        {rows.map((row, i) => (
          <div key={i} className="flex items-center justify-between gap-3 rounded-lg px-2 py-1.5 transition-colors hover:bg-secondary/50">
            <div className="flex min-w-0 items-center gap-2.5">
              <span className="w-4 text-[11px] font-semibold tabular-nums text-muted-foreground/40">{i + 1}</span>
              <span className="truncate text-[13px] text-foreground">{row.name}</span>
            </div>
            <div className="shrink-0 text-right">
              <span className="text-[13px] font-semibold tabular-nums text-foreground">
                {getValue ? getValue(row) : row.awarded_sum_eur != null ? formatCurrency(row.awarded_sum_eur, locale) : row.count ?? "—"}
              </span>
              {row.project_count != null ? (
                <span className="ml-2 text-[11px] text-muted-foreground/50">{row.project_count} {t("projectShort")}</span>
              ) : null}
            </div>
          </div>
        ))}
        {rows.length === 0 ? <p className="py-4 text-center text-[13px] text-muted-foreground/50">{common("noData")}</p> : null}
      </div>
    </div>
  );
}
