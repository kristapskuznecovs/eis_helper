import { Bookmark, Building2, Calendar, Clock, ExternalLink, MapPin, Tag } from "lucide-react";
import { useLocale, useTranslations } from "next-intl";
import { formatCurrency, formatDate } from "@/lib/format";
import type { TenderResult } from "@/lib/types/tender";

interface TenderResultCardProps {
  result: TenderResult;
  isBookmarked?: boolean;
  onToggleBookmark?: (tender: TenderResult) => void;
}

const OPEN_STATUSES = new Set(["Open", "Izsludināts", "Pieteikumi/piedāvājumi atvērti"]);

const TenderResultCard = ({ result, isBookmarked, onToggleBookmark }: TenderResultCardProps) => {
  const locale = useLocale();
  const t = useTranslations("search");
  const common = useTranslations("common");
  const isOpen = OPEN_STATUSES.has(result.status);

  return (
    <div className="group animate-slide-up rounded-2xl border border-border/40 bg-card p-5 shadow-card transition-all duration-300 hover:border-primary/20 hover:shadow-elevated">
      <div className="flex items-start justify-between gap-4">
        <div className="min-w-0 flex-1 space-y-2.5">
          <div className="flex flex-wrap items-center gap-2.5">
            <span className={`inline-flex items-center gap-1 rounded-full px-2.5 py-1 text-[11px] font-semibold uppercase tracking-wide ${isOpen ? "bg-match-bg text-match" : "bg-secondary text-muted-foreground"}`}>
              <span className={`h-1.5 w-1.5 rounded-full ${isOpen ? "bg-match" : "bg-muted-foreground/50"}`} />
              {result.status}
            </span>
            <span className="font-mono text-[12px] text-muted-foreground/60">{result.procurement_id}</span>
          </div>

          <h3 className="text-[15px] font-semibold leading-snug tracking-tight text-foreground">{result.title}</h3>

          <div className="flex flex-wrap items-center gap-x-4 gap-y-1.5 text-[13px] text-muted-foreground">
            <span className="inline-flex items-center gap-1.5">
              <Building2 className="h-3.5 w-3.5 text-muted-foreground/50" /> {result.buyer}
            </span>
            <span className="inline-flex items-center gap-1.5">
              <MapPin className="h-3.5 w-3.5 text-muted-foreground/50" /> {result.region}
            </span>
            <span className="inline-flex items-center gap-1.5 font-mono text-[12px]">
              <Tag className="h-3.5 w-3.5 text-muted-foreground/50" /> {result.cpv_main}
            </span>
          </div>

          <div className="flex flex-wrap items-center gap-x-4 gap-y-1.5 text-[13px] text-muted-foreground/80">
            <span className="inline-flex items-center gap-1.5">
              <Calendar className="h-3.5 w-3.5 text-muted-foreground/40" /> {formatDate(result.publication_date, locale)}
            </span>
            <span className="inline-flex items-center gap-1.5">
              <Clock className="h-3.5 w-3.5 text-muted-foreground/40" /> {formatDate(result.submission_deadline, locale)}
            </span>
            {result.estimated_value_eur ? (
              <span className="tabular-nums font-semibold text-foreground">{formatCurrency(result.estimated_value_eur, locale)}</span>
            ) : null}
            {result.procedure_type ? <span className="text-[12px] text-muted-foreground/50">{result.procedure_type}</span> : null}
          </div>

          <p className="text-[12px] leading-relaxed text-accent-foreground/60">{t("match", { reason: result.match_reason })}</p>
        </div>

        <div className="flex shrink-0 flex-col gap-1.5 pt-0.5">
          {onToggleBookmark ? (
            <button
              type="button"
              onClick={() => onToggleBookmark(result)}
              className={`rounded-xl p-2 transition-all duration-200 ${isBookmarked ? "bg-accent text-primary shadow-card" : "text-muted-foreground/40 hover:bg-accent hover:text-primary"}`}
              title={isBookmarked ? t("bookmarks.remove") : t("bookmarks.add")}
            >
              <Bookmark className="h-4 w-4" fill={isBookmarked ? "currentColor" : "none"} strokeWidth={2} />
            </button>
          ) : null}
          <a
            href={result.eis_url}
            target="_blank"
            rel="noopener noreferrer"
            className="rounded-xl p-2 text-muted-foreground/40 transition-all duration-200 hover:bg-accent hover:text-primary"
            title={common("openInEis")}
          >
            <ExternalLink className="h-4 w-4" strokeWidth={2} />
          </a>
        </div>
      </div>
    </div>
  );
};

export default TenderResultCard;
