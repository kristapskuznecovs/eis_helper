import { AlertCircle, RefreshCw, SearchX } from "lucide-react";
import { useTranslations } from "next-intl";
import { Button } from "@/components/ui/Button";
import type { TenderResult } from "@/lib/types/tender";
import TenderResultCard from "./TenderResultCard";

interface ResultsSectionProps {
  results: TenderResult[] | null;
  totalCount: number;
  isLoading: boolean;
  hasSearched: boolean;
  error: string | null;
  onRetry: () => void;
  onSuggestion: (action: string) => void;
  isBookmarked?: (id: string) => boolean;
  onToggleBookmark?: (tender: TenderResult) => void;
}

const ResultsSection = ({
  results,
  totalCount,
  isLoading,
  hasSearched,
  error,
  onRetry,
  onSuggestion,
  isBookmarked,
  onToggleBookmark,
}: ResultsSectionProps) => {
  const t = useTranslations("search");
  const common = useTranslations("common");

  if (!hasSearched) {
    return null;
  }

  if (isLoading) {
    return (
      <div className="animate-fade-in py-16 text-center">
        <div className="inline-flex flex-col items-center gap-3">
          <div className="relative">
            <div className="h-10 w-10 animate-spin rounded-full border-2 border-primary/20 border-t-primary" />
          </div>
          <span className="text-[14px] text-muted-foreground/70">{t("searching")}</span>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="animate-fade-in space-y-4 py-16 text-center">
        <div className="inline-flex items-center gap-2 text-[14px] text-destructive/80">
          <AlertCircle className="h-4 w-4" />
          {error}
        </div>
        <Button variant="outline" size="sm" onClick={onRetry} className="rounded-xl">
          <RefreshCw className="mr-1.5 h-3.5 w-3.5" /> {common("retry")}
        </Button>
      </div>
    );
  }

  if (results && results.length === 0) {
    return (
      <div className="animate-fade-in space-y-5 py-16 text-center">
        <SearchX className="mx-auto h-10 w-10 text-muted-foreground/30" />
        <p className="text-[14px] text-muted-foreground">{t("noResults")}</p>
        <div className="flex flex-wrap justify-center gap-2">
          {[
            { key: "remove_region", label: t("suggestions.remove_region") },
            { key: "broaden_category", label: t("suggestions.broaden_category") },
            { key: "keywords_only", label: t("suggestions.keywords_only") },
            { key: "open_advanced", label: t("suggestions.open_advanced") },
          ].map((suggestion) => (
            <Button
              key={suggestion.key}
              variant="outline"
              size="sm"
              onClick={() => onSuggestion(suggestion.key)}
              className="rounded-xl text-[13px]"
            >
              {suggestion.label}
            </Button>
          ))}
        </div>
      </div>
    );
  }

  if (!results) {
    return null;
  }

  return (
    <div className="animate-fade-in space-y-4">
      <div className="flex items-center justify-between">
        <p className="text-[14px] text-muted-foreground">
          <span className="tabular-nums font-semibold text-foreground">{totalCount}</span>{" "}
          {t("resultsFound", { count: totalCount })}
        </p>
      </div>
      <div className="space-y-3">
        {results.map((result, index) => (
          <div key={result.procurement_id} style={{ animationDelay: `${index * 50}ms` }}>
            <TenderResultCard
              result={result}
              isBookmarked={isBookmarked?.(result.procurement_id)}
              onToggleBookmark={onToggleBookmark}
            />
          </div>
        ))}
      </div>
    </div>
  );
};

export default ResultsSection;
