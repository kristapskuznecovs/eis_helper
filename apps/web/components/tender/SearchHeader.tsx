import { Search } from "lucide-react";
import { useTranslations } from "next-intl";

const SearchHeader = () => {
  const t = useTranslations("search");

  return (
    <header className="space-y-3 pb-2 text-center">
      <div className="inline-flex items-center gap-3">
        <div className="rounded-2xl bg-primary/10 p-2.5 shadow-card">
          <Search className="h-5 w-5 text-primary" strokeWidth={2.5} />
        </div>
        <h1 className="text-[28px] font-bold tracking-tight text-foreground">{t("title")}</h1>
      </div>
      <p className="mx-auto max-w-lg text-[15px] leading-relaxed text-muted-foreground">
        {t("subtitle")}
        <span className="hidden text-muted-foreground/70 sm:inline"> {t("subtitleExtra")}</span>
      </p>
    </header>
  );
};

export default SearchHeader;
