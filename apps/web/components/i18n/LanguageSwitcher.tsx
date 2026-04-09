"use client";

import { usePathname, useSearchParams } from "next/navigation";
import { useLocale, useTranslations } from "next-intl";
import { replaceLocaleInPathname, type AppLocale } from "@/i18n/config";

const switchLocale = (locale: AppLocale) => {
  document.cookie = `eis_locale=${locale}; path=/; max-age=31536000; samesite=lax`;
};

export default function LanguageSwitcher() {
  const locale = useLocale() as AppLocale;
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const t = useTranslations("common.language");

  const query = searchParams.toString();

  return (
    <div className="inline-flex items-center gap-1 rounded-full border border-border/50 bg-card/70 p-1">
      {(["lv", "en"] as const).map((nextLocale) => {
        const href = `${replaceLocaleInPathname(pathname || "/", nextLocale)}${query ? `?${query}` : ""}`;
        const active = locale === nextLocale;
        return (
          <button
            key={nextLocale}
            type="button"
            onClick={() => {
              if (active) return;
              switchLocale(nextLocale);
              window.location.assign(href);
            }}
            className={`rounded-full px-2.5 py-1 text-[12px] font-semibold uppercase tracking-wide transition-colors ${
              active ? "bg-primary text-primary-foreground" : "text-muted-foreground hover:bg-secondary hover:text-foreground"
            }`}
            aria-pressed={active}
          >
            {t(nextLocale)}
          </button>
        );
      })}
    </div>
  );
}
