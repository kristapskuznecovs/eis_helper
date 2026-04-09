export const locales = ["lv", "en"] as const;

export type AppLocale = (typeof locales)[number];

export const defaultLocale: AppLocale = "lv";
export const localeCookieName = "eis_locale";
export const localeHeaderName = "x-resolved-locale";

export function isValidLocale(value: string): value is AppLocale {
  return (locales as readonly string[]).includes(value);
}

export function localizeHref(locale: AppLocale, href: string): string {
  if (!href.startsWith("/")) return href;
  if (href === "/") return `/${locale}`;
  const normalized = href.startsWith(`/${locale}/`) || href === `/${locale}`;
  return normalized ? href : `/${locale}${href}`;
}

export function replaceLocaleInPathname(pathname: string, locale: AppLocale): string {
  const parts = pathname.split("/").filter(Boolean);
  if (parts.length === 0) return `/${locale}`;
  if (isValidLocale(parts[0])) {
    parts[0] = locale;
    return `/${parts.join("/")}`;
  }
  return localizeHref(locale, pathname);
}
