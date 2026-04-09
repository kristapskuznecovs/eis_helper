import { defaultLocale, type AppLocale } from "@/i18n/config";

export function getIntlLocale(locale: string): string {
  return locale === "en" ? "en-GB" : "lv-LV";
}

export function formatCurrency(value: number, locale: string = defaultLocale): string {
  return new Intl.NumberFormat(getIntlLocale(locale), {
    style: "currency",
    currency: "EUR",
    maximumFractionDigits: 0,
  }).format(value);
}

export function formatDate(value: string, locale: string = defaultLocale): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }

  return date.toLocaleDateString(getIntlLocale(locale), {
    day: "numeric",
    month: "short",
    year: "numeric",
  });
}

export function asAppLocale(locale: string): AppLocale {
  return locale === "en" ? "en" : "lv";
}
