import type { AbstractIntlMessages } from "next-intl";
import { defaultLocale, isValidLocale, type AppLocale } from "@/i18n/config";

export async function getMessages(locale: string): Promise<{ locale: AppLocale; messages: AbstractIntlMessages }> {
  const resolved = isValidLocale(locale) ? locale : defaultLocale;
  const messages = (await import(`@/locales/${resolved}.json`)).default as AbstractIntlMessages;
  return { locale: resolved, messages };
}
