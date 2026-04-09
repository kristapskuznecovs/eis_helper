import { getRequestConfig } from "next-intl/server";
import { headers } from "next/headers";
import { defaultLocale, localeHeaderName } from "@/i18n/config";
import { getMessages } from "@/i18n/messages";

export default getRequestConfig(async () => {
  const headerStore = await headers();
  const requestedLocale = headerStore.get(localeHeaderName) ?? defaultLocale;
  const { locale, messages } = await getMessages(requestedLocale);

  return {
    locale,
    messages,
  };
});
