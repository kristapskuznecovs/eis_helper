import "./globals.css";
import type { Metadata } from "next";
import { headers } from "next/headers";
import { NextIntlClientProvider } from "next-intl";
import type { ReactNode } from "react";
import { defaultLocale, localeHeaderName } from "@/i18n/config";
import { getMessages } from "@/i18n/messages";

export const metadata: Metadata = {
  title: "EIS Tenders",
  description: "Find relevant public tenders from EIS",
};

export default async function RootLayout({ children }: { children: ReactNode }) {
  const headerStore = await headers();
  const requestedLocale = headerStore.get(localeHeaderName) ?? defaultLocale;
  const { locale, messages } = await getMessages(requestedLocale);

  return (
    <html lang={locale}>
      <body>
        <NextIntlClientProvider locale={locale} messages={messages}>
          {children}
        </NextIntlClientProvider>
      </body>
    </html>
  );
}
