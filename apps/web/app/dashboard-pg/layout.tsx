"use client";

import type { ReactNode } from "react";
import { useTranslations } from "next-intl";
import LanguageSwitcher from "@/components/i18n/LanguageSwitcher";
import LocalizedLink from "@/components/i18n/LocalizedLink";

export default function DashboardPgLayout({ children }: { children: ReactNode }) {
  const t = useTranslations();
  const navLinks = [
    { href: "/dashboard-pg", label: t("nav.overview") },
    { href: "/dashboard-pg/company", label: t("nav.company") },
    { href: "/dashboard-pg/purchaser", label: t("nav.purchaser") },
    { href: "/dashboard-pg/risk", label: t("nav.risk") },
  ];

  return (
    <div className="min-h-screen bg-background">
      <header className="glass sticky top-0 z-50 border-b border-border/40">
        <div className="mx-auto flex h-14 max-w-6xl items-center justify-between gap-6 px-4">
          <div className="flex items-center gap-3">
            <LanguageSwitcher />
            <span className="text-[15px] font-semibold tracking-tight text-foreground">{t("dashboard.headerCkan")}</span>
          </div>
          <nav className="flex items-center gap-1">
            {navLinks.map((link) => (
              <LocalizedLink
                key={link.href}
                href={link.href}
                className="rounded-lg px-3 py-1.5 text-[13px] font-medium text-muted-foreground transition-colors hover:bg-secondary hover:text-foreground"
              >
                {link.label}
              </LocalizedLink>
            ))}
            <LocalizedLink
              href="/dashboard"
              className="ml-3 rounded-lg px-3 py-1.5 text-[13px] font-medium text-muted-foreground transition-colors hover:bg-secondary hover:text-foreground"
            >
              ← {t("dashboard.sqliteLink")}
            </LocalizedLink>
            <LocalizedLink
              href="/"
              className="ml-1 rounded-lg px-3 py-1.5 text-[13px] font-medium text-muted-foreground transition-colors hover:bg-secondary hover:text-foreground"
            >
              ← {t("nav.tenders")}
            </LocalizedLink>
          </nav>
        </div>
      </header>
      <main className="mx-auto max-w-6xl px-4 py-8">{children}</main>
    </div>
  );
}
