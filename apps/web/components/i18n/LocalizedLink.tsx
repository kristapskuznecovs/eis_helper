"use client";

import Link, { type LinkProps } from "next/link";
import { useLocale } from "next-intl";
import type { ComponentPropsWithoutRef, ReactNode } from "react";
import { localizeHref, type AppLocale } from "@/i18n/config";

type AnchorProps = Omit<ComponentPropsWithoutRef<typeof Link>, keyof LinkProps>;

interface LocalizedLinkProps extends Omit<LinkProps, "href">, AnchorProps {
  href: string;
  localeOverride?: AppLocale;
  children: ReactNode;
}

export default function LocalizedLink({ href, localeOverride, children, ...props }: LocalizedLinkProps) {
  const locale = useLocale() as AppLocale;
  return (
    <Link href={localizeHref(localeOverride ?? locale, href)} {...props}>
      {children}
    </Link>
  );
}
