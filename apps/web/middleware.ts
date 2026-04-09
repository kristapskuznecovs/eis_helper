import { NextResponse, type NextRequest } from "next/server";
import { defaultLocale, isValidLocale, localeCookieName, localeHeaderName, localizeHref, locales } from "@/i18n/config";

function detectLocale(request: NextRequest) {
  const cookieLocale = request.cookies.get(localeCookieName)?.value;
  if (cookieLocale && isValidLocale(cookieLocale)) {
    return cookieLocale;
  }

  const acceptLanguage = request.headers.get("accept-language") ?? "";
  for (const part of acceptLanguage.split(",")) {
    const language = part.trim().split(";")[0]?.slice(0, 2).toLowerCase();
    if (language && isValidLocale(language)) {
      return language;
    }
  }

  return defaultLocale;
}

export function middleware(request: NextRequest) {
  const { pathname } = request.nextUrl;
  const segments = pathname.split("/").filter(Boolean);
  const pathLocale = segments[0];
  const resolvedLocale = isValidLocale(pathLocale) ? pathLocale : detectLocale(request);

  if (!isValidLocale(pathLocale)) {
    const redirectUrl = request.nextUrl.clone();
    redirectUrl.pathname = localizeHref(resolvedLocale, pathname);
    const response = NextResponse.redirect(redirectUrl);
    response.cookies.set(localeCookieName, resolvedLocale, { path: "/", maxAge: 60 * 60 * 24 * 365, sameSite: "lax" });
    return response;
  }

  const requestHeaders = new Headers(request.headers);
  requestHeaders.set(localeHeaderName, resolvedLocale);

  const response = NextResponse.next({
    request: {
      headers: requestHeaders,
    },
  });
  response.cookies.set(localeCookieName, resolvedLocale, { path: "/", maxAge: 60 * 60 * 24 * 365, sameSite: "lax" });
  return response;
}

export const config = {
  matcher: ["/((?!api|_next|.*\\..*).*)"],
};
