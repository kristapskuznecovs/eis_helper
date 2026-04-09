import { defaultLocale } from "@/i18n/config";

export function getApiBaseUrl(): string {
  if (typeof window === "undefined") {
    // Server-side: call backend directly
    return process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";
  }
  // Client-side: use Next.js proxy rewrite (same origin)
  return "";
}

export function getLocaleHeaders(locale: string = defaultLocale): Record<string, string> {
  return {
    "Accept-Language": locale,
    "X-Locale": locale,
  };
}
