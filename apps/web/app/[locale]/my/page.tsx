"use client";

import { Suspense, useEffect, useRef, useState } from "react";
import { Activity, Bookmark, ChevronRight, SlidersHorizontal, User } from "lucide-react";
import { useRouter } from "next/navigation";
import { useLocale, useTranslations } from "next-intl";
import LanguageSwitcher from "@/components/i18n/LanguageSwitcher";
import LocalizedLink from "@/components/i18n/LocalizedLink";
import { localizeHref } from "@/i18n/config";
import type { AppLocale } from "@/i18n/config";
import { ActivityCard } from "@/components/workspace/ActivityCard";
import { SavedFilterCard } from "@/components/workspace/SavedFilterCard";
import { StatsBar } from "@/components/workspace/StatsBar";
import BookmarkedCard from "@/components/tender/BookmarkedCard";
import { fetchMyActivity } from "@/lib/api/activity";
import { resolveCompanyCpv, suggestCompanies } from "@/lib/api/chat";
import { useBookmarks } from "@/lib/hooks/useBookmarks";
import { useFilterNewCounts } from "@/lib/hooks/useFilterNewCounts";
import { useMyCompany } from "@/lib/hooks/useMyCompany";
import { useSavedFilters } from "@/lib/hooks/useSavedFilters";
import type { MyActivityResponse, SavedFilter } from "@/lib/types/tender";

type Tab = "profile" | "activity" | "filters" | "bookmarks";

function parseActivityDate(value?: string | null): number {
  if (!value) return Number.NEGATIVE_INFINITY;

  const iso = value.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (iso) return new Date(Number(iso[1]), Number(iso[2]) - 1, Number(iso[3])).getTime();

  const lv = value.match(/^(\d{2})\.(\d{2})\.(\d{4})$/);
  if (lv) return new Date(Number(lv[3]), Number(lv[2]) - 1, Number(lv[1])).getTime();

  const parsed = new Date(value).getTime();
  return Number.isNaN(parsed) ? Number.NEGATIVE_INFINITY : parsed;
}

export default function MyWorkspacePage() {
  const t = useTranslations("my");
  const locale = useLocale() as AppLocale;
  const router = useRouter();

  const TABS: { id: Tab; label: string; icon: React.ElementType }[] = [
    { id: "profile", label: t("tabs.profile"), icon: User },
    { id: "activity", label: t("tabs.activity"), icon: Activity },
    { id: "filters", label: t("tabs.filters"), icon: SlidersHorizontal },
    { id: "bookmarks", label: t("tabs.bookmarks"), icon: Bookmark },
  ];
  const [tab, setTab] = useState<Tab>("profile");

  const { company, setCompany } = useMyCompany();
  const { filters: savedFilters, remove: removeFilter } = useSavedFilters();
  const { counts: filterNewCounts } = useFilterNewCounts(savedFilters, locale);
  const { bookmarks, remove: removeBookmark, clear: clearBookmarks } = useBookmarks();

  // Profile tab state
  const [draft, setDraft] = useState("");
  const [verifyState, setVerifyState] = useState<"idle" | "loading" | "found" | "notfound">("idle");
  const [matchCount, setMatchCount] = useState(0);
  const companyInputRef = useRef<HTMLInputElement>(null);
  const [suggestions, setSuggestions] = useState<{ name: string; reg_number: string | null }[]>([]);
  const [showSuggestions, setShowSuggestions] = useState(false);
  const suggestTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const draftRegNumber = useRef<string | null>(null);

  // Activity tab state
  const [activity, setActivity] = useState<MyActivityResponse | null>(null);
  const [activityLoading, setActivityLoading] = useState(false);
  const [activityError, setActivityError] = useState<string | null>(null);
  const sortedWins = [...(activity?.wins ?? [])].sort(
    (a, b) => parseActivityDate(b.signed_date) - parseActivityDate(a.signed_date),
  );
  const sortedParticipations = [...(activity?.participations ?? [])].sort(
    (a, b) => parseActivityDate(b.submission_deadline) - parseActivityDate(a.submission_deadline),
  );

  // Sync draft when company loads from localStorage
  useEffect(() => {
    if (company && !draft) {
      setDraft(company.name);
      draftRegNumber.current = company.reg_number;
    }
  }, [company]); // eslint-disable-line react-hooks/exhaustive-deps

  // Load activity when switching to activity tab
  useEffect(() => {
    if (tab !== "activity" || !company) return;
    if (activity?.company === company.name) return;
    setActivityLoading(true);
    setActivityError(null);
    fetchMyActivity(company.name, company.reg_number)
      .then(setActivity)
      .catch(() => setActivityError(t("activity.error")))
      .finally(() => setActivityLoading(false));
  }, [tab, company]); // eslint-disable-line react-hooks/exhaustive-deps

  const handleVerify = async () => {
    const name = draft.trim();
    if (!name) return;
    setVerifyState("loading");
    try {
      const result = await resolveCompanyCpv(name, locale, draftRegNumber.current);
      setMatchCount(result.match_count);
      setVerifyState(result.match_count > 0 ? "found" : "notfound");
    } catch {
      setVerifyState("notfound");
    }
  };

  const handleSaveCompany = () => {
    setCompany(draft.trim(), draftRegNumber.current);
    setVerifyState("idle");
  };

  const handleRunFilter = (filter: SavedFilter) => {
    sessionStorage.setItem("eis_run_filter", JSON.stringify(filter.filters));
    sessionStorage.setItem("eis_run_filter_id", filter.id);
    router.push(localizeHref(locale, "/"));
  };

  const handleEditFilter = (filter: SavedFilter) => {
    sessionStorage.setItem("eis_edit_filter", JSON.stringify(filter));
    router.push(localizeHref(locale, "/"));
  };

  return (
    <div className="min-h-screen bg-background">
      <nav className="glass sticky top-0 z-50 border-b border-border/40">
        <div className="mx-auto flex h-14 max-w-3xl items-center justify-between px-4">
          <LocalizedLink
            href="/"
            className="inline-flex items-center gap-1.5 text-[13px] font-medium text-muted-foreground transition-colors hover:text-foreground"
          >
            {t("back")}
          </LocalizedLink>
          <span className="text-[15px] font-semibold tracking-tight text-foreground">{t("title")}</span>
          <Suspense fallback={<div className="w-16" />}>
            <LanguageSwitcher />
          </Suspense>
        </div>
      </nav>

      <div className="mx-auto max-w-3xl px-4 py-8">
        {/* Tabs */}
        <div className="mb-6 flex gap-1 rounded-2xl bg-secondary/50 p-1">
          {TABS.map(({ id, label, icon: Icon }) => (
            <button
              key={id}
              type="button"
              onClick={() => setTab(id)}
              className={`flex flex-1 items-center justify-center gap-1.5 rounded-xl py-2 text-[13px] font-medium transition-all duration-200 ${
                tab === id
                  ? "bg-card text-foreground shadow-card"
                  : "text-muted-foreground/60 hover:text-foreground"
              }`}
            >
              <Icon className="h-3.5 w-3.5" />
              <span className="hidden sm:inline">{label}</span>
            </button>
          ))}
        </div>

        {/* Profile Tab */}
        {tab === "profile" && (
          <div className="space-y-4">
            <div>
              <h2 className="text-[18px] font-bold text-foreground">{t("profile.heading")}</h2>
              <p className="mt-1 text-[13px] text-muted-foreground/70">
                {t("profile.description")}
              </p>
            </div>
            <div className="rounded-2xl border border-border/50 bg-card p-5 shadow-card space-y-4">
              <div className="flex flex-col gap-3 sm:flex-row sm:items-end">
                <div className="relative flex-1 space-y-1.5">
                  <label className="text-[12px] font-medium text-muted-foreground/70">{t("profile.label")}</label>
                  <input
                    ref={companyInputRef}
                    value={draft}
                    onChange={(e) => {
                      const val = e.target.value;
                      setDraft(val);
                      draftRegNumber.current = null; // cleared when user types freely
                      setVerifyState("idle");
                      if (suggestTimerRef.current) clearTimeout(suggestTimerRef.current);
                      if (val.trim().length >= 2) {
                        suggestTimerRef.current = setTimeout(async () => {
                          const results = await suggestCompanies(val, locale);
                          setSuggestions(results);
                          setShowSuggestions(results.length > 0);
                        }, 250);
                      } else {
                        setSuggestions([]);
                        setShowSuggestions(false);
                      }
                    }}
                    onKeyDown={(e) => {
                      if (e.key === "Enter") { setShowSuggestions(false); handleVerify(); }
                      if (e.key === "Escape") setShowSuggestions(false);
                    }}
                    onBlur={() => setTimeout(() => setShowSuggestions(false), 150)}
                    onFocus={() => suggestions.length > 0 && setShowSuggestions(true)}
                    placeholder={t("profile.placeholder")}
                    className="h-9 w-full rounded-lg border-0 bg-secondary/60 px-3 text-[14px] text-foreground placeholder:text-muted-foreground/40 focus:bg-background focus:outline-none focus:ring-2 focus:ring-primary/30"
                  />
                  {showSuggestions && (
                    <ul className="absolute left-0 right-0 top-full z-50 mt-1 overflow-hidden rounded-xl border border-border/50 bg-card shadow-lg">
                      {suggestions.map((s) => (
                        <li key={s.name}>
                          <button
                            type="button"
                            onMouseDown={() => {
                              setDraft(s.name);
                              draftRegNumber.current = s.reg_number;
                              setSuggestions([]);
                              setShowSuggestions(false);
                              setVerifyState("idle");
                            }}
                            className="flex w-full items-center justify-between gap-3 px-3 py-2 text-left transition-colors hover:bg-secondary/60"
                          >
                            <span className="truncate text-[13px] text-foreground">{s.name}</span>
                            {s.reg_number && (
                              <span className="shrink-0 font-mono text-[11px] text-muted-foreground/50">{s.reg_number}</span>
                            )}
                          </button>
                        </li>
                      ))}
                    </ul>
                  )}
                </div>
                <div className="flex gap-2">
                  <button
                    type="button"
                    onClick={handleVerify}
                    disabled={!draft.trim() || verifyState === "loading"}
                    className="h-9 rounded-lg border border-border/60 bg-card px-4 text-[13px] font-medium text-foreground/70 transition-colors hover:border-primary/30 hover:text-primary disabled:opacity-40"
                  >
                    {verifyState === "loading" ? t("profile.checking") : t("profile.verify")}
                  </button>
                  {draft.trim() !== company?.name && draft.trim() && (
                    <button
                      type="button"
                      onClick={handleSaveCompany}
                      className="h-9 rounded-lg bg-primary px-4 text-[13px] font-medium text-primary-foreground transition-colors hover:bg-primary/90"
                    >
                      {t("profile.save")}
                    </button>
                  )}
                </div>
              </div>

              {verifyState === "found" && (
                <p className="text-[12px] font-medium text-match">
                  {t("profile.found", { count: matchCount })}
                </p>
              )}
              {verifyState === "notfound" && (
                <p className="text-[12px] font-medium text-amber-500">
                  {t("profile.notFound")}
                </p>
              )}

              {company && (
                <div className="flex items-center justify-between rounded-xl bg-secondary/50 px-4 py-3">
                  <div>
                    <p className="text-[12px] text-muted-foreground/60">{t("profile.currentlySet")}</p>
                    <p className="text-[14px] font-semibold text-foreground">{company.name}</p>
                    {company.reg_number && (
                      <p className="font-mono text-[11px] text-muted-foreground/50">{company.reg_number}</p>
                    )}
                  </div>
                  <button
                    type="button"
                    onClick={() => setTab("activity")}
                    className="inline-flex items-center gap-1 text-[12px] font-medium text-primary transition-colors hover:text-primary/80"
                  >
                    {t("profile.viewActivity")} <ChevronRight className="h-3.5 w-3.5" />
                  </button>
                </div>
              )}
            </div>
          </div>
        )}

        {/* Activity Tab */}
        {tab === "activity" && (
          <div className="space-y-5">
            <div>
              <h2 className="text-[18px] font-bold text-foreground">{t("activity.heading")}</h2>
              <p className="mt-1 text-[13px] text-muted-foreground/70">
                {t("activity.description")}
              </p>
            </div>

            {!company ? (
              <div className="rounded-2xl border border-border/40 bg-card p-8 text-center shadow-card">
                <p className="text-[14px] text-muted-foreground/60">
                  {t.rich("activity.noCompany", {
                    link: (chunks) => (
                      <button
                        type="button"
                        onClick={() => setTab("profile")}
                        className="font-medium text-primary hover:text-primary/80"
                      >
                        {chunks}
                      </button>
                    ),
                  })}
                </p>
              </div>
            ) : activityLoading ? (
              <div className="rounded-2xl border border-border/40 bg-card p-8 text-center shadow-card">
                <p className="text-[13px] text-muted-foreground/50">{t("activity.loading", { company: company?.name ?? "" })}</p>
              </div>
            ) : activityError ? (
              <div className="rounded-2xl border border-border/40 bg-card p-8 text-center shadow-card">
                <p className="text-[13px] text-red-500">{activityError}</p>
              </div>
            ) : activity ? (
              <>
                <StatsBar stats={activity.stats} />

                <section className="space-y-2">
                  <h3 className="flex items-center gap-2 text-[14px] font-semibold text-foreground">
                    {t("activity.wins")}
                    <span className="rounded-full bg-match-bg px-2 py-0.5 text-[11px] font-medium text-match">
                      {activity.stats.total_wins}
                    </span>
                  </h3>
                  {activity.wins.length === 0 ? (
                    <p className="text-[13px] text-muted-foreground/50">{t("activity.noWins")}</p>
                  ) : (
                    <div className="space-y-2">
                      {sortedWins.map((item) => (
                        <ActivityCard key={item.procurement_id} item={item} variant="win" />
                      ))}
                    </div>
                  )}
                </section>

                <section className="space-y-2">
                  <h3 className="flex items-center gap-2 text-[14px] font-semibold text-foreground">
                    {t("activity.participations")}
                    <span className="rounded-full bg-secondary px-2 py-0.5 text-[11px] font-medium text-muted-foreground">
                      {activity.stats.total_participations}
                    </span>
                  </h3>
                  {activity.participations.length === 0 ? (
                    <p className="text-[13px] text-muted-foreground/50">{t("activity.noParticipations")}</p>
                  ) : (
                    <div className="space-y-2">
                      {sortedParticipations.map((item) => (
                        <ActivityCard key={item.procurement_id} item={item} variant="participation" />
                      ))}
                    </div>
                  )}
                </section>
              </>
            ) : null}
          </div>
        )}

        {/* Saved Filters Tab */}
        {tab === "filters" && (
          <div className="space-y-4">
            <div>
              <h2 className="text-[18px] font-bold text-foreground">{t("tabs.filters")}</h2>
              <p className="mt-1 text-[13px] text-muted-foreground/70">
                {t("filters.description")}
              </p>
            </div>

            {savedFilters.length === 0 ? (
              <div className="rounded-2xl border border-border/40 bg-card p-8 text-center shadow-card">
                <p className="text-[14px] text-muted-foreground/60">{t("filters.empty")}</p>
                <LocalizedLink
                  href="/"
                  className="mt-3 inline-flex items-center gap-1 text-[13px] font-medium text-primary hover:text-primary/80"
                >
                  {t("filters.goToSearch")} <ChevronRight className="h-3.5 w-3.5" />
                </LocalizedLink>
              </div>
            ) : (
              <div className="space-y-2">
                {[...savedFilters]
                  .sort((a, b) => (filterNewCounts[b.id] ?? 0) - (filterNewCounts[a.id] ?? 0))
                  .map((filter) => (
                    <SavedFilterCard
                      key={filter.id}
                      filter={filter}
                      newCount={filterNewCounts[filter.id] ?? 0}
                      onRun={() => handleRunFilter(filter)}
                      onEdit={() => handleEditFilter(filter)}
                      onDelete={() => removeFilter(filter.id)}
                    />
                  ))}
              </div>
            )}
          </div>
        )}

        {/* Bookmarks Tab */}
        {tab === "bookmarks" && (
          <div className="space-y-4">
            <div className="flex items-center justify-between">
              <div>
                <h2 className="text-[18px] font-bold text-foreground">{t("tabs.bookmarks")}</h2>
                <p className="mt-1 text-[13px] text-muted-foreground/70">
                  {t("bookmarks.description")}
                </p>
              </div>
              {bookmarks.length > 0 && (
                <button
                  type="button"
                  onClick={clearBookmarks}
                  className="text-[12px] font-medium text-muted-foreground/60 transition-colors hover:text-foreground"
                >
                  {t("bookmarks.clearAll")}
                </button>
              )}
            </div>

            {bookmarks.length === 0 ? (
              <div className="rounded-2xl border border-border/40 bg-card p-8 text-center shadow-card">
                <p className="text-[14px] text-muted-foreground/60">{t("bookmarks.empty")}</p>
                <LocalizedLink
                  href="/"
                  className="mt-3 inline-flex items-center gap-1 text-[13px] font-medium text-primary hover:text-primary/80"
                >
                  {t("bookmarks.findTenders")} <ChevronRight className="h-3.5 w-3.5" />
                </LocalizedLink>
              </div>
            ) : (
              <div className="space-y-3">
                {bookmarks.map((tender) => (
                  <BookmarkedCard
                    key={tender.procurement_id}
                    result={tender}
                    onRemove={removeBookmark}
                  />
                ))}
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
