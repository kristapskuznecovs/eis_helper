"use client";

import { Suspense, useCallback, useEffect, useState } from "react";
import { BookmarkPlus } from "lucide-react";
import { useLocale, useTranslations } from "next-intl";
import LanguageSwitcher from "@/components/i18n/LanguageSwitcher";
import LocalizedLink from "@/components/i18n/LocalizedLink";
import ActiveFilters from "@/components/tender/ActiveFilters";
import AdvancedFilters from "@/components/tender/AdvancedFilters";
import ChatThread from "@/components/tender/ChatThread";
import ResultsSection from "@/components/tender/ResultsSection";
import SearchHeader from "@/components/tender/SearchHeader";
import { SaveFilterInline } from "@/components/workspace/SaveFilterInline";
import { postChat, postSearch, resolveCompanyCandidates, resolveCompanyCpv } from "@/lib/api/chat";
import { useBookmarks } from "@/lib/hooks/useBookmarks";
import { useFilterNewCounts } from "@/lib/hooks/useFilterNewCounts";
import { useMyCompany } from "@/lib/hooks/useMyCompany";
import { useSavedFilters } from "@/lib/hooks/useSavedFilters";
import type { ActiveFilter, ChatState, ExtractedFilters, SavedFilter, TenderResult } from "@/lib/types/tender";
import enMessages from "@/locales/en.json";
import lvMessages from "@/locales/lv.json";

const generateId = () => `${Date.now()}-${Math.random().toString(36).slice(2)}`;
type ChatLocale = "lv" | "en";

const CHAT_MESSAGES = {
  en: enMessages.search.chatUi,
  lv: lvMessages.search.chatUi,
} as const;

function getChatMessages(locale: ChatLocale | null | undefined) {
  return CHAT_MESSAGES[locale === "en" ? "en" : "lv"];
}

export default function HomePage() {
  const locale = useLocale();
  const t = useTranslations();
  const { company } = useMyCompany();
  const { filters: savedFilters, save: saveFilter, markSeen } = useSavedFilters();
  const { totalNew } = useFilterNewCounts(savedFilters, locale);
  const [mounted, setMounted] = useState(false);
  const [showSaveFilter, setShowSaveFilter] = useState(false);
  const [savedConfirm, setSavedConfirm] = useState(false);

  const [myCompanyCpvPrefixes, setMyCompanyCpvPrefixes] = useState<string[]>([]);

  useEffect(() => { setMounted(true); }, []);

  useEffect(() => {
    if (!company) { setMyCompanyCpvPrefixes([]); return; }
    resolveCompanyCpv(company.name, locale, company.reg_number)
      .then((profile) => setMyCompanyCpvPrefixes(profile.cpv_prefixes))
      .catch(() => {});
  }, [company, locale]);

  const [chat, setChat] = useState<ChatState>({
    messages: [],
    isWaiting: false,
    isSearching: false,
    extractedFilters: null,
    sessionId: null,
    chatLocale: null,
  });
  const [advancedFilters, setAdvancedFilters] = useState<ExtractedFilters>({});
  const [advancedOpen, setAdvancedOpen] = useState(false);
  const [results, setResults] = useState<TenderResult[] | null>(null);
  const [totalCount, setTotalCount] = useState(0);
  const [error, setError] = useState<string | null>(null);
  const { bookmarks, toggle: toggleBookmark, isBookmarked } = useBookmarks();

  // On mount: check if a filter was sent from /my page via sessionStorage
  useEffect(() => {
    const runRaw = sessionStorage.getItem("eis_run_filter");
    const editRaw = sessionStorage.getItem("eis_edit_filter");
    if (runRaw) {
      sessionStorage.removeItem("eis_run_filter");
      try {
        const filters: ExtractedFilters = JSON.parse(runRaw);
        runSearch(filters, null);
      } catch { /* ignore */ }
    } else if (editRaw) {
      sessionStorage.removeItem("eis_edit_filter");
      try {
        const saved: SavedFilter = JSON.parse(editRaw);
        setAdvancedFilters(saved.filters);
        setAdvancedOpen(true);
      } catch { /* ignore */ }
    }
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  const hasActiveFilters = Object.values(advancedFilters).some(
    (v) => v !== undefined && v !== null && v !== "" && (!Array.isArray(v) || v.length > 0),
  );

  const handleSaveFilter = useCallback((name: string) => {
    saveFilter(name, advancedFilters);
    setShowSaveFilter(false);
    setSavedConfirm(true);
    setTimeout(() => setSavedConfirm(false), 2500);
  }, [advancedFilters, saveFilter]);

  const runSearch = useCallback(async (filters: ExtractedFilters, sessionId: string | null) => {
    setChat((prev) => ({ ...prev, isSearching: true, extractedFilters: filters, sessionId }));
    setAdvancedFilters(filters);
    setError(null);

    try {
      const resp = await postSearch({ filters }, locale);
      setResults(resp.results);
      setTotalCount(resp.total_count);

      // Mark saved filter as seen if this search came from one
      const filterId = sessionStorage.getItem("eis_run_filter_id");
      if (filterId) {
        sessionStorage.removeItem("eis_run_filter_id");
        const maxDate = resp.results
          .map((r) => r.publication_date)
          .filter(Boolean)
          .sort()
          .at(-1);
        if (maxDate) markSeen(filterId, maxDate);
      }
    } catch {
      setError(t("search.loadError"));
    } finally {
      setChat((prev) => ({ ...prev, isSearching: false }));
    }
  }, [locale, markSeen, t]);

  const handleChatMessage = useCallback(async (text: string) => {
    const effectiveChatLocale: ChatLocale = chat.chatLocale ?? (locale === "en" ? "en" : "lv");
    const chatText = getChatMessages(effectiveChatLocale);

    // If we're in a company disambiguation flow, handle the user's pick
    if (chat.pendingCompanyResolve) {
      const { mergedFilters } = chat.pendingCompanyResolve;
      const userMessage = { id: generateId(), role: "user" as const, content: text };
      setChat((prev) => ({
        ...prev,
        messages: [...prev.messages, userMessage],
        isWaiting: true,
        pendingCompanyResolve: null,
      }));

      let updatedFilters = { ...mergedFilters };
      const chosenName = text.trim();
      if (chosenName !== chatText.noneOfThese) {
        try {
          const profile = await resolveCompanyCpv(chosenName, locale);
          if (profile.cpv_prefixes.length > 0) {
            updatedFilters = {
              ...updatedFilters,
              similar_company_cpv_prefixes: Array.from(new Set([
                ...(updatedFilters.similar_company_cpv_prefixes ?? []),
                ...profile.cpv_prefixes,
              ])),
              cpv_prefixes: Array.from(new Set([
                ...(updatedFilters.cpv_prefixes ?? []),
                ...profile.cpv_prefixes,
              ])),
            };
          }
        } catch { /* continue without */ }
      }

      // If there are more companies to disambiguate, show next one
      const remaining = chat.pendingCompanyResolve.remaining;
      if (remaining.length > 0) {
        const nextCompany = remaining[0];
        try {
          const candidates = await resolveCompanyCandidates(nextCompany, locale);
          if (candidates.length > 0) {
            const quickReplies = [...candidates.slice(0, 6).map((c) => c.name), chatText.noneOfThese];
            setChat((prev) => ({
              ...prev,
              messages: [...prev.messages, {
                id: generateId(),
                role: "assistant" as const,
                  content: getChatMessages(prev.chatLocale ?? effectiveChatLocale).disambiguationQuestion.replace("{name}", nextCompany),
                quick_replies: quickReplies,
              }],
              pendingCompanyResolve: { remaining: remaining.slice(1), mergedFilters: updatedFilters },
              isWaiting: false,
            }));
            return;
          }
        } catch { /* fall through */ }
      }

      setChat((prev) => ({ ...prev, isWaiting: false }));
      await runSearch(updatedFilters, chat.sessionId);
      return;
    }

    const userMessage = { id: generateId(), role: "user" as const, content: text };
    const currentMessages = [...chat.messages, userMessage];

    setChat((prev) => ({
      ...prev,
      messages: [...prev.messages, userMessage],
      isWaiting: true,
    }));

    try {
      const response = await postChat(
        {
          messages: currentMessages.map(({ role, content }) => ({ role, content })),
          my_company: company && myCompanyCpvPrefixes.length > 0
            ? { name: company.name, cpv_prefixes: myCompanyCpvPrefixes }
            : undefined,
          chat_locale: chat.chatLocale ?? undefined,
        },
        locale,
        chat.sessionId,
      );

      const sessionId = response.session_id ?? chat.sessionId;
      const responseChatLocale = response.chat_locale ?? chat.chatLocale ?? (locale === "en" ? "en" : "lv");

      if (response.type === "question") {
        const assistantMessage = {
          id: generateId(),
          role: "assistant" as const,
          content: response.message,
          quick_replies: response.quick_replies,
          filter_summary: response.filter_summary,
        };
        setChat((prev) => ({
          ...prev,
          messages: [...prev.messages, assistantMessage],
          isWaiting: false,
          sessionId,
          chatLocale: responseChatLocale,
        }));
      } else {
        const assistantMessage = {
          id: generateId(),
          role: "assistant" as const,
          content: response.message,
        };
        setChat((prev) => ({
          ...prev,
          messages: [...prev.messages, assistantMessage],
          isWaiting: false,
          sessionId,
          chatLocale: responseChatLocale,
        }));

        let mergedFilters = response.filters;
        const similarCos = response.filters.similar_companies ?? [];
        if (similarCos.length > 0) {
          try {
            // Always disambiguate — show candidates for the first company so user confirms the exact entity
            const candidates = await resolveCompanyCandidates(similarCos[0], locale);
            if (candidates.length > 0) {
              const quickReplies = [
                ...candidates.slice(0, 6).map((c) => c.name),
                getChatMessages(responseChatLocale).noneOfThese,
              ];
              setChat((prev) => ({
                ...prev,
                messages: [...prev.messages, {
                  id: generateId(),
                  role: "assistant" as const,
                  content: getChatMessages(responseChatLocale).disambiguationFound.replace("{name}", similarCos[0]),
                  quick_replies: quickReplies,
                }],
                pendingCompanyResolve: { remaining: similarCos.slice(1), mergedFilters },
                chatLocale: responseChatLocale,
              }));
              return; // wait for user to pick
            }
            // No candidates — fall back to direct CPV lookup for all companies
            const profiles = await Promise.all(similarCos.map((c) => resolveCompanyCpv(c, locale)));
            const resolvedPrefixes = Array.from(new Set(profiles.flatMap((p) => p.cpv_prefixes)));
            if (resolvedPrefixes.length > 0) {
              mergedFilters = {
                ...mergedFilters,
                similar_company_cpv_prefixes: resolvedPrefixes,
                cpv_prefixes: Array.from(new Set([...(mergedFilters.cpv_prefixes ?? []), ...resolvedPrefixes])),
              };
            }
          } catch {
            // Continue without CPV enrichment if lookup fails.
          }
        }

        await runSearch(mergedFilters, sessionId);
      }
    } catch {
      const errorMessage = {
        id: generateId(),
        role: "assistant" as const,
        content: getChatMessages(chat.chatLocale ?? (locale === "en" ? "en" : "lv")).assistantError,
      };
      setChat((prev) => ({
        ...prev,
        messages: [...prev.messages, errorMessage],
        isWaiting: false,
      }));
    }
  }, [chat.messages, chat.sessionId, locale, runSearch, t]);

  const handleRetry = useCallback(() => {
    if (chat.extractedFilters) {
      runSearch(chat.extractedFilters, chat.sessionId);
    }
  }, [chat.extractedFilters, chat.sessionId, runSearch]);

  const handleAdvancedSearch = useCallback(async (filters: ExtractedFilters) => {
    await runSearch(filters, chat.sessionId);
  }, [chat.sessionId, runSearch]);

  const derivedActiveFilters: ActiveFilter[] = (() => {
    const latestWithSummary = [...chat.messages].reverse().find(
      (message) => message.role === "assistant" && message.filter_summary,
    );
    if (!latestWithSummary?.filter_summary) return [];
    return Object.entries(latestWithSummary.filter_summary).map(([key, value]) => ({
      key,
      label: t(`search.filters.${key}`),
      value,
    }));
  })();

  const handleRemoveFilter = useCallback((key: string) => {
    const chatText = getChatMessages(chat.chatLocale ?? (locale === "en" ? "en" : "lv"));
    handleChatMessage(chatText.removeFilterIntent.replace("{filter}", key.replace(/_/g, " ")));
  }, [chat.chatLocale, handleChatMessage, locale]);

  const handleSuggestion = useCallback((action: string) => {
    const chatText = getChatMessages(chat.chatLocale ?? (locale === "en" ? "en" : "lv"));
    const messages: Record<string, string> = {
      remove_region: chatText.suggestionRemoveRegion,
      broaden_category: chatText.suggestionBroadenCategory,
      keywords_only: chatText.suggestionKeywordsOnly,
      open_advanced: chatText.suggestionOpenAdvanced,
    };
    const message = messages[action];
    if (message) handleChatMessage(message);
  }, [chat.chatLocale, handleChatMessage, locale]);

  const hasSearched = results !== null || chat.isSearching;

  return (
    <div className="min-h-screen bg-background">
      <nav className="glass sticky top-0 z-50 border-b border-border/40">
        <div className="mx-auto flex h-14 max-w-3xl items-center justify-between px-4">
          <span className="text-[15px] font-semibold tracking-tight text-foreground">{t("search.navTitle")}</span>
          <div className="flex items-center gap-1">
            <LocalizedLink
              href="/dashboard"
              className="inline-flex items-center gap-2 rounded-full px-3 py-1.5 text-[13px] font-medium text-muted-foreground transition-all duration-200 hover:bg-secondary/80 hover:text-foreground"
            >
              {t("nav.dashboard")}
            </LocalizedLink>
            <LocalizedLink
              href="/my"
              className="relative inline-flex items-center gap-2 rounded-full px-3 py-1.5 text-[13px] font-medium text-muted-foreground transition-all duration-200 hover:bg-secondary/80 hover:text-foreground"
            >
              {t("nav.my")}
              {mounted && totalNew > 0 ? (
                <span className="absolute -right-1 -top-1 flex h-4 min-w-4 items-center justify-center rounded-full bg-primary px-1 text-[10px] font-bold text-primary-foreground">
                  {totalNew > 99 ? "99+" : totalNew}
                </span>
              ) : mounted && (company || bookmarks.length > 0) ? (
                <span className="absolute right-1.5 top-1.5 h-1.5 w-1.5 rounded-full bg-primary" />
              ) : null}
            </LocalizedLink>
            <Suspense fallback={null}>
              <LanguageSwitcher />
            </Suspense>
          </div>
        </div>
      </nav>

      {company && (
        <div className="border-b border-border/40 bg-secondary/40">
          <div className="mx-auto flex h-9 max-w-3xl items-center justify-between px-4">
            <span className="text-[13px] text-muted-foreground">
              {t("my.searchingAs", { company: company.name })}
            </span>
            <LocalizedLink href="/my" className="text-[12px] font-medium text-primary transition-colors hover:text-primary/80">
              {t("my.myWorkspace")}
            </LocalizedLink>
          </div>
        </div>
      )}

      <div className="mx-auto max-w-3xl space-y-6 px-4 py-10 sm:py-14">
        <SearchHeader />

        <ChatThread
          messages={chat.messages}
          isWaiting={chat.isWaiting}
          onSend={handleChatMessage}
          disabled={chat.isSearching}
        />

        <div className="space-y-0">
          <AdvancedFilters
            filters={advancedFilters}
            onChange={setAdvancedFilters}
            onSearch={handleAdvancedSearch}
            open={advancedOpen}
            onToggle={() => setAdvancedOpen((prev) => !prev)}
            saveControls={
              hasActiveFilters ? (
                savedConfirm ? (
                  <span className="text-[13px] font-medium text-primary">
                    {t("my.filters.saved")}
                  </span>
                ) : (
                  <button
                    type="button"
                    onClick={() => setShowSaveFilter((prev) => !prev)}
                    aria-pressed={showSaveFilter}
                    className={`flex h-10 items-center gap-1.5 rounded-xl border px-3 text-[13px] font-medium transition-colors ${
                      showSaveFilter
                        ? "border-primary/40 bg-primary/10 text-primary"
                        : "border-border/60 bg-card text-muted-foreground hover:border-border hover:text-foreground"
                    }`}
                  >
                    <BookmarkPlus className="h-4 w-4" />
                    {t("my.filters.saveThisSearch")}
                  </button>
                )
              ) : null
            }
            saveForm={
              hasActiveFilters && showSaveFilter && !savedConfirm ? (
                <SaveFilterInline
                  onSave={handleSaveFilter}
                  onCancel={() => setShowSaveFilter(false)}
                />
              ) : null
            }
          />
        </div>

        <ActiveFilters filters={derivedActiveFilters} onRemove={handleRemoveFilter} />

        <ResultsSection
          results={results}
          totalCount={totalCount}
          isLoading={chat.isSearching}
          hasSearched={hasSearched}
          error={error}
          onRetry={handleRetry}
          onSuggestion={handleSuggestion}
          isBookmarked={isBookmarked}
          onToggleBookmark={toggleBookmark}
        />
      </div>
    </div>
  );
}
