"use client";

import type { ReactNode } from "react";
import { useEffect, useState } from "react";
import { SlidersHorizontal } from "lucide-react";
import { useLocale, useTranslations } from "next-intl";
import { Button } from "@/components/ui/Button";
import { fetchProcedureTypes, resolveCompanyCpv } from "@/lib/api/chat";
import { cpvLabel } from "@/lib/cpv";
import type { ExtractedFilters } from "@/lib/types/tender";

interface AdvancedFiltersProps {
  filters: ExtractedFilters;
  onChange: (filters: ExtractedFilters) => void;
  onSearch: (filters: ExtractedFilters) => void;
  open: boolean;
  onToggle: () => void;
  saveControls?: ReactNode;
}

const inputClass =
  "h-9 w-full rounded-lg border-0 bg-secondary/60 px-3 text-[14px] text-foreground placeholder:text-muted-foreground/40 transition-all duration-200 focus:bg-card focus:outline-none focus:ring-2 focus:ring-primary/30";
const selectClass = `${inputClass} appearance-none cursor-pointer`;

const AdvancedFilters = ({ filters, onChange, onSearch, open, onToggle, saveControls }: AdvancedFiltersProps) => {
  const locale = useLocale();
  const t = useTranslations("search.advanced");
  const common = useTranslations("common");
  const [similarInput, setSimilarInput] = useState("");
  const [resolving, setResolving] = useState(false);
  const [resolvedLabels, setResolvedLabels] = useState<Record<string, string>>({});
  const [procedureTypes, setProcedureTypes] = useState<string[]>([]);

  useEffect(() => {
    fetchProcedureTypes().then(setProcedureTypes).catch(() => {});
  }, []);

  const update = <K extends keyof ExtractedFilters>(key: K, value: ExtractedFilters[K]) => {
    onChange({ ...filters, [key]: value });
  };

  const keywordsStr = (filters.keywords ?? []).join(", ");
  const [cpvInput, setCpvInput] = useState("");

  const similarCpvPrefixes = filters.similar_company_cpv_prefixes ?? [];
  // Non-similar CPV prefixes (manually added or from category)
  const manualCpvPrefixes = (filters.cpv_prefixes ?? []).filter(
    (p) => !similarCpvPrefixes.includes(p),
  );

  const addCpvPrefix = (raw: string) => {
    const trimmed = raw.trim().replace(/\D/g, "").slice(0, 2);
    if (!trimmed || trimmed.length < 2) return;
    const existing = filters.cpv_prefixes ?? [];
    if (!existing.includes(trimmed)) {
      update("cpv_prefixes", [...existing, trimmed]);
    }
    setCpvInput("");
  };

  const removeCpvPrefix = (prefix: string) => {
    update(
      "cpv_prefixes",
      (filters.cpv_prefixes ?? []).filter((p) => p !== prefix),
    );
  };

  const handleResolveSimilar = async () => {
    const name = similarInput.trim();
    if (!name) return;
    setResolving(true);
    try {
      const profile = await resolveCompanyCpv(name, locale);
      if (profile.cpv_prefixes.length === 0) return;
      setResolvedLabels((prev) => ({ ...prev, ...profile.cpv_labels }));
      const existing = filters.similar_company_cpv_prefixes ?? [];
      const merged = Array.from(new Set([...existing, ...profile.cpv_prefixes]));
      const allCpv = Array.from(new Set([...(filters.cpv_prefixes ?? []), ...profile.cpv_prefixes]));
      const allCos = Array.from(new Set([...(filters.similar_companies ?? []), name]));
      onChange({
        ...filters,
        similar_companies: allCos,
        similar_company_cpv_prefixes: merged,
        cpv_prefixes: allCpv,
      });
      setSimilarInput("");
    } finally {
      setResolving(false);
    }
  };

  const removeSimilarCpv = (prefix: string) => {
    const updated = similarCpvPrefixes.filter((p) => p !== prefix);
    const allCpv = (filters.cpv_prefixes ?? []).filter((p) => p !== prefix);
    onChange({ ...filters, similar_company_cpv_prefixes: updated, cpv_prefixes: allCpv });
  };

  return (
    <div>
      <button
        type="button"
        onClick={onToggle}
        className="inline-flex items-center gap-1.5 text-[13px] font-medium text-muted-foreground/70 transition-colors duration-200 hover:text-foreground"
      >
        <SlidersHorizontal className="h-3.5 w-3.5" />
        {open ? t("hide") : t("show")}
      </button>

      {open && (
        <div className="animate-slide-up mt-3 space-y-4 rounded-2xl border border-border/50 bg-card p-5 shadow-card">
          <div className="flex items-center justify-between">
            <h3 className="text-[15px] font-semibold text-foreground">{t("title")}</h3>
            <button
              type="button"
              onClick={() => onChange({ status: "open" })}
              className="text-[12px] font-medium text-muted-foreground/60 transition-colors hover:text-foreground"
            >
              {t("resetAll")}
            </button>
          </div>

          <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
            <div className="space-y-1.5">
              <label className="text-[12px] font-medium text-muted-foreground/70">Keywords</label>
              <input
                className={inputClass}
                value={keywordsStr}
                onChange={(e) =>
                  update(
                    "keywords",
                    e.target.value
                      .split(",")
                      .map((k) => k.trim())
                      .filter(Boolean),
                  )
                }
                placeholder={t("keywordsPlaceholder")}
              />
            </div>

            <div className="space-y-1.5">
              <label className="text-[12px] font-medium text-muted-foreground/70">Issuer (buyer)</label>
              <input
                className={inputClass}
                value={filters.buyer ?? ""}
                onChange={(e) => update("buyer", e.target.value || undefined)}
                placeholder="e.g. Rīgas dome, Valsts kase"
              />
            </div>

            <div className="space-y-1.5">
              <label className="text-[12px] font-medium text-muted-foreground/70">Category</label>
              <select
                className={selectClass}
                value={filters.category ?? ""}
                onChange={(e) => update("category", e.target.value || undefined)}
              >
                <option value="">{common("any")}</option>
                <option value="construction">Construction</option>
                <option value="it">IT & Software</option>
                <option value="consulting">Consulting</option>
                <option value="training">Training</option>
                <option value="engineering">Engineering</option>
                <option value="healthcare">Healthcare</option>
                <option value="supplies">Supplies</option>
              </select>
            </div>

            <div className="space-y-1.5">
              <label className="text-[12px] font-medium text-muted-foreground/70">Region</label>
              <select
                className={selectClass}
                value={filters.planning_region ?? ""}
                onChange={(e) => update("planning_region", e.target.value || undefined)}
              >
                <option value="">All Latvia</option>
                <option value="Rīga">Rīga</option>
                <option value="Pierīga">Pierīga</option>
                <option value="Vidzeme">Vidzeme</option>
                <option value="Kurzeme">Kurzeme</option>
                <option value="Zemgale">Zemgale</option>
                <option value="Latgale">Latgale</option>
              </select>
            </div>

            <div className="space-y-1.5">
              <label className="text-[12px] font-medium text-muted-foreground/70">Subject type</label>
              <select
                className={selectClass}
                value={filters.subject_type ?? ""}
                onChange={(e) => update("subject_type", (e.target.value as ExtractedFilters["subject_type"]) || undefined)}
              >
                <option value="">{common("any")}</option>
                <option value="works">Works</option>
                <option value="services">Services</option>
                <option value="supplies">Supplies</option>
              </select>
            </div>

            <div className="space-y-1.5">
              <label className="text-[12px] font-medium text-muted-foreground/70">CPV category</label>
              <div className="flex gap-2">
                <input
                  className={inputClass}
                  value={cpvInput}
                  onChange={(e) => setCpvInput(e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === "Enter" || e.key === ",") {
                      e.preventDefault();
                      addCpvPrefix(cpvInput);
                    }
                  }}
                  placeholder="e.g. 45 or 72"
                  maxLength={3}
                />
                <button
                  type="button"
                  onClick={() => addCpvPrefix(cpvInput)}
                  disabled={!cpvInput.trim()}
                  className="shrink-0 h-9 rounded-lg bg-secondary/80 px-3 text-[13px] font-medium text-foreground/70 transition-colors hover:bg-secondary disabled:opacity-40"
                >
                  Add
                </button>
              </div>
              {manualCpvPrefixes.length > 0 && (
                <div className="flex flex-wrap gap-1.5 pt-1">
                  {manualCpvPrefixes.map((prefix) => (
                    <span
                      key={prefix}
                      className="inline-flex items-center gap-1 rounded-full bg-secondary/80 px-2.5 py-0.5 text-[12px] font-medium text-foreground/70"
                    >
                      {prefix} — {cpvLabel(prefix)}
                      <button
                        type="button"
                        onClick={() => removeCpvPrefix(prefix)}
                        className="ml-0.5 opacity-50 hover:opacity-100"
                      >
                        ×
                      </button>
                    </span>
                  ))}
                </div>
              )}
            </div>

            <div className="space-y-1.5 sm:col-span-2">
              <label className="text-[12px] font-medium text-muted-foreground/70">
                Similar company <span className="text-muted-foreground/40 font-normal">(finds their CPV categories)</span>
              </label>
              <div className="flex gap-2">
                <input
                  className={inputClass}
                  value={similarInput}
                  onChange={(e) => setSimilarInput(e.target.value)}
                  onKeyDown={(e) => { if (e.key === "Enter") handleResolveSimilar(); }}
                  placeholder="e.g. RBS, Accenture Latvia, SSE Riga"
                />
                <button
                  type="button"
                  onClick={handleResolveSimilar}
                  disabled={!similarInput.trim() || resolving}
                  className="shrink-0 h-9 rounded-lg bg-primary/10 px-3 text-[13px] font-medium text-primary transition-colors hover:bg-primary/20 disabled:opacity-40"
                >
                  {resolving ? "..." : t("findCpvs")}
                </button>
              </div>
              {similarCpvPrefixes.length > 0 && (
                <div className="flex flex-wrap gap-1.5 pt-1">
                  {similarCpvPrefixes.map((prefix) => (
                    <span
                      key={prefix}
                      className="inline-flex items-center gap-1 rounded-full bg-primary/10 px-2.5 py-0.5 text-[12px] font-medium text-primary"
                    >
                      {prefix} — {resolvedLabels[prefix] ?? cpvLabel(prefix)}
                      <button
                        type="button"
                        onClick={() => removeSimilarCpv(prefix)}
                        className="ml-0.5 opacity-50 hover:opacity-100"
                      >
                        ×
                      </button>
                    </span>
                  ))}
                </div>
              )}
              {filters.similar_companies && filters.similar_companies.length > 0 && (
                <p className="text-[11px] text-muted-foreground/50">
                  {t("basedOn", { value: filters.similar_companies.join(", ") })}
                </p>
              )}
            </div>

            <div className="space-y-1.5">
              <label className="text-[12px] font-medium text-muted-foreground/70">Min value (EUR)</label>
              <input
                type="number"
                className={inputClass}
                value={filters.value_min_eur ?? ""}
                onChange={(e) => update("value_min_eur", e.target.value ? Number(e.target.value) : undefined)}
                placeholder="e.g. 20000"
              />
            </div>

            <div className="space-y-1.5">
              <label className="text-[12px] font-medium text-muted-foreground/70">Max value (EUR)</label>
              <input
                type="number"
                className={inputClass}
                value={filters.value_max_eur ?? ""}
                onChange={(e) => update("value_max_eur", e.target.value ? Number(e.target.value) : undefined)}
                placeholder="e.g. 500000"
              />
            </div>

            <div className="space-y-1.5">
              <label className="text-[12px] font-medium text-muted-foreground/70">Deadline within</label>
              <select
                className={selectClass}
                value={filters.deadline_days ?? ""}
                onChange={(e) => update("deadline_days", e.target.value ? Number(e.target.value) : undefined)}
              >
                <option value="">Any time</option>
                <option value="14">2 weeks</option>
                <option value="30">1 month</option>
                <option value="90">3 months</option>
              </select>
            </div>

            <div className="space-y-1.5">
              <label className="text-[12px] font-medium text-muted-foreground/70">Sort by</label>
              <select
                className={selectClass}
                value={filters.sort ?? ""}
                onChange={(e) => update("sort", (e.target.value as ExtractedFilters["sort"]) || undefined)}
              >
                <option value="">Relevance</option>
                <option value="date_desc">Newest first</option>
                <option value="deadline">Deadline soonest</option>
                <option value="value_desc">Highest value</option>
              </select>
            </div>

            <div className="space-y-1.5">
              <label className="text-[12px] font-medium text-muted-foreground/70">Status</label>
              <select
                className={selectClass}
                value={filters.status ?? "open"}
                onChange={(e) => update("status", e.target.value || "open")}
              >
                <option value="open">Open (announced)</option>
                <option value="all">All statuses</option>
                <option value="Izsludināts">Izsludināts</option>
                <option value="Pieteikumi/piedāvājumi atvērti">Pieteikumi / piedāvājumi atvērti</option>
                <option value="Lēmums pieņemts">Lēmums pieņemts</option>
                <option value="Uzsākta līguma slēgšana">Uzsākta līguma slēgšana</option>
                <option value="Līgums noslēgts">Līgums noslēgts</option>
                <option value="Noslēgts">Noslēgts</option>
                <option value="Pārtraukts">Pārtraukts</option>
                <option value="Izbeigts">Izbeigts</option>
              </select>
            </div>

            <div className="space-y-1.5">
              <label className="text-[12px] font-medium text-muted-foreground/70">Procedure type</label>
              <input
                list="procedure-types-list"
                className={inputClass}
                value={filters.procedure_type ?? ""}
                onChange={(e) => update("procedure_type", e.target.value || undefined)}
                placeholder={common("any")}
              />
              <datalist id="procedure-types-list">
                {procedureTypes.map((pt) => (
                  <option key={pt} value={pt} />
                ))}
              </datalist>
            </div>

            <div className="space-y-1.5">
              <label className="text-[12px] font-medium text-muted-foreground/70">Published from</label>
              <input
                type="date"
                className={inputClass}
                value={filters.pub_date_from ?? ""}
                onChange={(e) => update("pub_date_from", e.target.value || undefined)}
              />
            </div>

            <div className="space-y-1.5">
              <label className="text-[12px] font-medium text-muted-foreground/70">Published to</label>
              <input
                type="date"
                className={inputClass}
                value={filters.pub_date_to ?? ""}
                onChange={(e) => update("pub_date_to", e.target.value || undefined)}
              />
            </div>
          </div>

          <div className="space-y-2">
            <div className="flex flex-col gap-2 sm:flex-row sm:items-center">
              <Button
                onClick={() => onSearch(filters)}
                className="h-10 w-full rounded-xl font-semibold shadow-card transition-all duration-200 hover:shadow-elevated sm:w-auto"
              >
                {t("submit")}
              </Button>
              {saveControls}
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default AdvancedFilters;
