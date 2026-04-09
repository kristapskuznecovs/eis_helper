"use client";

import { useEffect, useRef, useState } from "react";
import { Check } from "lucide-react";
import { useTranslations } from "next-intl";

interface SaveFilterInlineProps {
  onSave: (name: string) => void;
  onCancel: () => void;
}

export function SaveFilterInline({ onSave, onCancel }: SaveFilterInlineProps) {
  const t = useTranslations("my.filters");
  const [name, setName] = useState("");
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    inputRef.current?.focus();
  }, []);

  const handleSave = () => {
    const trimmed = name.trim();
    if (!trimmed) return;
    onSave(trimmed);
  };

  return (
    <div className="animate-slide-up rounded-2xl border border-border/50 bg-card p-4 shadow-card">
      <p className="mb-2 text-[12px] font-medium text-muted-foreground/70">{t("savePrompt")}</p>
      <div className="flex gap-2">
        <input
          ref={inputRef}
          value={name}
          onChange={(e) => setName(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter") handleSave();
            if (e.key === "Escape") onCancel();
          }}
          placeholder={t("savePlaceholder")}
          className="h-9 flex-1 rounded-lg border-0 bg-secondary/60 px-3 text-[14px] text-foreground placeholder:text-muted-foreground/40 focus:bg-card focus:outline-none focus:ring-2 focus:ring-primary/30"
        />
        <button
          type="button"
          onClick={handleSave}
          disabled={!name.trim()}
          className="flex h-9 items-center gap-1.5 rounded-lg bg-primary px-3 text-[13px] font-medium text-primary-foreground transition-colors hover:bg-primary/90 disabled:opacity-40"
        >
          <Check className="h-3.5 w-3.5" />
          {t("save")}
        </button>
        <button
          type="button"
          onClick={onCancel}
          className="h-9 rounded-lg px-3 text-[13px] font-medium text-muted-foreground/60 transition-colors hover:text-foreground"
        >
          {t("cancel")}
        </button>
      </div>
    </div>
  );
}
