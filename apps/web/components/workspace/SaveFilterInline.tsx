"use client";

import { useEffect, useRef, useState } from "react";
import { BookmarkPlus, Check } from "lucide-react";
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
    <div className="animate-slide-up rounded-xl border border-border/60 bg-card p-3 shadow-sm">
      <p className="mb-2 flex items-center gap-1.5 text-[12px] font-semibold text-foreground/70 uppercase tracking-wide">
        <BookmarkPlus className="h-3.5 w-3.5" />
        {t("savePrompt")}
      </p>
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
          className="h-9 flex-1 rounded-lg border border-border/60 bg-background px-3 text-[14px] text-foreground placeholder:text-muted-foreground/40 focus:border-primary/50 focus:outline-none focus:ring-2 focus:ring-primary/20"
        />
        <button
          type="button"
          onClick={handleSave}
          disabled={!name.trim()}
          className="flex h-9 items-center gap-1.5 rounded-lg bg-primary px-4 text-[13px] font-semibold text-primary-foreground transition-colors hover:bg-primary/90 disabled:opacity-35"
        >
          <Check className="h-3.5 w-3.5" />
          {t("save")}
        </button>
        <button
          type="button"
          onClick={onCancel}
          className="h-9 rounded-lg px-3 text-[13px] font-medium text-muted-foreground/70 transition-colors hover:text-foreground"
        >
          {t("cancel")}
        </button>
      </div>
    </div>
  );
}
