"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { ArrowUp } from "lucide-react";
import { useTranslations } from "next-intl";
import { Button } from "@/components/ui/Button";
import type { ChatMessage } from "@/lib/types/tender";

interface ChatThreadProps {
  messages: ChatMessage[];
  isWaiting: boolean;
  onSend: (text: string) => void;
  disabled?: boolean;
}

const TypingIndicator = () => (
  <div className="animate-fade-in flex max-w-[80%] items-end gap-2">
    <div className="rounded-2xl rounded-tl-sm border border-border/40 bg-card px-4 py-3 shadow-card">
      <div className="flex items-center gap-1">
        <span className="typing-dot h-1.5 w-1.5 rounded-full bg-muted-foreground/50" />
        <span className="typing-dot h-1.5 w-1.5 rounded-full bg-muted-foreground/50" />
        <span className="typing-dot h-1.5 w-1.5 rounded-full bg-muted-foreground/50" />
      </div>
    </div>
  </div>
);

interface MessageBubbleProps {
  message: ChatMessage;
  selectedReplies: string[];
  onQuickReplyToggle: (text: string) => void;
  onOther: () => void;
  isInteractive: boolean;
}

const MessageBubble = ({
  message,
  selectedReplies,
  onQuickReplyToggle,
  onOther,
  isInteractive,
}: MessageBubbleProps) => {
  const isUser = message.role === "user";

  return (
    <div className={`animate-slide-up flex flex-col gap-2 ${isUser ? "items-end" : "items-start"}`}>
      <div className={`max-w-[80%] rounded-2xl px-4 py-3 text-[14px] leading-relaxed ${
        isUser
          ? "rounded-tr-sm bg-primary text-primary-foreground"
          : "rounded-tl-sm border border-border/40 bg-card text-foreground shadow-card"
      }`}>
        {message.content}
      </div>

      {!isUser && message.quick_replies && message.quick_replies.length > 0 && (
        <div className="animate-fade-in flex flex-wrap gap-2">
          {message.quick_replies.map((reply) => (
            (() => {
              const isOther = reply.toLowerCase() === "other" || reply === "Cits";
              const isSelected = selectedReplies.includes(reply);

              return (
                <button
                  key={reply}
                  type="button"
                  disabled={!isInteractive}
                  onClick={() => (isOther ? onOther() : onQuickReplyToggle(reply))}
                  aria-pressed={isSelected}
                  className={`rounded-full border px-3 py-1.5 text-[12px] font-medium transition-all duration-200 hover:shadow-elevated disabled:cursor-default disabled:opacity-60 ${
                    isOther
                      ? "border-border/60 bg-secondary text-muted-foreground hover:bg-card hover:text-foreground"
                      : isSelected
                        ? "border-primary bg-primary text-primary-foreground shadow-elevated"
                        : "border-chip-border bg-chip text-chip-foreground hover:bg-accent hover:text-accent-foreground"
                  }`}
                >
                  {reply}
                </button>
              );
            })()
          ))}
        </div>
      )}
    </div>
  );
};

const ChatThread = ({ messages, isWaiting, onSend, disabled }: ChatThreadProps) => {
  const t = useTranslations("search");
  const placeholders = [
    t("placeholders.construction1"),
    t("placeholders.training"),
    t("placeholders.renovation"),
    t("placeholders.engineering"),
  ];
  const [draft, setDraft] = useState("");
  const bottomRef = useRef<HTMLDivElement>(null);
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const [placeholder, setPlaceholder] = useState(placeholders[0]);
  const [selectedReplies, setSelectedReplies] = useState<string[]>([]);

  useEffect(() => {
    setPlaceholder(placeholders[Math.floor(Math.random() * placeholders.length)]);
  }, [placeholders]);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages, isWaiting]);

  const activeQuickReplyMessageId = useMemo(
    () =>
      [...messages]
        .reverse()
        .find((message) => message.role === "assistant" && message.quick_replies && message.quick_replies.length > 0)?.id ??
      null,
    [messages],
  );

  useEffect(() => {
    setSelectedReplies([]);
  }, [activeQuickReplyMessageId]);

  const adjustHeight = () => {
    const el = textareaRef.current;
    if (!el) return;
    el.style.height = "auto";
    const lineHeight = 24;
    const maxHeight = lineHeight * 4 + 24;
    el.style.height = `${Math.min(el.scrollHeight, maxHeight)}px`;
  };

  const handleChange = (e: React.ChangeEvent<HTMLTextAreaElement>) => {
    setDraft(e.target.value);
    adjustHeight();
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      submit();
    }
  };

  const submit = () => {
    const text = draft.trim();
    const selectedText = selectedReplies.join(", ");
    const payload = text || selectedText;
    if (!payload || isWaiting || disabled) return;
    setDraft("");
    setSelectedReplies([]);
    if (textareaRef.current) textareaRef.current.style.height = "auto";
    onSend(payload);
  };

  const handleQuickReplyToggle = (text: string) => {
    if (isWaiting || disabled) return;
    setSelectedReplies((prev) => (prev.includes(text) ? prev.filter((item) => item !== text) : [...prev, text]));
  };

  const handleOther = () => {
    if (isWaiting || disabled) return;
    textareaRef.current?.focus();
  };

  const isEmpty = messages.length === 0;
  const canSubmit = Boolean(draft.trim() || selectedReplies.length > 0) && !isWaiting && !disabled;

  return (
    <div className="space-y-4">
      {isEmpty ? (
        <label className="text-[13px] font-semibold uppercase tracking-wide text-muted-foreground/80">
          {t("describeLabel")}
        </label>
      ) : (
        <div className="space-y-4">
          {messages.map((message) => (
            <MessageBubble
              key={message.id}
              message={message}
              selectedReplies={message.id === activeQuickReplyMessageId ? selectedReplies : []}
              onQuickReplyToggle={handleQuickReplyToggle}
              onOther={handleOther}
              isInteractive={message.id === activeQuickReplyMessageId}
            />
          ))}
          {isWaiting && <TypingIndicator />}
          <div ref={bottomRef} />
        </div>
      )}

      <div className="group relative flex items-end gap-2.5">
        <div className="relative flex-1">
          <textarea
            ref={textareaRef}
            value={draft}
            onChange={handleChange}
            onKeyDown={handleKeyDown}
            placeholder={isEmpty ? placeholder : t("placeholderDefault")}
            rows={1}
            disabled={isWaiting || disabled}
            className="w-full resize-none rounded-xl border-0 bg-card px-4 py-3 text-[15px] text-foreground placeholder:text-muted-foreground/50 shadow-card transition-all duration-200 focus:outline-none focus:ring-2 focus:ring-primary/40 focus:shadow-elevated disabled:opacity-60"
            style={{ minHeight: "48px", maxHeight: "120px" }}
          />
        </div>
        <Button
          type="button"
          onClick={submit}
          disabled={!canSubmit}
          className="h-12 w-12 shrink-0 rounded-xl p-0 shadow-card transition-all duration-200 hover:shadow-elevated"
        >
          <ArrowUp className="h-5 w-5" strokeWidth={2.5} />
        </Button>
      </div>

      {isEmpty && (
        <p className="text-[12px] text-muted-foreground/50">
          {t("enterHint")}
        </p>
      )}
    </div>
  );
};

export default ChatThread;
