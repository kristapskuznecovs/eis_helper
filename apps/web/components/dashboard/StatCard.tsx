interface StatCardProps {
  label: string;
  value: string | number | null;
  sub?: string;
}

export function StatCard({ label, value, sub }: StatCardProps) {
  return (
    <div className="rounded-2xl border border-border/40 bg-card p-5 shadow-card">
      <p className="text-[12px] font-semibold uppercase tracking-wide text-muted-foreground/70">{label}</p>
      <p className="mt-1.5 text-[26px] font-bold tabular-nums tracking-tight text-foreground">
        {value ?? "—"}
      </p>
      {sub ? <p className="mt-0.5 text-[12px] text-muted-foreground/60">{sub}</p> : null}
    </div>
  );
}
