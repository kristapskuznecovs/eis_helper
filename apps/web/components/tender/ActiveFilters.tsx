import { X } from "lucide-react";
import { ActiveFilter } from "@/lib/types/tender";

interface ActiveFiltersProps {
  filters: ActiveFilter[];
  onRemove: (key: string) => void;
}

const ActiveFilters = ({ filters, onRemove }: ActiveFiltersProps) => {
  if (filters.length === 0) {
    return null;
  }

  return (
    <div className="flex flex-wrap gap-2 animate-fade-in">
      {filters.map((filter) => (
        <span
          key={filter.key}
          className="group inline-flex items-center gap-1.5 rounded-full border border-chip-border bg-chip px-3 py-1.5 text-[12px] font-medium text-chip-foreground shadow-card transition-all duration-200 hover:shadow-elevated"
        >
          <span className="text-muted-foreground">{filter.label}:</span>
          <span className="capitalize">{filter.value}</span>
          <button
            type="button"
            onClick={() => onRemove(filter.key)}
            className="ml-0.5 rounded-full p-0.5 opacity-50 transition-all hover:bg-secondary hover:opacity-100"
          >
            <X className="h-3 w-3" />
          </button>
        </span>
      ))}
    </div>
  );
};

export default ActiveFilters;
