import type { Severity } from "../types";

interface Props {
  severity: Severity;
  className?: string;
}

export function SeverityBadge({ severity, className = "" }: Props) {
  const styles = {
    HIGH: "bg-[var(--color-high)] text-white",
    MEDIUM: "bg-[var(--color-medium)] text-white",
    LOW: "bg-[var(--color-low)] text-white",
  };

  return (
    <span
      className={`inline-flex items-center px-2 py-0.5 rounded-sm border border-[var(--color-border)] font-mono font-bold text-[11px] uppercase tracking-wider ${styles[severity]} ${className}`}
    >
      {severity}
    </span>
  );
}
