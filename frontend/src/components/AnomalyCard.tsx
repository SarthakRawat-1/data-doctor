import type { AnomalyDetail } from "../types";

interface Props {
  anomaly: AnomalyDetail;
  isPrimary?: boolean;
}

export function AnomalyCard({ anomaly, isPrimary = false }: Props) {
  return (
    <div className={`card relative ${isPrimary ? 'border-[var(--color-high)] shadow-sm' : ''}`}>
      {isPrimary && (
        <div className="absolute top-0 left-0 w-1 h-full bg-[var(--color-high)]" />
      )}
      <div className="flex items-start justify-between mb-4">
        <div className="flex items-center gap-3">
          <span className="px-2 py-1 bg-[rgba(255,255,255,0.05)] border border-[var(--color-border)] text-[10px] uppercase font-bold text-[var(--color-text-muted)] font-mono">
            {anomaly.type}
          </span>
          {isPrimary && (
            <span className="text-[10px] font-bold text-[var(--color-high)] uppercase tracking-wider font-mono">Primary Cause</span>
          )}
        </div>
        <span className="label-mono bg-[var(--color-bg)] px-2 py-1 border border-[var(--color-border)]">
          Depth: {anomaly.depth}
        </span>
      </div>

      <h4 className="font-header text-xl font-bold mb-1 text-[var(--color-text)]">{anomaly.name}</h4>
      {anomaly.entity_type && (
        <p className="label-mono text-[var(--color-text-muted)] mb-4">type: {anomaly.entity_type}</p>
      )}

      {anomaly.description && (
        <p className="text-sm font-medium leading-relaxed bg-[var(--color-bg)] p-4 border border-[var(--color-border)] text-[var(--color-text)]">
          {anomaly.description}
        </p>
      )}
    </div>
  );
}
