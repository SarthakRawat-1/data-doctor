import type { AnomalyDetail } from "../types";

interface Props {
  anomaly: AnomalyDetail;
  isPrimary?: boolean;
}

export function AnomalyCard({ anomaly, isPrimary = false }: Props) {
  return (
    <div className="relative border border-[var(--color-border)] bg-[var(--color-bg-alt)] hover:border-white/30 transition-colors p-6 group">
      {isPrimary && (
        <div className="absolute top-0 left-0 w-1 h-full bg-[var(--color-high)] shadow-[0_0_10px_var(--color-high)]" />
      )}
      {!isPrimary && (
        <div className="absolute top-0 left-0 w-1 h-full bg-[var(--color-medium)] opacity-50" />
      )}
      
      <div className="flex items-start justify-between mb-5 pl-2">
        <div className="flex items-center gap-3">
          <span className="px-2 py-1 bg-[rgba(255,255,255,0.05)] border border-[var(--color-border)] text-[10px] uppercase font-bold text-white font-mono tracking-wider">
            {anomaly.type}
          </span>
          {isPrimary && (
            <span className="text-[10px] font-bold text-[var(--color-high)] uppercase tracking-widest font-mono">PRIMARY ROOT CAUSE</span>
          )}
        </div>
        <div className="flex items-center gap-2 border border-[var(--color-border)] bg-[var(--color-bg)] px-2 py-1">
          <span className="w-1.5 h-1.5 bg-[var(--color-text-muted)] rounded-full"></span>
          <span className="font-mono text-[10px] text-[var(--color-text-muted)] uppercase tracking-wider">
            DEPTH: {anomaly.depth}
          </span>
        </div>
      </div>
      
      <div className="pl-2">
        <h4 className="font-header text-xl font-bold mb-2 text-white">{anomaly.name}</h4>
        {anomaly.entity_type && (
          <p className="font-mono text-xs text-[var(--color-text-muted)] mb-4">entity_type: <span className="text-white">{anomaly.entity_type}</span></p>
        )}
        
        {anomaly.description && (
          <div className="mt-4 bg-[var(--color-bg)] p-4 border border-[var(--color-border)]">
            <p className="text-sm text-[var(--color-text-muted)] leading-relaxed font-mono">
              {anomaly.description}
            </p>
          </div>
        )}
      </div>
    </div>
  );
}
