import type { ImpactedAssets } from "../types";

export function BlastRadius({ impacted }: { impacted: ImpactedAssets }) {
  const hasImpact = impacted.total_impact_count > 0;

  if (!hasImpact) {
    return (
      <div className="card flex items-center justify-center py-12 text-[var(--color-text-muted)] border-dashed border-2">
        <p className="font-medium">No downstream impact detected.</p>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-3 gap-1">
        <div className="card bg-[var(--color-bg)] flex flex-col items-center justify-center py-8">
          <span className="font-header text-4xl font-bold mb-2 text-[var(--color-text)]">{impacted.tables.length}</span>
          <span className="label-mono text-[var(--color-text-muted)]">TABLES</span>
        </div>
        <div className="card bg-[var(--color-bg)] flex flex-col items-center justify-center py-8 border-[var(--color-medium)]/40">
          <span className="font-header text-4xl font-bold mb-2 text-[var(--color-medium)]">{impacted.dashboards.length}</span>
          <span className="label-mono text-[var(--color-medium)]">DASHBOARDS</span>
        </div>
        <div className="card bg-[var(--color-bg)] flex flex-col items-center justify-center py-8 border-[#22D3EE]/40">
          <span className="font-header text-4xl font-bold mb-2 text-[#22D3EE]">{impacted.ml_models.length}</span>
          <span className="label-mono text-[#22D3EE]">ML MODELS</span>
        </div>
      </div>

      <div className="card">
        <h4 className="label-mono border-b border-[var(--color-border)] pb-3 mb-4 text-[var(--color-text-muted)]">IMPACTED ASSETS LIST</h4>
        <div className="space-y-3 max-h-80 overflow-y-auto pr-4">
          {impacted.dashboards.map((d, i) => (
            <div key={`d-${i}`} className="flex items-center gap-3">
              <span className="px-2 py-0.5 bg-[var(--color-medium)]/10 text-[var(--color-medium)] border border-[var(--color-medium)]/30 text-[10px] font-mono uppercase font-bold">DASHBOARD</span>
              <span className="font-mono text-sm text-[var(--color-text-muted)] cursor-default truncate">{String(d.fullyQualifiedName || d.name || 'unknown')}</span>
            </div>
          ))}
          {impacted.ml_models.map((m, i) => (
            <div key={`m-${i}`} className="flex items-center gap-3">
              <span className="px-2 py-0.5 bg-[#22D3EE]/10 text-[#22D3EE] border border-[#22D3EE]/30 text-[10px] font-mono uppercase font-bold">ML MODEL</span>
              <span className="font-mono text-sm text-[var(--color-text-muted)] cursor-default truncate">{String(m.fullyQualifiedName || m.name || 'unknown')}</span>
            </div>
          ))}
          {impacted.tables.map((t, i) => (
            <div key={`t-${i}`} className="flex items-center gap-3">
              <span className="px-2 py-0.5 bg-[rgba(255,255,255,0.05)] border border-[var(--color-border)] text-[var(--color-text-muted)] text-[10px] font-mono uppercase font-bold">TABLE</span>
              <span className="font-mono text-sm text-[var(--color-text-muted)] cursor-default truncate">{String(t.fullyQualifiedName || t.name || 'unknown')}</span>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
