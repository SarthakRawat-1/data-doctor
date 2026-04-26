import { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import type { DiagnosisResponse } from "../types";
import { SeverityBadge } from "../components/SeverityBadge";
import { ConfidenceGauge } from "../components/ConfidenceGauge";
import { AnomalyCard } from "../components/AnomalyCard";
import { BlastRadius } from "../components/BlastRadius";
import { FixCard } from "../components/FixCard";

export function Results() {
  const navigate = useNavigate();
  const [data] = useState<DiagnosisResponse | null>(() => {
    try {
      const stored = localStorage.getItem("datadoctor_current");
      if (stored) {
        return JSON.parse(stored);
      }
    } catch {
      // Ignore parse errors
    }
    return null;
  });

  useEffect(() => {
    if (!data) {
      navigate("/diagnose");
    }
  }, [data, navigate]);

  if (!data) return null;

  const handleCopyJson = () => {
    navigator.clipboard.writeText(JSON.stringify(data, null, 2));
  };

  return (
    <div className="max-w-7xl mx-auto px-4 py-12 sm:px-6 lg:px-8 pb-32 animate-fade-in min-h-screen">
      {/* Sleek Incident Banner */}
      <div className="mb-12 border border-[var(--color-border)] bg-[var(--color-bg-alt)] p-8">
        <div className="flex flex-col md:flex-row md:items-start justify-between gap-8 border-b border-[var(--color-border)] pb-8 mb-8">
          <div>
            <div className="flex flex-wrap items-center gap-3 mb-6">
              <span className="px-3 py-1 bg-[rgba(255,255,255,0.05)] border border-[var(--color-border)] text-white font-mono text-xs">
                INCIDENT: {data.incident_id}
              </span>
              <span className="text-[var(--color-text-muted)] font-mono text-xs">
                {new Date(data.timestamp).toLocaleString()}
              </span>
              {data.execution_time_ms && (
                <span className="text-[var(--color-text-muted)] font-mono text-xs">
                  EXEC_TIME: {data.execution_time_ms.toFixed(0)}MS
                </span>
              )}
            </div>
            <h1 className="font-header text-5xl font-bold mb-4 tracking-tight">Diagnostic Report</h1>
            <div className="inline-flex items-center gap-4 bg-[var(--color-bg)] border border-[var(--color-border)] px-5 py-3">
              <span className="label-mono text-[var(--color-text-muted)]">TARGET FQN</span>
              <span className="font-mono text-sm font-bold text-white">{data.target_asset}</span>
            </div>
          </div>
          
          <div className="flex gap-1">
            <div className="bg-[var(--color-bg)] border border-[var(--color-border)] p-5 min-w-[140px] flex flex-col items-center justify-center">
              <p className="label-mono text-[var(--color-text-muted)] mb-3">SEVERITY</p>
              <SeverityBadge severity={data.severity} className="text-sm px-4 py-1.5" />
            </div>
            <div className="bg-[var(--color-bg)] border border-[var(--color-border)] p-5 min-w-[140px] flex flex-col items-center justify-center">
              <p className="label-mono text-[var(--color-text-muted)] mb-1 text-center">CONFIDENCE</p>
              <ConfidenceGauge score={data.confidence_score} className="scale-75 origin-center" />
            </div>
          </div>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-12">
        {/* Left Column: Root Cause & Factors */}
        <div className="lg:col-span-2 space-y-12">
          <section>
            <div className="flex items-center gap-4 mb-6">
              <div className="w-8 h-8 border border-white text-white flex items-center justify-center font-mono font-bold text-sm bg-white/10">R</div>
              <h2 className="font-header text-2xl font-bold tracking-tight">Root Cause Analysis</h2>
            </div>
            
            {data.primary_root_cause ? (
              <AnomalyCard anomaly={data.primary_root_cause} isPrimary={true} />
            ) : (
              <div className="border border-[var(--color-border)] bg-[rgba(255,255,255,0.02)] p-12 flex items-center justify-center text-center">
                <p className="text-[var(--color-text-muted)] font-mono text-sm">No distinct root cause could be identified upstream.<br/>The issue may be isolated to the target asset or require deeper traversal.</p>
              </div>
            )}
          </section>

          {data.contributing_factors.length > 0 && (
            <section>
              <h3 className="label-mono mb-4 text-[var(--color-text-muted)] flex items-center gap-4">
                <span>CONTRIBUTING FACTORS ({data.contributing_factors.length})</span>
                <div className="flex-1 h-px bg-[var(--color-border)]"></div>
              </h3>
              <div className="space-y-4">
                {data.contributing_factors.map((factor, idx) => (
                  <AnomalyCard key={idx} anomaly={factor} />
                ))}
              </div>
            </section>
          )}

          <section className="pt-8 mt-12 border-t border-[var(--color-border)]">
            <div className="flex items-center gap-4 mb-6">
              <div className="w-8 h-8 border border-white text-white flex items-center justify-center font-mono font-bold text-sm bg-white/10">F</div>
              <h2 className="font-header text-2xl font-bold tracking-tight">Suggested Fixes</h2>
            </div>
            <div className="space-y-6">
              {data.suggested_fixes.map((fix, idx) => (
                <FixCard key={idx} fix={fix} index={idx + 1} />
              ))}
            </div>
          </section>
        </div>

        {/* Right Column: Blast Radius */}
        <div className="space-y-8 lg:pl-8 lg:border-l lg:border-[var(--color-border)]">
          <section>
            <div className="flex items-center gap-4 mb-6">
              <div className="w-8 h-8 border border-[var(--color-medium)] text-[var(--color-medium)] flex items-center justify-center font-mono font-bold text-sm bg-[var(--color-medium)]/10">B</div>
              <h2 className="font-header text-2xl font-bold tracking-tight">Blast Radius</h2>
            </div>
            <BlastRadius impacted={data.impacted_assets} />
          </section>
        </div>
      </div>

      {/* Sticky Action Bar */}
      <div className="fixed bottom-0 left-0 right-0 border-t border-[var(--color-border)] bg-[var(--color-bg)]/80 backdrop-blur-md p-4 z-40">
        <div className="max-w-7xl mx-auto flex items-center justify-between px-4 sm:px-6 lg:px-8">
          <button
            onClick={() => navigate("/diagnose")}
            className="label-mono text-[var(--color-text-muted)] hover:text-white transition-colors flex items-center gap-2"
          >
            <span>&larr;</span> NEW DIAGNOSIS
          </button>
          <div className="flex gap-4">
            <button
              onClick={handleCopyJson}
              className="minimal-button-outline px-6 py-2.5 label-mono text-xs"
            >
              COPY JSON
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
