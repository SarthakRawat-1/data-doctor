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
    <div className="max-w-7xl mx-auto px-4 py-12 sm:px-6 lg:px-8 pb-32 animate-fade-in">
      {/* Header Section */}
      <div className="mb-12 border-b border-[var(--color-border)] pb-8">
        <div className="flex flex-col md:flex-row md:items-end justify-between gap-8">
          <div>
            <div className="flex flex-wrap items-center gap-4 mb-4">
              <span className="label-mono bg-[var(--color-bg-alt)] border border-[var(--color-border)] px-2 py-1">
                ID: {data.incident_id}
              </span>
              <span className="label-mono text-[var(--color-text-muted)]">
                {new Date(data.timestamp).toLocaleString()}
              </span>
              {data.execution_time_ms && (
                <span className="label-mono text-[var(--color-text-muted)]">
                  {data.execution_time_ms.toFixed(0)}MS
                </span>
              )}
            </div>
            <h1 className="font-header text-5xl font-bold mb-4">Incident Report</h1>
            <div className="inline-flex items-center gap-3 bg-[var(--color-bg-alt)] border border-[var(--color-border)] px-4 py-2">
              <span className="label-mono text-[var(--color-text-muted)]">TARGET FQN</span>
              <span className="font-mono text-sm font-medium">{data.target_asset}</span>
            </div>
          </div>

          <div className="flex items-center gap-8 bg-[var(--color-bg-alt)] p-6 border border-[var(--color-border)]">
            <div>
              <p className="label-mono text-[var(--color-text-muted)] mb-2">SEVERITY</p>
              <SeverityBadge severity={data.severity} className="text-base px-4 py-1" />
            </div>
            <div className="w-px h-16 bg-[var(--color-border)]"></div>
            <div>
              <p className="label-mono text-[var(--color-text-muted)] mb-[-4px] text-center">CONFIDENCE</p>
              <ConfidenceGauge score={data.confidence_score} className="scale-75 origin-top" />
            </div>
          </div>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-12">
        {/* Left Column: Root Cause & Factors */}
        <div className="lg:col-span-2 space-y-12">
          <section>
            <h2 className="font-header text-2xl font-bold mb-6 flex items-center gap-3">
              <span className="w-6 h-6 border border-[var(--color-brand)] rounded-full flex items-center justify-center text-sm font-mono text-[var(--color-brand)]">R</span>
              Root Cause Analysis
            </h2>

            {data.primary_root_cause ? (
              <AnomalyCard anomaly={data.primary_root_cause} isPrimary={true} />
            ) : (
              <div className="card text-center text-[var(--color-text-muted)] border-dashed border-2 font-medium py-12">
                No distinct root cause could be identified upstream. The issue may be isolated to the target asset or require deeper traversal.
              </div>
            )}
          </section>

          {data.contributing_factors.length > 0 && (
            <section>
              <h3 className="label-mono mb-4 border-b border-[var(--color-border)] pb-2">
                CONTRIBUTING FACTORS ({data.contributing_factors.length})
              </h3>
              <div className="space-y-4">
                {data.contributing_factors.map((factor, idx) => (
                  <AnomalyCard key={idx} anomaly={factor} />
                ))}
              </div>
            </section>
          )}

          <section className="pt-8 border-t border-[var(--color-border)]">
            <h2 className="font-header text-2xl font-bold mb-6 flex items-center gap-3">
              <span className="w-6 h-6 border border-[var(--color-brand)] rounded-full flex items-center justify-center text-sm font-mono text-[var(--color-brand)]">F</span>
              Suggested Fixes
            </h2>
            <div className="space-y-6">
              {data.suggested_fixes.map((fix, idx) => (
                <FixCard key={idx} fix={fix} index={idx + 1} />
              ))}
            </div>
          </section>
        </div>

        {/* Right Column: Blast Radius */}
        <div className="space-y-8 border-t lg:border-t-0 lg:border-l border-[var(--color-border)] pt-12 lg:pt-0 lg:pl-12">
          <section>
            <h2 className="font-header text-2xl font-bold mb-6 flex items-center gap-3">
              <span className="w-6 h-6 border border-[var(--color-brand)] rounded-full flex items-center justify-center text-sm font-mono text-[var(--color-brand)]">B</span>
              Blast Radius
            </h2>
            <BlastRadius impacted={data.impacted_assets} />
          </section>
        </div>
      </div>

      {/* Sticky Action Bar */}
      <div className="fixed bottom-0 left-0 right-0 border-t border-[var(--color-border)] bg-[var(--color-bg)] p-4 z-40 shadow-lg">
        <div className="max-w-7xl mx-auto flex items-center justify-between px-4 sm:px-6 lg:px-8">
          <button
            onClick={() => navigate("/diagnose")}
            className="label-mono font-medium hover:text-[var(--color-brand)] transition-colors underline decoration-[var(--color-border)] underline-offset-4"
          >
            &larr; NEW DIAGNOSIS
          </button>
          <div className="flex gap-4">
            <button
              onClick={handleCopyJson}
              className="minimal-button-outline px-6 py-2 label-mono"
            >
              COPY JSON
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
