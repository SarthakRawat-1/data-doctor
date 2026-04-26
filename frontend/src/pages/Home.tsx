import { useState } from "react";
import { Link } from "react-router-dom";
import type { DiagnosisResponse } from "../types";

export function Home() {
  const [recent] = useState<DiagnosisResponse[]>(() => {
    try {
      const stored = localStorage.getItem("datadoctor_recent");
      if (stored) {
        return JSON.parse(stored).slice(0, 5);
      }
    } catch (e) {
      console.error(e);
    }
    return [];
  });

  return (
    <div className="min-h-screen">
      {/* Hero Section */}
      <section className="relative w-full border-b border-[var(--color-border)] overflow-hidden">
        <div className="absolute inset-0 bg-grid-pattern opacity-30"></div>
        <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-3/4 h-3/4 bg-blue-600/10 blur-[120px] rounded-full pointer-events-none"></div>
        
        <div className="relative max-w-7xl mx-auto px-4 py-32 sm:px-6 lg:px-8 flex flex-col items-center text-center">
          <div className="inline-flex items-center gap-2 px-3 py-1 bg-[rgba(255,255,255,0.05)] border border-[var(--color-border)] mb-8 animate-fade-in">
            <span className="w-2 h-2 bg-[var(--color-low)] rounded-none animate-pulse"></span>
            <span className="label-mono">Data Doctor Platform v1.0</span>
          </div>
          
          <h1 className="text-5xl sm:text-6xl lg:text-7xl font-header font-bold text-[var(--color-text)] mb-8 max-w-5xl tracking-tight leading-[1.1] animate-slide-up">
            Diagnose data failures instantly.<br />
            <span className="text-[var(--color-text-muted)]">Get actionable fixes in seconds.</span>
          </h1>
          
          <p className="text-xl text-[var(--color-text-muted)] max-w-3xl mx-auto mb-12 animate-slide-up" style={{ animationDelay: "100ms", animationFillMode: "both" }}>
            Data Doctor is an advanced observability engine built on top of OpenMetadata. It executes bidirectional BFS traversals to pinpoint pipeline failures, calculates mathematical confidence scores, and generates automated SQL fixes.
          </p>
          
          <div className="flex flex-col sm:flex-row items-center justify-center gap-4 animate-slide-up" style={{ animationDelay: "200ms", animationFillMode: "both" }}>
            <Link
              to="/diagnose"
              className="w-full sm:w-auto px-8 py-3.5 minimal-button text-sm uppercase tracking-wider label-mono"
            >
              Start Diagnosis
            </Link>
            <Link
              to="/diagnose?demo=true"
              className="w-full sm:w-auto px-8 py-3.5 minimal-button-outline text-sm uppercase tracking-wider label-mono"
            >
              Run Demo Scenario
            </Link>
          </div>
        </div>
      </section>

      {/* The Engine Section */}
      <section className="border-b border-[var(--color-border)] bg-[var(--color-bg-alt)]">
        <div className="max-w-7xl mx-auto px-4 py-24 sm:px-6 lg:px-8">
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-16 items-center">
            <div>
              <div className="label-mono text-[var(--color-text-muted)] mb-4">THE GRAPH ENGINE</div>
              <h2 className="text-4xl font-header font-bold mb-6">Bidirectional BFS Traversal</h2>
              <p className="text-[var(--color-text-muted)] text-lg mb-6 leading-relaxed">
                When an incident is reported, Data Doctor doesn't guess. It directly queries the OpenMetadata Lineage API, executing a Breadth-First Search (BFS) in two directions simultaneously.
              </p>
              <ul className="space-y-4 font-mono text-sm">
                <li className="flex items-start gap-4 p-4 border border-[var(--color-border)] bg-[var(--color-bg)]">
                  <span className="text-[var(--color-high)] mt-0.5">▲</span>
                  <div>
                    <strong className="text-white block mb-1">Upstream Root Cause Traversal</strong>
                    <span className="text-[var(--color-text-muted)]">Scans up to depth N to find the source of the anomaly before it propagated.</span>
                  </div>
                </li>
                <li className="flex items-start gap-4 p-4 border border-[var(--color-border)] bg-[var(--color-bg)]">
                  <span className="text-[var(--color-medium)] mt-0.5">▼</span>
                  <div>
                    <strong className="text-white block mb-1">Downstream Blast Radius Mapping</strong>
                    <span className="text-[var(--color-text-muted)]">Maps all affected Tables, Dashboards, and ML Models impacted by the failure.</span>
                  </div>
                </li>
              </ul>
            </div>
            
            <div className="relative h-full min-h-[400px] border border-[var(--color-border)] bg-[var(--color-bg)] p-8 overflow-hidden">
              <div className="absolute inset-0 bg-grid-pattern opacity-10"></div>
              {/* Abstract Graphic representing DAG */}
              <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-full max-w-sm">
                <div className="flex justify-center">
                  <div className="px-4 py-2 border border-[var(--color-high)] text-[var(--color-high)] font-mono text-xs font-bold bg-[var(--color-high)]/10">ROOT: airflow.task.failed</div>
                </div>
                <div className="w-px h-8 bg-white/40 mx-auto"></div>
                <div className="flex justify-center">
                  <div className="px-4 py-2 border border-white/20 text-[var(--color-text-muted)] font-mono text-xs">snowflake.raw.users</div>
                </div>
                <div className="w-px h-8 bg-white/40 mx-auto"></div>
                <div className="flex justify-center">
                  <div className="px-4 py-2 border border-white text-white font-mono text-xs font-bold bg-white/10 ring-2 ring-white/20">TARGET FQN</div>
                </div>
                
                {/* Branching Downstream Edges */}
                <div className="w-px h-6 bg-white/40 mx-auto"></div>
                <div className="w-48 h-px bg-white/40 mx-auto"></div>
                <div className="flex justify-between w-48 mx-auto mb-2">
                  <div className="w-px h-6 bg-white/40"></div>
                  <div className="w-px h-6 bg-white/40"></div>
                </div>
                
                <div className="flex justify-between px-10">
                  <div className="px-3 py-1 border border-[var(--color-medium)] text-[var(--color-medium)] font-mono text-[10px] bg-[var(--color-medium)]/10">DASHBOARD</div>
                  <div className="px-3 py-1 border border-[#9a73f5] text-[#9a73f5] font-mono text-[10px] bg-[#9a73f5]/10">ML MODEL</div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </section>

      {/* 6 Detectors Grid */}
      <section className="border-b border-[var(--color-border)]">
        <div className="max-w-7xl mx-auto px-4 py-24 sm:px-6 lg:px-8">
          <div className="text-center mb-16">
            <div className="label-mono text-[var(--color-text-muted)] mb-4">DETECTION CAPABILITIES</div>
            <h2 className="text-4xl font-header font-bold">6 Core Anomaly Detectors</h2>
            <p className="text-[var(--color-text-muted)] mt-4 max-w-2xl mx-auto">Every node in the traversal path is rigorously evaluated against our suite of deterministic anomaly detectors.</p>
          </div>
          
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-1">
            {[
              { id: '01', title: 'Pipeline Failures', desc: 'Detects failed Airflow DAGs, dbt run failures, and pipeline execution errors mapped directly in OpenMetadata.' },
              { id: '02', title: 'Schema Drift', desc: 'Identifies structural changes: columns added, deleted, or data type modifications without warning.' },
              { id: '03', title: 'Volume Drops', desc: 'Dynamic thresholding flags sudden >5% drops in row counts indicating partial data loads.' },
              { id: '04', title: 'Freshness SLA', desc: 'Monitors timestamp metadata to catch stale tables that missed their regular update windows.' },
              { id: '05', title: 'Test Failures', desc: 'Integrates with Great Expectations/dbt tests to flag nodes where data quality assertions are failing.' },
              { id: '06', title: 'Distribution Drift', desc: 'Statistically detects field-level distribution changes like unexpected spikes in null proportions or distinct counts.' }
            ].map((feature, i) => (
              <div key={i} className="card h-full p-8 border-[var(--color-border)] hover:bg-[rgba(255,255,255,0.02)]">
                <span className="font-mono text-sm text-[var(--color-text-muted)] block mb-4 border-b border-[var(--color-border)] pb-2">{feature.id}</span>
                <h3 className="font-header text-xl font-bold mb-3">{feature.title}</h3>
                <p className="text-[var(--color-text-muted)] text-sm leading-relaxed">{feature.desc}</p>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* Scoring & Action Layer */}
      <section className="border-b border-[var(--color-border)]">
        <div className="grid grid-cols-1 lg:grid-cols-2 divide-y lg:divide-y-0 lg:divide-x divide-[var(--color-border)]">
          <div className="p-12 lg:p-24 bg-[var(--color-bg)]">
            <div className="label-mono text-[var(--color-text-muted)] mb-4">CONFIDENCE ENGINE</div>
            <h2 className="text-3xl font-header font-bold mb-6">Deterministic Scoring</h2>
            <p className="text-[var(--color-text-muted)] mb-8 leading-relaxed">
              We don't use AI to guess the root cause. We calculate it. Based on extensive research, our scoring engine combines graph distance penalties, severity weights, and structural vs. behavioral multipliers to accurately pinpoint anomalies.
            </p>
            <div className="p-6 border border-[var(--color-border)] bg-[#0A0A0A] font-mono text-sm text-[var(--color-text-muted)] overflow-x-auto">
              <span className="text-purple-400">def</span> <span className="text-blue-400">calculate_confidence</span>(depth, severity, type):<br/>
              &nbsp;&nbsp;&nbsp;&nbsp;base_score = max(<span className="text-orange-400">0.2</span>, <span className="text-orange-400">1.0</span> - (depth * <span className="text-orange-400">0.15</span>))<br/>
              &nbsp;&nbsp;&nbsp;&nbsp;sev_multiplier = {`{`}HIGH: <span className="text-orange-400">1.2</span>, LOW: <span className="text-orange-400">0.8</span>{`}`}<br/>
              &nbsp;&nbsp;&nbsp;&nbsp;type_weight = structural_check(type)<br/>
              &nbsp;&nbsp;&nbsp;&nbsp;<span className="text-purple-400">return</span> min(<span className="text-orange-400">1.0</span>, base_score * sev_multiplier * type_weight)
            </div>
          </div>
          
          <div className="p-12 lg:p-24 bg-[var(--color-bg-alt)]">
            <div className="label-mono text-[var(--color-text-muted)] mb-4">AUTOMATED GOVERNANCE</div>
            <h2 className="text-3xl font-header font-bold mb-6">Action & Remediation</h2>
            <p className="text-[var(--color-text-muted)] mb-8 leading-relaxed">
              Once the root cause is deterministically identified, Data Doctor takes action to prevent downstream consumer impact and assist data engineers.
            </p>
            <ul className="space-y-6">
              <li className="flex items-start gap-4">
                <div className="flex-shrink-0 w-8 h-8 border border-[var(--color-border)] bg-[var(--color-bg)] flex items-center justify-center font-mono text-xs">AI</div>
                <div>
                  <h4 className="font-bold text-white mb-1">Groq LLM Integration</h4>
                  <p className="text-sm text-[var(--color-text-muted)]">Generates context-aware markdown explanations and executable SQL remediation scripts.</p>
                </div>
              </li>
              <li className="flex items-start gap-4">
                <div className="flex-shrink-0 w-8 h-8 border border-[var(--color-border)] bg-[var(--color-bg)] flex items-center justify-center font-mono text-xs">OM</div>
                <div>
                  <h4 className="font-bold text-white mb-1">OpenMetadata Tagging</h4>
                  <p className="text-sm text-[var(--color-text-muted)]">Automatically applies `DataQuality.Critical` and `UnderInvestigation` tags to the blast radius.</p>
                </div>
              </li>
              <li className="flex items-start gap-4">
                <div className="flex-shrink-0 w-8 h-8 border border-[var(--color-border)] bg-[var(--color-bg)] flex items-center justify-center font-mono text-xs">WH</div>
                <div>
                  <h4 className="font-bold text-white mb-1">Slack & PagerDuty Webhooks</h4>
                  <p className="text-sm text-[var(--color-text-muted)]">Routes the complete incident report to the responsible data engineering team.</p>
                </div>
              </li>
            </ul>
          </div>
        </div>
      </section>

      {/* Recent Diagnoses Table */}
      {recent.length > 0 && (
        <section className="bg-[var(--color-bg)] pb-32">
          <div className="max-w-7xl mx-auto px-4 py-24 sm:px-6 lg:px-8">
            <div className="mb-8 flex items-end justify-between">
              <div>
                <h2 className="text-3xl font-header font-bold mb-2">Recent Executions</h2>
                <p className="text-[var(--color-text-muted)]">Historical root cause analyses executed in your browser.</p>
              </div>
            </div>
            
            <div className="border border-[var(--color-border)] overflow-hidden">
              <table className="min-w-full divide-y divide-[var(--color-border)]">
                <thead>
                  <tr className="bg-[rgba(255,255,255,0.02)]">
                    <th className="px-6 py-4 text-left label-mono text-[var(--color-text-muted)]">Incident ID</th>
                    <th className="px-6 py-4 text-left label-mono text-[var(--color-text-muted)]">Target Asset</th>
                    <th className="px-6 py-4 text-left label-mono text-[var(--color-text-muted)]">Severity</th>
                    <th className="px-6 py-4 text-left label-mono text-[var(--color-text-muted)]">Timestamp</th>
                    <th className="px-6 py-4 text-right label-mono text-[var(--color-text-muted)]">Action</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-[var(--color-border)]">
                  {recent.map((item, idx) => (
                    <tr key={idx} className="hover:bg-[rgba(255,255,255,0.02)] transition-colors">
                      <td className="px-6 py-4 whitespace-nowrap font-mono text-sm text-[var(--color-text-muted)]">{item.incident_id.split('-')[0]}</td>
                      <td className="px-6 py-4 font-mono text-sm text-white">{item.target_asset}</td>
                      <td className="px-6 py-4 whitespace-nowrap">
                        <span className={`inline-block w-2 h-2 mr-2 ${item.severity === 'HIGH' ? 'bg-[var(--color-high)]' : item.severity === 'MEDIUM' ? 'bg-[var(--color-medium)]' : 'bg-[var(--color-low)]'}`} />
                        <span className="text-sm font-mono">{item.severity}</span>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap font-mono text-sm text-[var(--color-text-muted)]">
                        {new Date(item.timestamp).toLocaleString()}
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-right">
                        <button
                          onClick={() => {
                            localStorage.setItem("datadoctor_current", JSON.stringify(item));
                            window.location.href = "/results";
                          }}
                          className="text-xs font-mono font-bold text-white hover:opacity-70 transition-opacity border-b border-[var(--color-border)] pb-0.5"
                        >
                          VIEW REPORT
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </section>
      )}
    </div>
  );
}
