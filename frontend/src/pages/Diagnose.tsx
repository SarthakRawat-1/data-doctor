import { useState, useEffect } from "react";
import { useNavigate, useSearchParams } from "react-router-dom";
import { diagnose, runDemo, listDatasets, listDatasetFQNs, listScenarios } from "../api";
import type { DiagnosisRequest, DatasetInfo, FQNInfo, ScenarioInfo } from "../types";

export function Diagnose() {
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const isDemo = searchParams.get("demo") === "true";

  const [fqn, setFqn] = useState("");
  const [upstreamDepth, setUpstreamDepth] = useState(5);
  const [downstreamDepth, setDownstreamDepth] = useState(5);
  const [enhanceWithAi, setEnhanceWithAi] = useState(true);
  const [applyTags, setApplyTags] = useState(false);
  
  // Multi-tenant support
  const [useCustomOM, setUseCustomOM] = useState(false);
  const [omHostPort, setOmHostPort] = useState("");
  const [omJwtToken, setOmJwtToken] = useState("");
  
  // Interactive demo state
  const [showInteractiveDemo, setShowInteractiveDemo] = useState(false);
  const [datasets, setDatasets] = useState<DatasetInfo[]>([]);
  const [selectedDataset, setSelectedDataset] = useState<string>("");
  const [fqns, setFqns] = useState<FQNInfo[]>([]);
  const [selectedFqn, setSelectedFqn] = useState<string>("");
  const [scenarios, setScenarios] = useState<ScenarioInfo[]>([]);
  const [selectedScenario, setSelectedScenario] = useState<string>("clean");
  
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Load datasets and scenarios on mount
  useEffect(() => {
    async function loadDemoData() {
      try {
        const [datasetsRes, scenariosRes] = await Promise.all([
          listDatasets(),
          listScenarios()
        ]);
        setDatasets(datasetsRes.datasets);
        setScenarios(scenariosRes.scenarios);
      } catch (err) {
        console.error("Failed to load demo data:", err);
      }
    }
    loadDemoData();
  }, []);

  // Load FQNs when dataset OR scenario changes
  useEffect(() => {
    if (selectedDataset && selectedScenario) {
      async function loadFQNs() {
        try {
          const fqnsRes = await listDatasetFQNs(selectedDataset, selectedScenario);
          setFqns(fqnsRes.fqns);
          // Auto-select first FQN
          if (fqnsRes.fqns.length > 0) {
            setSelectedFqn(fqnsRes.fqns[0].fqn);
          }
        } catch (err) {
          console.error("Failed to load FQNs:", err);
        }
      }
      loadFQNs();
    }
  }, [selectedDataset, selectedScenario]);

  // eslint-disable-next-line react-hooks/exhaustive-deps
  useEffect(() => {
    if (isDemo) {
      handleDemo();
    }
  }, [isDemo]);

  async function handleDemo() {
    setLoading(true);
    setError(null);
    try {
      const res = await runDemo();
      localStorage.setItem("datadoctor_current", JSON.stringify(res.diagnosis));
      
      const stored = localStorage.getItem("datadoctor_recent");
      const recent = stored ? JSON.parse(stored) : [];
      localStorage.setItem("datadoctor_recent", JSON.stringify([res.diagnosis, ...recent].slice(0, 5)));
      
      navigate("/results");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to run demo scenario");
      setLoading(false);
    }
  }

  async function handleInteractiveDemoSubmit() {
    if (!selectedDataset) {
      setError("Please select a dataset");
      return;
    }
    if (!selectedFqn) {
      setError("Please select a table");
      return;
    }

    setLoading(true);
    setError(null);
    
    const req: DiagnosisRequest = {
      target_fqn: selectedFqn,
      upstream_depth: upstreamDepth,
      downstream_depth: downstreamDepth,
    };

    try {
      const res = await diagnose(req, enhanceWithAi, applyTags);
      localStorage.setItem("datadoctor_current", JSON.stringify(res));
      
      const stored = localStorage.getItem("datadoctor_recent");
      const recent = stored ? JSON.parse(stored) : [];
      localStorage.setItem("datadoctor_recent", JSON.stringify([res, ...recent].slice(0, 5)));
      
      navigate("/results");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Diagnosis failed");
      setLoading(false);
    }
  }

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (!fqn.trim()) {
      setError("Please enter a Target FQN");
      return;
    }

    // Validate custom OpenMetadata credentials if enabled
    if (useCustomOM) {
      if (!omHostPort.trim()) {
        setError("Please enter OpenMetadata Host URL");
        return;
      }
      if (!omJwtToken.trim()) {
        setError("Please enter JWT Token");
        return;
      }
    }

    setLoading(true);
    setError(null);
    
    const req: DiagnosisRequest = {
      target_fqn: fqn.trim(),
      upstream_depth: upstreamDepth,
      downstream_depth: downstreamDepth,
    };

    // Add custom OpenMetadata credentials if provided
    if (useCustomOM) {
      req.openmetadata_host_port = omHostPort.trim();
      req.openmetadata_jwt_token = omJwtToken.trim();
    }

    try {
      const res = await diagnose(req, enhanceWithAi, applyTags);
      localStorage.setItem("datadoctor_current", JSON.stringify(res));
      
      const stored = localStorage.getItem("datadoctor_recent");
      const recent = stored ? JSON.parse(stored) : [];
      localStorage.setItem("datadoctor_recent", JSON.stringify([res, ...recent].slice(0, 5)));
      
      navigate("/results");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Diagnosis failed");
      setLoading(false);
    }
  }

  if (loading) {
    return (
      <div className="max-w-4xl mx-auto px-4 py-32 sm:px-6 lg:px-8 text-center animate-fade-in">
        <div className="relative w-24 h-24 mx-auto mb-12">
          <div className="absolute inset-0 border-t-2 border-[var(--color-text)] rounded-full animate-spin"></div>
          <div className="absolute inset-2 border-r-2 border-[var(--color-text-muted)] rounded-full animate-spin" style={{ animationDirection: 'reverse', animationDuration: '1.5s' }}></div>
          <div className="absolute inset-4 border-b-2 border-blue-500 rounded-full animate-spin" style={{ animationDuration: '3s' }}></div>
        </div>
        <h2 className="font-header text-3xl font-bold mb-4">Executing Graph Traversal</h2>
        <p className="text-[var(--color-text-muted)] font-mono text-sm max-w-lg mx-auto">
          Querying OpenMetadata Lineage API. Evaluating nodes against 6 anomaly detectors. Calculating confidence scores...
        </p>
        
        <div className="mt-12 space-y-4 max-w-xl mx-auto text-left">
          <div className="h-1 w-full bg-[rgba(255,255,255,0.05)] overflow-hidden">
            <div className="h-full bg-white w-1/2 animate-[skeleton_1s_ease-in-out_infinite]" style={{ transformOrigin: 'left' }}></div>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="max-w-7xl mx-auto px-4 py-16 sm:px-6 lg:px-8 animate-fade-in min-h-screen">
      <div className="mb-12 border-b border-[var(--color-border)] pb-8">
        <h1 className="font-header text-4xl font-bold mb-3">Diagnostic Command Center</h1>
        <p className="text-[var(--color-text-muted)] text-lg">Configure traversal parameters and execute a deterministic root cause analysis.</p>
      </div>

      {error && (
        <div className="mb-8 p-4 bg-[var(--color-high)]/10 border border-[var(--color-high)] text-[var(--color-text)] text-sm font-medium flex items-center gap-3">
          <span className="font-bold">ERROR:</span> {error}
        </div>
      )}

      {/* Interactive Demo Section */}
      <div className="mb-12">
        <button
          onClick={() => setShowInteractiveDemo(!showInteractiveDemo)}
          className="w-full p-6 border-2 border-blue-500/30 bg-blue-500/5 hover:bg-blue-500/10 transition-colors flex items-center justify-between group"
        >
          <div className="text-left">
            <h2 className="font-header text-2xl font-bold text-white mb-2 flex items-center gap-3">
              <span className="text-3xl">🎮</span>
              Interactive Demo Mode
            </h2>
            <p className="text-[var(--color-text-muted)]">
              Explore 3 datasets with pre-configured anomaly scenarios
            </p>
          </div>
          <div className="text-blue-400 group-hover:text-blue-300 transition-colors">
            {showInteractiveDemo ? "▼" : "▶"}
          </div>
        </button>

        {showInteractiveDemo && (
          <div className="mt-6 p-8 border border-blue-500/30 bg-[rgba(59,130,246,0.02)] space-y-8">
            {/* Dataset Selector */}
            <div className="space-y-4">
              <label className="block label-mono text-[var(--color-text-muted)]">
                1. SELECT DATASET
              </label>
              <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                {datasets.map((dataset) => (
                  <button
                    key={dataset.id}
                    onClick={() => setSelectedDataset(dataset.id)}
                    className={`p-6 border-2 transition-all text-left ${
                      selectedDataset === dataset.id
                        ? "border-blue-500 bg-blue-500/10"
                        : "border-[var(--color-border)] bg-[var(--color-bg-alt)] hover:border-blue-500/50"
                    }`}
                  >
                    <div className="text-4xl mb-3">{dataset.icon}</div>
                    <h3 className="font-bold text-white mb-2">{dataset.name}</h3>
                    <p className="text-sm text-[var(--color-text-muted)] mb-3">
                      {dataset.description}
                    </p>
                    <div className="text-xs font-mono text-blue-400">
                      {dataset.table_count} tables
                    </div>
                  </button>
                ))}
              </div>
            </div>

            {/* FQN Selector */}
            {selectedDataset && fqns.length > 0 && (
              <div className="space-y-4">
                <label className="block label-mono text-[var(--color-text-muted)]">
                  2. SELECT TABLE TO DIAGNOSE
                </label>
                <div className="space-y-2">
                  {fqns.map((fqnInfo) => (
                    <button
                      key={fqnInfo.fqn}
                      onClick={() => setSelectedFqn(fqnInfo.fqn)}
                      className={`w-full p-4 border transition-all text-left ${
                        selectedFqn === fqnInfo.fqn
                          ? "border-blue-500 bg-blue-500/10"
                          : "border-[var(--color-border)] bg-[var(--color-bg-alt)] hover:border-blue-500/50"
                      }`}
                    >
                      <div className="flex items-start justify-between gap-4">
                        <div className="flex-1">
                          <h4 className="font-mono text-sm text-white mb-1">
                            {fqnInfo.table_name}
                          </h4>
                          <p className="text-xs text-[var(--color-text-muted)] mb-2">
                            {fqnInfo.description}
                          </p>
                          <p className="text-xs font-mono text-blue-400">
                            {fqnInfo.fqn}
                          </p>
                        </div>
                        {fqnInfo.row_count && (
                          <div className="text-xs font-mono text-[var(--color-text-muted)]">
                            {fqnInfo.row_count.toLocaleString()} rows
                          </div>
                        )}
                      </div>
                    </button>
                  ))}
                </div>
              </div>
            )}

            {/* Scenario Selector */}
            {selectedFqn && (
              <div className="space-y-4">
                <label className="block label-mono text-[var(--color-text-muted)]">
                  3. SELECT ANOMALY SCENARIO
                </label>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                  {scenarios.map((scenario) => (
                    <button
                      key={scenario.id}
                      onClick={() => setSelectedScenario(scenario.id)}
                      className={`p-4 border transition-all text-left ${
                        selectedScenario === scenario.id
                          ? "border-blue-500 bg-blue-500/10"
                          : "border-[var(--color-border)] bg-[var(--color-bg-alt)] hover:border-blue-500/50"
                      }`}
                    >
                      <div className="flex items-start justify-between gap-3 mb-2">
                        <h4 className="font-bold text-white text-sm">{scenario.name}</h4>
                        <span
                          className={`text-xs font-mono px-2 py-0.5 ${
                            scenario.severity === "HIGH"
                              ? "bg-red-500/20 text-red-400"
                              : scenario.severity === "MEDIUM"
                              ? "bg-yellow-500/20 text-yellow-400"
                              : "bg-green-500/20 text-green-400"
                          }`}
                        >
                          {scenario.severity}
                        </span>
                      </div>
                      <p className="text-xs text-[var(--color-text-muted)] mb-2">
                        {scenario.description}
                      </p>
                      {scenario.anomaly_types.length > 0 && (
                        <div className="flex flex-wrap gap-1">
                          {scenario.anomaly_types.map((type) => (
                            <span
                              key={type}
                              className="text-xs font-mono px-2 py-0.5 bg-white/5 text-blue-400"
                            >
                              {type}
                            </span>
                          ))}
                        </div>
                      )}
                    </button>
                  ))}
                </div>
              </div>
            )}

            {/* Execute Button */}
            {selectedFqn && (
              <div className="pt-4 border-t border-blue-500/30">
                <div className="flex items-center justify-between">
                  <div className="text-sm text-[var(--color-text-muted)] font-mono">
                    Ready to diagnose: <span className="text-white">{selectedFqn}</span>
                  </div>
                  <button
                    onClick={handleInteractiveDemoSubmit}
                    className="minimal-button px-8 py-3 label-mono text-sm bg-blue-500/10 border-blue-500 hover:bg-blue-500/20"
                  >
                    RUN INTERACTIVE DEMO
                  </button>
                </div>
                {selectedScenario !== "clean" && (
                  <div className="mt-4 p-3 bg-blue-500/10 border border-blue-500/30 text-xs text-blue-400 font-mono">
                    ℹ️ Scenario "{selectedScenario}" is pre-configured with anomalies. Results will show detected issues instantly.
                  </div>
                )}
              </div>
            )}
          </div>
        )}
      </div>

      {/* Divider */}
      <div className="mb-12 flex items-center gap-4">
        <div className="flex-1 border-t border-[var(--color-border)]"></div>
        <span className="text-[var(--color-text-muted)] font-mono text-sm">OR USE MANUAL MODE</span>
        <div className="flex-1 border-t border-[var(--color-border)]"></div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-12">
        {/* Left Column: Configuration Form */}
        <div className="lg:col-span-2">
          <form onSubmit={handleSubmit} className="space-y-10">
            {/* Target FQN */}
            <div className="space-y-3">
              <label htmlFor="fqn" className="block label-mono text-[var(--color-text-muted)]">Target Entity FQN</label>
              <div className="relative">
                <span className="absolute left-4 top-1/2 -translate-y-1/2 text-[var(--color-text-muted)] font-mono font-bold">{'>'}</span>
                <input
                  id="fqn"
                  type="text"
                  className="w-full bg-[var(--color-bg-alt)] border border-[var(--color-border)] text-white font-mono text-sm py-4 pl-10 pr-4 focus:outline-none focus:border-white transition-colors"
                  placeholder="e.g. snowflake.analytics.public.dim_customer"
                  value={fqn}
                  onChange={(e) => setFqn(e.target.value)}
                  autoComplete="off"
                />
              </div>
            </div>

            {/* Depth Sliders */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
              <div className="space-y-4 p-6 border border-[var(--color-border)] bg-[rgba(255,255,255,0.02)]">
                <div className="flex justify-between items-center">
                  <label className="label-mono text-[var(--color-text-muted)]">Upstream Depth</label>
                  <span className="font-mono font-bold text-white bg-white/10 px-2 py-0.5 border border-white/20">{upstreamDepth}</span>
                </div>
                <input
                  type="range"
                  min="1" max="10"
                  value={upstreamDepth}
                  onChange={(e) => setUpstreamDepth(parseInt(e.target.value))}
                  className="w-full h-1 bg-[var(--color-border)] rounded-none appearance-none cursor-pointer accent-white"
                />
                <p className="text-xs text-[var(--color-text-muted)] font-mono">Root cause search distance.</p>
              </div>

              <div className="space-y-4 p-6 border border-[var(--color-border)] bg-[rgba(255,255,255,0.02)]">
                <div className="flex justify-between items-center">
                  <label className="label-mono text-[var(--color-text-muted)]">Downstream Depth</label>
                  <span className="font-mono font-bold text-white bg-white/10 px-2 py-0.5 border border-white/20">{downstreamDepth}</span>
                </div>
                <input
                  type="range"
                  min="1" max="10"
                  value={downstreamDepth}
                  onChange={(e) => setDownstreamDepth(parseInt(e.target.value))}
                  className="w-full h-1 bg-[var(--color-border)] rounded-none appearance-none cursor-pointer accent-white"
                />
                <p className="text-xs text-[var(--color-text-muted)] font-mono">Blast radius search distance.</p>
              </div>
            </div>

            {/* Feature Toggles */}
            <div className="space-y-6">
              <h3 className="label-mono text-[var(--color-text-muted)] border-b border-[var(--color-border)] pb-2">Execution Modules</h3>
              
              {/* Custom OpenMetadata Toggle */}
              <div className="flex items-center justify-between p-6 border border-[var(--color-border)] hover:border-white/30 transition-colors bg-[var(--color-bg-alt)]">
                <div>
                  <h4 className="font-bold text-white mb-1">Use Custom OpenMetadata</h4>
                  <p className="text-sm text-[var(--color-text-muted)]">Connect to your own OpenMetadata instance instead of server default.</p>
                </div>
                <button
                  type="button"
                  onClick={() => setUseCustomOM(!useCustomOM)}
                  className={`relative inline-flex h-6 w-11 items-center rounded-full transition-colors focus:outline-none ${useCustomOM ? 'bg-purple-500' : 'bg-gray-700'}`}
                >
                  <span className={`inline-block h-4 w-4 transform rounded-full bg-white transition-transform ${useCustomOM ? 'translate-x-6' : 'translate-x-1'}`} />
                </button>
              </div>

              {/* Custom OpenMetadata Credentials (Conditional) */}
              {useCustomOM && (
                <div className="space-y-4 p-6 border border-purple-500/30 bg-purple-500/5">
                  <div className="space-y-2">
                    <label htmlFor="omHost" className="block label-mono text-[var(--color-text-muted)] text-xs">OpenMetadata API URL</label>
                    <input
                      id="omHost"
                      type="text"
                      className="w-full bg-[var(--color-bg-alt)] border border-[var(--color-border)] text-white font-mono text-xs py-3 px-4 focus:outline-none focus:border-purple-500 transition-colors"
                      placeholder="https://sandbox.open-metadata.org/api"
                      value={omHostPort}
                      onChange={(e) => setOmHostPort(e.target.value)}
                      autoComplete="off"
                    />
                  </div>
                  <div className="space-y-2">
                    <label htmlFor="omToken" className="block label-mono text-[var(--color-text-muted)] text-xs">JWT Token</label>
                    <textarea
                      id="omToken"
                      className="w-full bg-[var(--color-bg-alt)] border border-[var(--color-border)] text-white font-mono text-xs py-3 px-4 focus:outline-none focus:border-purple-500 transition-colors resize-none"
                      placeholder="eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9..."
                      rows={3}
                      value={omJwtToken}
                      onChange={(e) => setOmJwtToken(e.target.value)}
                    />
                  </div>
                  <p className="text-xs text-purple-400 font-mono">
                    💡 Get your JWT token from OpenMetadata: Settings → Bots → ingestion-bot
                  </p>
                </div>
              )}
              
              <div className="flex items-center justify-between p-6 border border-[var(--color-border)] hover:border-white/30 transition-colors bg-[var(--color-bg-alt)]">
                <div>
                  <h4 className="font-bold text-white mb-1">AI Remediation Engine</h4>
                  <p className="text-sm text-[var(--color-text-muted)]">Utilize Groq LLMs to generate context-aware SQL fixes.</p>
                </div>
                <button
                  type="button"
                  onClick={() => setEnhanceWithAi(!enhanceWithAi)}
                  className={`relative inline-flex h-6 w-11 items-center rounded-full transition-colors focus:outline-none ${enhanceWithAi ? 'bg-blue-500' : 'bg-gray-700'}`}
                >
                  <span className={`inline-block h-4 w-4 transform rounded-full bg-white transition-transform ${enhanceWithAi ? 'translate-x-6' : 'translate-x-1'}`} />
                </button>
              </div>

              <div className="flex items-center justify-between p-6 border border-[var(--color-border)] hover:border-white/30 transition-colors bg-[var(--color-bg-alt)]">
                <div>
                  <h4 className="font-bold text-white mb-1">Automated OM Tagging</h4>
                  <p className="text-sm text-[var(--color-text-muted)]">Write `DataQuality.Critical` tags back to OpenMetadata for impacted assets.</p>
                </div>
                <button
                  type="button"
                  onClick={() => setApplyTags(!applyTags)}
                  className={`relative inline-flex h-6 w-11 items-center rounded-full transition-colors focus:outline-none ${applyTags ? 'bg-red-500' : 'bg-gray-700'}`}
                >
                  <span className={`inline-block h-4 w-4 transform rounded-full bg-white transition-transform ${applyTags ? 'translate-x-6' : 'translate-x-1'}`} />
                </button>
              </div>
            </div>

            <div className="pt-8 flex items-center justify-between">
              <button
                type="button"
                onClick={handleDemo}
                className="text-sm font-mono font-medium text-[var(--color-text-muted)] hover:text-white transition-colors underline decoration-[var(--color-border)] underline-offset-4"
              >
                [ RUN DEMO SCENARIO ]
              </button>
              <button
                type="submit"
                className="minimal-button px-10 py-4 label-mono text-sm"
              >
                EXECUTE DIAGNOSIS
              </button>
            </div>
          </form>
        </div>

        {/* Right Column: Dynamic Execution Plan */}
        <div className="lg:col-span-1">
          <div className="sticky top-28 border border-[var(--color-border)] bg-[#050505] overflow-hidden">
            <div className="bg-[#111] border-b border-[var(--color-border)] px-4 py-3 flex items-center gap-2">
              <div className="w-3 h-3 rounded-full bg-[#FF5F56]"></div>
              <div className="w-3 h-3 rounded-full bg-[#FFBD2E]"></div>
              <div className="w-3 h-3 rounded-full bg-[#27C93F]"></div>
              <span className="ml-4 font-mono text-xs text-[var(--color-text-muted)]">execution_plan.log</span>
            </div>
            
            <div className="p-6 font-mono text-xs leading-relaxed space-y-4">
              <div>
                <span className="text-blue-400">INFO</span> [Init] Preparing diagnostic sequence...
              </div>
              
              {fqn ? (
                <div>
                  <span className="text-green-400">TARGET</span> Resolved FQN: <br/>
                  <span className="text-white break-all">{fqn}</span>
                </div>
              ) : (
                <div className="text-yellow-400">
                  <span className="text-yellow-400">WARN</span> Awaiting Target FQN...
                </div>
              )}

              <div className="border-t border-dashed border-white/10 pt-4">
                <span className="text-purple-400">GRAPH</span> Configuring BFS Traversal:
                <ul className="mt-2 space-y-1 text-[var(--color-text-muted)]">
                  <li>Upstream Depth: <span className="text-white">{upstreamDepth}</span> <span className="opacity-50">(~{Math.pow(2, upstreamDepth)} nodes max)</span></li>
                  <li>Downstream Depth: <span className="text-white">{downstreamDepth}</span> <span className="opacity-50">(~{Math.pow(2, downstreamDepth)} nodes max)</span></li>
                </ul>
              </div>

              <div className="border-t border-dashed border-white/10 pt-4">
                <span className="text-teal-400">MODULES</span> Active plugins:
                <ul className="mt-2 space-y-1">
                  <li className={enhanceWithAi ? "text-white" : "text-gray-600 line-through"}>
                    &gt; Groq AI Remediation
                  </li>
                  <li className={applyTags ? "text-white" : "text-gray-600 line-through"}>
                    &gt; OM Auto-Tagging
                  </li>
                </ul>
              </div>

              <div className="pt-4 flex items-center gap-2 text-[var(--color-text-muted)]">
                <span className="animate-pulse">_</span>
                <span>Ready to execute.</span>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
