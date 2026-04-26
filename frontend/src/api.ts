import type {
  DiagnosisRequest,
  DiagnosisResponse,
  DemoScenarioResponse,
  HealthCheckResponse,
} from "./types";

const API_BASE = import.meta.env.VITE_API_URL || "http://localhost:8000";

async function request<T>(url: string, options?: RequestInit): Promise<T> {
  const res = await fetch(`${API_BASE}${url}`, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    throw new Error(body.detail || `Request failed: ${res.status}`);
  }
  return res.json();
}

export async function healthCheck(): Promise<HealthCheckResponse> {
  return request("/api/v1/health");
}

export async function diagnose(
  req: DiagnosisRequest,
  enhanceWithAi = false,
  applyGovernanceTags = false
): Promise<DiagnosisResponse> {
  const params = new URLSearchParams();
  if (enhanceWithAi) params.set("enhance_with_ai", "true");
  if (applyGovernanceTags) params.set("apply_governance_tags", "true");
  const qs = params.toString() ? `?${params.toString()}` : "";
  return request(`/api/v1/diagnose${qs}`, {
    method: "POST",
    body: JSON.stringify(req),
  });
}

export async function runDemo(): Promise<DemoScenarioResponse> {
  return request("/api/v1/demo");
}
