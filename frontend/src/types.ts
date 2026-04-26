export type Severity = "HIGH" | "MEDIUM" | "LOW";

export type AnomalyType =
  | "pipeline_failure"
  | "stale_data"
  | "schema_change"
  | "data_quality_failure"
  | "volume_anomaly"
  | "distribution_drift";

export type FixAction =
  | "rerun_pipeline"
  | "update_schema"
  | "force_backfill"
  | "quarantine_data";

export interface AnomalyDetail {
  type: AnomalyType;
  name: string;
  depth: number;
  entity_id?: string;
  entity_type?: string;
  description?: string;
}

export interface ImpactedAssets {
  tables: Record<string, unknown>[];
  dashboards: Record<string, unknown>[];
  ml_models: Record<string, unknown>[];
  total_impact_count: number;
}

export interface SuggestedFix {
  action: FixAction;
  target: string;
  description: string;
  sql_script?: string;
  markdown_details?: string;
}

export interface DiagnosisRequest {
  target_fqn: string;
  upstream_depth: number;
  downstream_depth: number;
  // Multi-tenant support: Optional OpenMetadata credentials
  openmetadata_host_port?: string;
  openmetadata_jwt_token?: string;
}

export interface DiagnosisResponse {
  incident_id: string;
  target_asset: string;
  severity: Severity;
  confidence_score: number;
  primary_root_cause: AnomalyDetail | null;
  contributing_factors: AnomalyDetail[];
  impacted_assets: ImpactedAssets;
  suggested_fixes: SuggestedFix[];
  timestamp: string;
  execution_time_ms: number | null;
}

export interface HealthCheckResponse {
  status: string;
  app_name: string;
  version: string;
  openmetadata_connected: boolean;
  timestamp: string;
}

export interface DemoScenarioResponse {
  message: string;
  demo_fqn: string;
  diagnosis: DiagnosisResponse;
}

// Interactive Demo Types
export interface DatasetInfo {
  id: string;
  name: string;
  description: string;
  service_name: string;
  database_name: string;
  table_count: number;
  icon: string;
}

export interface FQNInfo {
  fqn: string;
  table_name: string;
  description: string;
  row_count: number | null;
}

export interface ScenarioInfo {
  id: string;
  name: string;
  description: string;
  anomaly_types: string[];
  severity: string;
}

export interface DatasetsResponse {
  datasets: DatasetInfo[];
}

export interface FQNsResponse {
  dataset_id: string;
  fqns: FQNInfo[];
}

export interface ScenariosResponse {
  scenarios: ScenarioInfo[];
}
