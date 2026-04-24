import { DataQuery, DataSourceJsonData } from '@grafana/data';

// ==================== Query Types ====================

export enum QueryType {
  Logs = 'logs',
  Traces = 'traces',
  TraceId = 'traceId',
  Metrics = 'metrics',
}

export interface QuickwitQuery extends DataQuery {
  /** Lucene query string */
  query: string;
  /** Target index ID */
  index: string;
  /** Query type selector */
  queryType: QueryType;
  /** Max results for log queries */
  size: number;
  /** Sort field */
  sortField?: string;
  /** Sort order */
  sortOrder?: 'asc' | 'desc';

  // Trace-specific fields
  /** Direct trace ID lookup */
  traceId?: string;
  /** Service name filter */
  serviceName?: string;
  /** Operation name filter */
  operationName?: string;
  /** Min duration filter (e.g. "100ms") */
  minDuration?: string;
  /** Max duration filter (e.g. "5s") */
  maxDuration?: string;
  /** Max number of traces to return */
  traceLimit?: number;

  // Metric-specific fields
  /** Aggregation metric type */
  metricType?: string;
  /** Group by field */
  groupBy?: string;
  /** Date histogram interval */
  groupByInterval?: string;
}

export const defaultQuery: Partial<QuickwitQuery> = {
  query: '*',
  index: '',
  queryType: QueryType.Logs,
  size: 100,
  sortOrder: 'desc',
  traceLimit: 20,
};

// ==================== Datasource Config ====================

export interface QuickwitOptions extends DataSourceJsonData {
  /** Quickwit base URL for proxy routing */
  quickwitUrl?: string;
  /** Default index */
  defaultIndex?: string;
  /** Log message field name */
  logMessageField?: string;
  /** Log level field name */
  logLevelField?: string;
  /** Trace index name */
  traceIndex?: string;
  /** Log index name */
  logIndex?: string;
}

export interface QuickwitSecureJsonData {
  apiKey?: string;
}

// ==================== Quickwit API Types ====================

export interface QwIndexConfig {
  index_id: string;
  index_uri?: string;
  doc_mapping: {
    field_mappings: QwFieldMapping[];
    timestamp_field?: string;
    tag_fields?: string[];
  };
  search_settings?: {
    default_search_fields?: string[];
  };
}

export interface QwIndex {
  index_config: QwIndexConfig;
  checkpoint?: Record<string, any>;
  create_timestamp?: number;
}

export interface QwFieldMapping {
  name: string;
  type: string;
  fast?: boolean;
  stored?: boolean;
  indexed?: boolean;
  field_mappings?: QwFieldMapping[];
}

export interface QwSearchResponse {
  hits: any[];
  num_hits: number;
  elapsed_time_micros: number;
  errors?: string[];
  aggregations?: Record<string, any>;
}

// ==================== Jaeger API Types ====================

export interface JaegerTrace {
  traceID: string;
  spans: JaegerSpan[];
  processes: Record<string, JaegerProcess>;
  warnings: string[] | null;
}

export interface JaegerSpan {
  traceID: string;
  spanID: string;
  operationName: string;
  references: JaegerReference[];
  startTime: number;
  duration: number;
  tags: JaegerKV[];
  logs: JaegerLog[];
  processID: string;
  warnings: string[] | null;
}

export interface JaegerReference {
  refType: string;
  traceID: string;
  spanID: string;
}

export interface JaegerProcess {
  serviceName: string;
  tags: JaegerKV[];
}

export interface JaegerKV {
  key: string;
  type: string;
  value: any;
}

export interface JaegerLog {
  timestamp: number;
  fields: JaegerKV[];
}

export interface JaegerApiResponse<T> {
  data: T;
  total?: number;
  limit?: number;
  offset?: number;
  errors?: string[] | null;
}
