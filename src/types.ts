import { DataQuery, DataSourceJsonData } from '@grafana/data';

// ==================== Query Types ====================

export enum QueryType {
  Logs = 'logs',
  Traces = 'traces',
  TraceId = 'traceId',
  Metrics = 'metrics',
}

export enum MetricAggType {
  Count = 'count',
  Avg = 'avg',
  Sum = 'sum',
  Min = 'min',
  Max = 'max',
  Percentiles = 'percentiles',
  Terms = 'terms',
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
  traceId?: string;
  serviceName?: string;
  operationName?: string;
  minDuration?: string;
  maxDuration?: string;
  traceLimit?: number;

  // Metric-specific fields
  /** Aggregation type */
  metricType?: string;
  /** Field to aggregate on (for avg/sum/min/max/percentiles) */
  metricField?: string;
  /** Group by field (date_histogram field) */
  groupBy?: string;
  /** Date histogram interval */
  groupByInterval?: string;
  /** Terms aggregation field (for top-N breakdown) */
  termsField?: string;
  /** Terms aggregation size */
  termsSize?: number;
  /** Sort order for results */
  metricSortOrder?: 'asc' | 'desc';

  // Internal: used by supplementary query
  _isLogsVolume?: boolean;
}

export const defaultQuery: Partial<QuickwitQuery> = {
  query: '*',
  index: '',
  queryType: QueryType.Logs,
  size: 100,
  sortOrder: 'desc',
  traceLimit: 20,
  metricType: MetricAggType.Count,
};

// ==================== Datasource Config ====================

export interface QuickwitOptions extends DataSourceJsonData {
  quickwitUrl?: string;
  defaultIndex?: string;
  logMessageField?: string;
  logLevelField?: string;
  traceIndex?: string;
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
  // Quickwit object fields may also have sub-fields
  record?: string;
  tokenizer?: string;
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
