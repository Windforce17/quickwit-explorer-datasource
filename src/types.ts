import { DataQuery, DataSourceJsonData } from '@grafana/data';

// ==================== Query Types ====================

export enum QueryType {
  Logs = 'logs',
  Traces = 'traces',
  TraceId = 'traceId',
  Metrics = 'metrics',
}

// ==================== Metric Aggregation Types ====================

export type MetricAggregationType = 'count' | 'avg' | 'sum' | 'min' | 'max' | 'percentiles' | 'cardinality';

export interface MetricAggregation {
  id: string;
  type: MetricAggregationType;
  field?: string;
  hide?: boolean;
  settings?: {
    percents?: string[];   // for percentiles
    missing?: string;
  };
}

// ==================== Bucket Aggregation Types ====================

export type BucketAggregationType = 'terms' | 'date_histogram';

export type TermsOrder = 'desc' | 'asc';

export interface BucketAggregation {
  id: string;
  type: BucketAggregationType;
  field?: string;
  settings?: {
    // Terms settings
    size?: string;
    order?: TermsOrder;
    orderBy?: string;        // '_count', '_key', or a metric id
    min_doc_count?: string;
    missing?: string;
    // Date histogram settings
    interval?: string;
    min_doc_count_hist?: string;
    offset?: string;
    timeZone?: string;
    trimEdges?: string;
  };
}

// ==================== Legacy MetricAggType (for backward compat) ====================

export enum MetricAggType {
  Count = 'count',
  Avg = 'avg',
  Sum = 'sum',
  Min = 'min',
  Max = 'max',
  Percentiles = 'percentiles',
  Terms = 'terms',
}

export enum MetricDisplayMode {
  TimeSeries = 'timeSeries',
  Table = 'table',
}

// ==================== Query Interface ====================

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

  // ==================== New Metrics Model ====================
  /** List of metric aggregations */
  metrics?: MetricAggregation[];
  /** List of bucket aggregations (group by layers) */
  bucketAggs?: BucketAggregation[];

  // ==================== Legacy Metric Fields (backward compat) ====================
  /** @deprecated Use metrics[] instead */
  metricType?: string;
  /** @deprecated Use metrics[].field instead */
  metricField?: string;
  /** @deprecated Use bucketAggs[] instead */
  groupBy?: string;
  /** Date histogram interval */
  groupByInterval?: string;
  /** @deprecated Use bucketAggs[] instead */
  termsField?: string;
  /** @deprecated Use bucketAggs[].settings.size instead */
  termsSize?: number;
  /** @deprecated Use bucketAggs[].settings.order instead */
  metricSortOrder?: 'asc' | 'desc';
  /** Display mode: time series graph or table */
  metricDisplayMode?: MetricDisplayMode;

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

// Default metrics configuration for new Metrics queries
export const defaultMetrics: MetricAggregation[] = [
  { id: '1', type: 'count' },
];

export const defaultBucketAggs: BucketAggregation[] = [
  { id: '2', type: 'date_histogram', field: '' },
];

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
