import {
  DataQueryRequest,
  DataQueryResponse,
  DataSourceApi,
  DataSourceInstanceSettings,
  FieldType,
  MutableDataFrame,
} from '@grafana/data';
import { getBackendSrv, getTemplateSrv } from '@grafana/runtime';
import {
  QuickwitQuery,
  QuickwitOptions,
  QwIndex,
  QwSearchResponse,
  QwFieldMapping,
  JaegerTrace,
  JaegerApiResponse,
  QueryType,
  MetricAggType,
  defaultQuery,
} from './types';

/** Default timeout for trace lookups (ms) */
const TRACE_TIMEOUT_MS = 5000;

/**
 * Compute a reasonable histogram interval string based on the time range.
 */
function computeAutoInterval(fromMs: number, toMs: number): string {
  const rangeSec = (toMs - fromMs) / 1000;
  if (rangeSec <= 300) return '5s';
  if (rangeSec <= 1800) return '15s';
  if (rangeSec <= 7200) return '1m';
  if (rangeSec <= 21600) return '5m';
  if (rangeSec <= 86400) return '15m';
  if (rangeSec <= 604800) return '1h';
  if (rangeSec <= 2592000) return '6h';
  return '1d';
}

/**
 * Detect the timestamp field name from index metadata.
 * Falls back through common field names.
 */
function detectTimestampField(indexes: QwIndex[], indexId: string): string {
  // Try exact match first
  let idx = indexes.find((i) => i.index_config.index_id === indexId);

  // Try wildcard pattern match (e.g. "otel-logs-v0_*" matches "otel-logs-v0_7")
  if (!idx && indexId.includes('*')) {
    const prefix = indexId.replace(/\*/g, '');
    idx = indexes.find((i) => i.index_config.index_id.startsWith(prefix));
  }

  if (idx?.index_config?.doc_mapping?.timestamp_field) {
    return idx.index_config.doc_mapping.timestamp_field;
  }

  // Fallback: look for common timestamp field names in field mappings
  if (idx) {
    const allFields = flattenFieldMappings(idx.index_config.doc_mapping.field_mappings);
    const commonTsNames = ['timestamp', '_timestamp', '@timestamp', 'timestamp_nanos'];
    for (const name of commonTsNames) {
      if (allFields.includes(name)) {
        return name;
      }
    }
  }

  return 'timestamp';
}

/**
 * Flatten field mappings recursively, handling both nested field_mappings
 * and object-type fields (type: "object").
 */
function flattenFieldMappings(mappings: QwFieldMapping[], prefix = ''): string[] {
  const fields: string[] = [];
  if (!mappings) return fields;

  for (const m of mappings) {
    const fullName = prefix ? `${prefix}.${m.name}` : m.name;

    // If this field has sub-fields (nested object or explicit field_mappings)
    if (m.field_mappings && m.field_mappings.length > 0) {
      // Also add the parent as a field (some queries use the parent path)
      fields.push(fullName);
      fields.push(...flattenFieldMappings(m.field_mappings, fullName));
    } else if (m.type === 'object' && m.field_mappings) {
      fields.push(...flattenFieldMappings(m.field_mappings, fullName));
    } else {
      fields.push(fullName);
    }
  }
  return fields;
}

/**
 * Flatten a JSON object into dot-notation key-value pairs for log labels.
 * e.g. { httpRequest: { clientIp: "1.2.3.4" } } => { "httpRequest.clientIp": "1.2.3.4" }
 */
function flattenObject(obj: any, prefix = ''): Record<string, string> {
  const result: Record<string, string> = {};
  if (!obj || typeof obj !== 'object') return result;

  for (const key of Object.keys(obj)) {
    const fullKey = prefix ? `${prefix}.${key}` : key;
    const val = obj[key];

    if (val !== null && typeof val === 'object' && !Array.isArray(val)) {
      Object.assign(result, flattenObject(val, fullKey));
    } else if (Array.isArray(val)) {
      result[fullKey] = JSON.stringify(val);
    } else if (val !== undefined && val !== null) {
      result[fullKey] = String(val);
    }
  }
  return result;
}

function extractTimestamp(hit: any): number {
  const tsFields = [
    'timestamp', '_timestamp', 'timestamp_nanos', '@timestamp',
    'span_start_timestamp_nanos',
  ];
  for (const field of tsFields) {
    const val = getNestedValue(hit, field);
    if (val !== undefined && val !== null) {
      const num = Number(val);
      if (!isNaN(num)) {
        if (num > 1e18) return Math.floor(num / 1e6);
        if (num > 1e15) return Math.floor(num / 1e3);
        if (num > 1e12) return num;
        return num * 1000;
      }
      const d = new Date(val);
      if (!isNaN(d.getTime())) {
        return d.getTime();
      }
    }
  }
  return Date.now();
}

function getNestedValue(obj: any, path: string): any {
  if (!path) return undefined;
  const parts = path.split('.');
  let current = obj;
  for (const part of parts) {
    if (current == null) return undefined;
    current = current[part];
  }
  return current;
}

/**
 * Convert a bucket key to milliseconds, handling various formats Quickwit may return.
 */
function bucketKeyToMs(key: number): number {
  // Microseconds (> year 2100 in ms)
  if (key > 4102444800000) {
    return key / 1000;
  }
  // Seconds (< year 2100 in seconds)
  if (key < 4102444800) {
    return key * 1000;
  }
  // Already milliseconds
  return key;
}

// ================================================================
//  MAIN DATASOURCE CLASS
// ================================================================

export class QuickwitExplorerDatasource extends DataSourceApi<QuickwitQuery, QuickwitOptions> {
  baseUrl: string;
  quickwitUrl: string;
  defaultIndex: string;
  logMessageField: string;
  logLevelField: string;
  traceIndex: string;
  logIndex: string;

  private indexCache: { data: QwIndex[]; ts: number } | null = null;
  private readonly INDEX_CACHE_TTL = 30000;
  private fieldCache: Map<string, { data: string[]; ts: number }> = new Map();
  private readonly FIELD_CACHE_TTL = 60000;

  constructor(instanceSettings: DataSourceInstanceSettings<QuickwitOptions>) {
    super(instanceSettings);
    this.baseUrl = instanceSettings.url ? `${instanceSettings.url}/qw` : '';
    this.quickwitUrl = instanceSettings.jsonData.quickwitUrl || '';
    this.defaultIndex = instanceSettings.jsonData.defaultIndex || '';
    this.logMessageField = instanceSettings.jsonData.logMessageField || 'body';
    this.logLevelField = instanceSettings.jsonData.logLevelField || 'severity_text';
    this.traceIndex = instanceSettings.jsonData.traceIndex || '';
    this.logIndex = instanceSettings.jsonData.logIndex || '';
  }

  // ================================================================
  //  MAIN QUERY DISPATCHER
  // ================================================================

  async query(options: DataQueryRequest<QuickwitQuery>): Promise<DataQueryResponse> {
    const promises = options.targets
      .filter((t) => !t.hide)
      .map((target) => {
        const q = { ...defaultQuery, ...target } as QuickwitQuery;
        switch (q.queryType) {
          case QueryType.Traces:
            return this.queryTraceSearch(q, options);
          case QueryType.TraceId:
            return this.queryTraceById(q, options);
          case QueryType.Metrics:
            return this.queryMetrics(q, options);
          case QueryType.Logs:
          default:
            return this.queryLogs(q, options);
        }
      });

    const results = await Promise.all(promises);
    return { data: results.flat() };
  }

  // ================================================================
  //  INDEX DISCOVERY
  // ================================================================

  async getIndexes(): Promise<QwIndex[]> {
    const now = Date.now();
    if (this.indexCache && now - this.indexCache.ts < this.INDEX_CACHE_TTL) {
      return this.indexCache.data;
    }
    try {
      const data = await this.get<QwIndex[]>('/api/v1/indexes');
      this.indexCache = { data: data || [], ts: now };
      return this.indexCache.data;
    } catch (e) {
      console.error('Failed to fetch indexes:', e);
      return this.indexCache?.data || [];
    }
  }

  async searchIndexes(query: string): Promise<string[]> {
    const indexes = await this.getIndexes();
    const pattern = query.toLowerCase();
    return indexes
      .map((idx) => idx.index_config.index_id)
      .filter((id) => !pattern || id.toLowerCase().includes(pattern))
      .sort();
  }

  async getFields(indexId: string): Promise<string[]> {
    if (!indexId) return [];
    const now = Date.now();
    const cached = this.fieldCache.get(indexId);
    if (cached && now - cached.ts < this.FIELD_CACHE_TTL) {
      return cached.data;
    }

    const indexes = await this.getIndexes();
    const idx = indexes.find((i) => i.index_config.index_id === indexId);
    if (!idx) return [];

    const fields = flattenFieldMappings(idx.index_config.doc_mapping.field_mappings);
    this.fieldCache.set(indexId, { data: fields, ts: now });
    return fields;
  }

  // ================================================================
  //  LOG QUERIES (with embedded histogram)
  // ================================================================

  private async queryLogs(
    query: QuickwitQuery,
    options: DataQueryRequest<QuickwitQuery>
  ): Promise<MutableDataFrame[]> {
    const index = query.index || this.logIndex || this.defaultIndex;
    if (!index) return [];

    const fromMs = options.range.from.valueOf();
    const toMs = options.range.to.valueOf();
    const from = fromMs / 1000;
    const to = toMs / 1000;
    const lucene = getTemplateSrv().replace(query.query || '*', options.scopedVars);

    const indexes = await this.getIndexes();
    const tsField = detectTimestampField(indexes, index);
    const interval = computeAutoInterval(fromMs, toMs);

    // Build request body: search + histogram aggregation in ONE request
    const body: Record<string, any> = {
      query: lucene,
      max_hits: query.size || 100,
      start_timestamp: Math.floor(from),
      end_timestamp: Math.ceil(to),
      sort_by: `${tsField}:desc`,
      aggs: {
        time_histogram: {
          date_histogram: {
            field: tsField,
            fixed_interval: interval,
          },
        },
      },
    };

    const resp = await this.post<QwSearchResponse>(`/api/v1/${index}/search`, body);
    const frames: MutableDataFrame[] = [];

    // --- Frame 1: Log entries ---
    if (resp?.hits?.length) {
      const msgField = this.logMessageField;
      const lvlField = this.logLevelField;
      const hasTraceIndex = !!this.traceIndex;

      // Collect all unique label keys from all hits for dynamic columns
      const allLabelKeys = new Set<string>();
      const flattenedHits: Array<Record<string, string>> = [];

      for (const hit of resp.hits) {
        const flat = flattenObject(hit);
        flattenedHits.push(flat);
        for (const key of Object.keys(flat)) {
          allLabelKeys.add(key);
        }
      }

      const frame = new MutableDataFrame({
        refId: query.refId,
        meta: { preferredVisualisationType: 'logs' },
        fields: [
          { name: 'timestamp', type: FieldType.time },
          { name: 'body', type: FieldType.string },
          { name: 'severity', type: FieldType.string },
          { name: 'id', type: FieldType.string },
          { name: 'traceID', type: FieldType.string },
          { name: 'spanID', type: FieldType.string },
          { name: 'labels', type: FieldType.other },
        ],
      });

      for (let i = 0; i < resp.hits.length; i++) {
        const hit = resp.hits[i];
        const flat = flattenedHits[i];
        const ts = extractTimestamp(hit);
        const msg = getNestedValue(hit, msgField);
        const level = getNestedValue(hit, lvlField) || '';
        const traceId = getNestedValue(hit, 'trace_id') || '';
        const spanId = getNestedValue(hit, 'span_id') || '';

        frame.add({
          timestamp: ts,
          body: typeof msg === 'string' ? msg : JSON.stringify(hit),
          severity: String(level),
          id: `${ts}-${traceId}-${spanId}-${i}`,
          traceID: traceId,
          spanID: spanId,
          labels: flat,
        });
      }

      // Trace link on traceID field
      if (hasTraceIndex) {
        const traceIdField = frame.fields.find((f) => f.name === 'traceID');
        if (traceIdField) {
          traceIdField.config = {
            ...traceIdField.config,
            links: [{
              title: 'View Trace',
              url: '',
              internal: {
                datasourceUid: this.uid,
                datasourceName: this.name,
                query: {
                  refId: 'trace-link',
                  queryType: QueryType.TraceId,
                  query: '',
                  index: this.traceIndex,
                  traceId: '${__value.raw}',
                  size: 100,
                } as any,
              },
            }],
          };
        }
      }

      // Quick-filter link on severity field
      const severityField = frame.fields.find((f) => f.name === 'severity');
      if (severityField) {
        severityField.config = {
          ...severityField.config,
          links: [{
            title: 'Filter by severity: ${__value.raw}',
            url: '',
            internal: {
              datasourceUid: this.uid,
              datasourceName: this.name,
              query: {
                refId: query.refId,
                queryType: QueryType.Logs,
                query: `${lvlField}:\${__value.raw}`,
                index: index,
                size: query.size || 100,
              } as any,
            },
          }],
        };
      }

      frames.push(frame);
    }

    // --- Frame 2: Histogram (full-range, not limited by max_hits) ---
    if (resp?.aggregations?.time_histogram?.buckets?.length) {
      const buckets = resp.aggregations.time_histogram.buckets as any[];
      const times: number[] = [];
      const values: number[] = [];

      for (const b of buckets) {
        times.push(bucketKeyToMs(b.key));
        values.push(b.doc_count);
      }

      const histFrame = new MutableDataFrame({
        refId: `${query.refId}-volume`,
        meta: {
          preferredVisualisationType: 'graph',
          custom: { resultType: 'time_series' },
        },
        fields: [
          { name: 'Time', type: FieldType.time, values: times },
          {
            name: 'Log Volume',
            type: FieldType.number,
            values,
            config: {
              displayNameFromDS: 'Log Volume',
            },
          },
        ],
      });

      frames.push(histFrame);
    }

    return frames;
  }

  // ================================================================
  //  TRACE QUERIES
  // ================================================================

  private async queryTraceById(
    query: QuickwitQuery,
    options: DataQueryRequest<QuickwitQuery>
  ): Promise<MutableDataFrame[]> {
    const index = query.index || this.traceIndex;
    if (!index) {
      return [this.buildErrorFrame(query.refId, 'No trace index configured.')];
    }

    const traceId = getTemplateSrv().replace(query.traceId || '', options.scopedVars);
    if (!traceId) return [];

    try {
      const resp = await this.getWithTimeout<JaegerApiResponse<JaegerTrace[]>>(
        `/api/v1/${index}/jaeger/api/traces/${traceId}`,
        TRACE_TIMEOUT_MS
      );
      if (!resp?.data?.length) {
        return [this.buildErrorFrame(query.refId, `Trace ${traceId} not found.`)];
      }
      return [this.jaegerTraceToFrame(resp.data[0], query.refId)];
    } catch (e: any) {
      const msg = e?.message || String(e);
      if (msg.includes('timeout') || msg.includes('aborted')) {
        return [this.buildErrorFrame(query.refId, `Trace lookup timed out. The trace may not exist.`)];
      }
      return [this.buildErrorFrame(query.refId, `Trace lookup failed: ${msg}`)];
    }
  }

  private async queryTraceSearch(
    query: QuickwitQuery,
    options: DataQueryRequest<QuickwitQuery>
  ): Promise<MutableDataFrame[]> {
    const index = query.index || this.traceIndex;
    if (!index) {
      return [this.buildErrorFrame(query.refId, 'No trace index configured.')];
    }

    if (query.traceId) {
      return this.queryTraceById({ ...query, queryType: QueryType.TraceId }, options);
    }

    const params = new URLSearchParams();
    if (query.serviceName) {
      params.set('service', getTemplateSrv().replace(query.serviceName, options.scopedVars));
    }
    if (query.operationName && query.operationName !== 'All') {
      params.set('operation', query.operationName);
    }
    if (query.query && query.query !== '*') {
      params.set('tags', getTemplateSrv().replace(query.query, options.scopedVars));
    }
    if (query.minDuration) params.set('minDuration', query.minDuration);
    if (query.maxDuration) params.set('maxDuration', query.maxDuration);

    const from = options.range.from.valueOf() * 1000;
    const to = options.range.to.valueOf() * 1000;
    params.set('start', from.toString());
    params.set('end', to.toString());
    params.set('limit', String(query.traceLimit || 20));

    try {
      const resp = await this.getWithTimeout<JaegerApiResponse<JaegerTrace[]>>(
        `/api/v1/${index}/jaeger/api/traces?${params.toString()}`,
        TRACE_TIMEOUT_MS * 2
      );
      if (!resp?.data?.length) {
        return [this.buildErrorFrame(query.refId, 'No traces found.')];
      }
      return [this.buildTraceSearchTable(resp.data, query.refId)];
    } catch (e: any) {
      return [this.buildErrorFrame(query.refId, `Trace search failed: ${e?.message || e}`)];
    }
  }

  private jaegerTraceToFrame(trace: JaegerTrace, refId: string): MutableDataFrame {
    const frame = new MutableDataFrame({
      refId,
      meta: {
        preferredVisualisationType: 'trace',
        custom: { traceFormat: 'jaeger' },
      },
      fields: [
        { name: 'traceID', type: FieldType.string },
        { name: 'spanID', type: FieldType.string },
        { name: 'parentSpanID', type: FieldType.string },
        { name: 'operationName', type: FieldType.string },
        { name: 'serviceName', type: FieldType.string },
        { name: 'serviceTags', type: FieldType.other },
        { name: 'startTime', type: FieldType.number },
        { name: 'duration', type: FieldType.number },
        { name: 'logs', type: FieldType.other },
        { name: 'tags', type: FieldType.other },
        { name: 'warnings', type: FieldType.other },
      ],
    });

    for (const span of trace.spans) {
      const process = trace.processes[span.processID] || { serviceName: 'unknown', tags: [] };
      const parentRef = span.references?.find((r) => r.refType === 'CHILD_OF');

      frame.add({
        traceID: span.traceID,
        spanID: span.spanID,
        parentSpanID: parentRef?.spanID || '',
        operationName: span.operationName,
        serviceName: process.serviceName,
        serviceTags: process.tags || [],
        startTime: span.startTime / 1000,
        duration: span.duration / 1000,
        logs: (span.logs || []).map((l) => ({
          timestamp: l.timestamp / 1000,
          fields: l.fields,
        })),
        tags: span.tags || [],
        warnings: span.warnings || [],
      });
    }

    if (this.logIndex) {
      const traceIdField = frame.fields.find((f) => f.name === 'traceID');
      if (traceIdField) {
        traceIdField.config = {
          ...traceIdField.config,
          links: [{
            title: 'View Logs for this Trace',
            url: '',
            internal: {
              datasourceUid: this.uid,
              datasourceName: this.name,
              query: {
                refId: 'trace-to-log',
                queryType: QueryType.Logs,
                query: 'trace_id:${__value.raw}',
                index: this.logIndex,
                size: 100,
              } as any,
            },
          }],
        };
      }
    }

    return frame;
  }

  private buildTraceSearchTable(traces: JaegerTrace[], refId: string): MutableDataFrame {
    const frame = new MutableDataFrame({
      refId,
      meta: { preferredVisualisationType: 'table' },
      fields: [
        { name: 'Trace ID', type: FieldType.string },
        { name: 'Trace Name', type: FieldType.string },
        { name: 'Service', type: FieldType.string },
        { name: 'Start Time', type: FieldType.time },
        { name: 'Duration (ms)', type: FieldType.number },
        { name: 'Spans', type: FieldType.number },
      ],
    });

    for (const trace of traces) {
      if (!trace.spans?.length) continue;
      const rootSpan = trace.spans.reduce((a, b) => (a.startTime < b.startTime ? a : b));
      const process = trace.processes[rootSpan.processID];
      const maxEnd = trace.spans.reduce((max, s) => Math.max(max, s.startTime + s.duration), 0);

      frame.add({
        'Trace ID': trace.traceID,
        'Trace Name': rootSpan.operationName,
        Service: process?.serviceName || 'unknown',
        'Start Time': Math.floor(rootSpan.startTime / 1000),
        'Duration (ms)': Math.round((maxEnd - rootSpan.startTime) / 1000),
        Spans: trace.spans.length,
      });
    }

    const traceIdField = frame.fields.find((f) => f.name === 'Trace ID');
    if (traceIdField) {
      traceIdField.config = {
        links: [{
          title: 'View Trace',
          url: '',
          internal: {
            datasourceUid: this.uid,
            datasourceName: this.name,
            query: {
              refId: 'trace-detail',
              queryType: QueryType.TraceId,
              query: '',
              index: this.traceIndex,
              traceId: '${__value.raw}',
              size: 100,
            } as any,
          },
        }],
      };
    }

    const serviceField = frame.fields.find((f) => f.name === 'Service');
    if (serviceField) {
      serviceField.config = {
        links: [{
          title: 'Filter by service: ${__value.raw}',
          url: '',
          internal: {
            datasourceUid: this.uid,
            datasourceName: this.name,
            query: {
              refId: 'service-filter',
              queryType: QueryType.Traces,
              query: '',
              index: this.traceIndex,
              serviceName: '${__value.raw}',
              size: 100,
              traceLimit: 20,
            } as any,
          },
        }],
      };
    }

    return frame;
  }

  // ================================================================
  //  TRACE SERVICES & OPERATIONS
  // ================================================================

  async getServices(index?: string): Promise<string[]> {
    const idx = index || this.traceIndex;
    if (!idx) return [];
    try {
      const resp = await this.getWithTimeout<JaegerApiResponse<string[]>>(
        `/api/v1/${idx}/jaeger/api/services`, TRACE_TIMEOUT_MS);
      return resp?.data || [];
    } catch { return []; }
  }

  async getOperations(service: string, index?: string): Promise<string[]> {
    const idx = index || this.traceIndex;
    if (!idx || !service) return [];
    try {
      const resp = await this.getWithTimeout<JaegerApiResponse<string[]>>(
        `/api/v1/${idx}/jaeger/api/services/${encodeURIComponent(service)}/operations`, TRACE_TIMEOUT_MS);
      return resp?.data || [];
    } catch { return []; }
  }

  // ================================================================
  //  METRIC / AGGREGATION QUERIES
  // ================================================================

  private async queryMetrics(
    query: QuickwitQuery,
    options: DataQueryRequest<QuickwitQuery>
  ): Promise<MutableDataFrame[]> {
    const index = query.index || this.logIndex || this.defaultIndex;
    if (!index) return [];

    const fromMs = options.range.from.valueOf();
    const toMs = options.range.to.valueOf();
    const from = fromMs / 1000;
    const to = toMs / 1000;
    const lucene = getTemplateSrv().replace(query.query || '*', options.scopedVars);

    const indexes = await this.getIndexes();
    const tsField = detectTimestampField(indexes, index);
    const interval = query.groupByInterval || computeAutoInterval(fromMs, toMs);
    const aggType = query.metricType || MetricAggType.Count;

    // Build aggregation based on type
    const aggs: Record<string, any> = {};

    if (aggType === MetricAggType.Terms) {
      // Terms aggregation: top-N values for a field
      const termsField = query.termsField || query.metricField;
      if (!termsField) {
        return [this.buildErrorFrame(query.refId, 'Terms aggregation requires a field. Set "Terms Field" in the query editor.')];
      }
      aggs.terms_agg = {
        terms: {
          field: termsField,
          size: query.termsSize || 10,
          order: { _count: query.metricSortOrder || 'desc' },
        },
      };
    } else {
      // Time-based histogram aggregation
      aggs.time_histogram = {
        date_histogram: {
          field: tsField,
          fixed_interval: interval,
        },
      };

      // Add sub-aggregation for non-count metrics
      if (aggType !== MetricAggType.Count && aggType !== '__logs_volume__' && query.metricField) {
        if (aggType === MetricAggType.Percentiles) {
          aggs.time_histogram.aggs = {
            metric: {
              percentiles: {
                field: query.metricField,
                percents: [50, 90, 95, 99],
              },
            },
          };
        } else {
          // avg, sum, min, max
          aggs.time_histogram.aggs = {
            metric: {
              [aggType]: { field: query.metricField },
            },
          };
        }
      }
    }

    const body = {
      query: lucene,
      max_hits: 0,
      start_timestamp: Math.floor(from),
      end_timestamp: Math.ceil(to),
      aggs,
    };

    try {
      const resp = await this.post<QwSearchResponse>(`/api/v1/${index}/search`, body);

      // Handle terms aggregation
      if (aggType === MetricAggType.Terms && resp?.aggregations?.terms_agg?.buckets) {
        return this.buildTermsFrame(resp.aggregations.terms_agg.buckets, query);
      }

      // Handle time histogram
      if (!resp?.aggregations?.time_histogram?.buckets?.length) {
        return [this.buildErrorFrame(query.refId,
          `No aggregation results. Ensure "${tsField}" is a fast field in index "${index}".`)];
      }

      const buckets = resp.aggregations.time_histogram.buckets as any[];

      if (aggType === MetricAggType.Percentiles && query.metricField) {
        return this.buildPercentilesFrame(buckets, query);
      }

      const times: number[] = [];
      const values: number[] = [];

      for (const b of buckets) {
        times.push(bucketKeyToMs(b.key));
        if (aggType !== MetricAggType.Count && b.metric) {
          values.push(b.metric.value ?? 0);
        } else {
          values.push(b.doc_count);
        }
      }

      const label = aggType === MetricAggType.Count
        ? 'Count'
        : `${aggType}(${query.metricField || ''})`;

      const frame = new MutableDataFrame({
        refId: query.refId,
        fields: [
          { name: 'Time', type: FieldType.time, values: times },
          { name: label, type: FieldType.number, values },
        ],
      });

      return [frame];
    } catch (e: any) {
      console.error('Metrics query failed:', e);
      return [this.buildErrorFrame(query.refId,
        `Aggregation failed: ${e?.message || e}. Ensure "${tsField}" is a fast field.`)];
    }
  }

  private buildTermsFrame(buckets: any[], query: QuickwitQuery): MutableDataFrame[] {
    const keys: string[] = [];
    const counts: number[] = [];

    for (const b of buckets) {
      keys.push(String(b.key));
      counts.push(b.doc_count);
    }

    const frame = new MutableDataFrame({
      refId: query.refId,
      meta: { preferredVisualisationType: 'table' },
      fields: [
        { name: query.termsField || 'Value', type: FieldType.string, values: keys },
        { name: 'Count', type: FieldType.number, values: counts },
      ],
    });

    // Add filter link on the value column
    const valueField = frame.fields[0];
    if (valueField) {
      valueField.config = {
        links: [{
          title: 'Filter by ${__value.raw}',
          url: '',
          internal: {
            datasourceUid: this.uid,
            datasourceName: this.name,
            query: {
              refId: query.refId,
              queryType: QueryType.Logs,
              query: `${query.termsField}:\${__value.raw}`,
              index: query.index || this.logIndex || this.defaultIndex,
              size: 100,
            } as any,
          },
        }],
      };
    }

    return [frame];
  }

  private buildPercentilesFrame(buckets: any[], query: QuickwitQuery): MutableDataFrame[] {
    const times: number[] = [];
    const p50: number[] = [];
    const p90: number[] = [];
    const p95: number[] = [];
    const p99: number[] = [];

    for (const b of buckets) {
      times.push(bucketKeyToMs(b.key));
      const pvals = b.metric?.values || {};
      p50.push(pvals['50.0'] ?? 0);
      p90.push(pvals['90.0'] ?? 0);
      p95.push(pvals['95.0'] ?? 0);
      p99.push(pvals['99.0'] ?? 0);
    }

    const field = query.metricField || '';
    const frame = new MutableDataFrame({
      refId: query.refId,
      fields: [
        { name: 'Time', type: FieldType.time, values: times },
        { name: `p50(${field})`, type: FieldType.number, values: p50 },
        { name: `p90(${field})`, type: FieldType.number, values: p90 },
        { name: `p95(${field})`, type: FieldType.number, values: p95 },
        { name: `p99(${field})`, type: FieldType.number, values: p99 },
      ],
    });

    return [frame];
  }

  // ================================================================
  //  HEALTH CHECK
  // ================================================================

  async testDatasource(): Promise<any> {
    try {
      const indexes = await this.getIndexes();
      return {
        status: 'success',
        message: `Connected to Quickwit. Found ${indexes.length} indexes.`,
      };
    } catch (e: any) {
      return {
        status: 'error',
        message: `Connection failed: ${e?.message || e}`,
      };
    }
  }

  // ================================================================
  //  HELPERS
  // ================================================================

  private buildErrorFrame(refId: string, message: string): MutableDataFrame {
    return new MutableDataFrame({
      refId,
      meta: { notices: [{ severity: 'warning' as any, text: message }] },
      fields: [{ name: 'message', type: FieldType.string, values: [message] }],
    });
  }

  private async get<T>(path: string): Promise<T> {
    const resp = await getBackendSrv().datasourceRequest({
      url: `${this.baseUrl}${path}`,
      method: 'GET',
    });
    return resp.data;
  }

  private async getWithTimeout<T>(path: string, timeoutMs: number): Promise<T> {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const resp = await getBackendSrv().datasourceRequest({
        url: `${this.baseUrl}${path}`,
        method: 'GET',
        // @ts-ignore
        signal: controller.signal,
      });
      return resp.data;
    } catch (e: any) {
      if (e?.name === 'AbortError' || controller.signal.aborted) {
        throw new Error(`Request timeout after ${timeoutMs}ms`);
      }
      throw e;
    } finally {
      clearTimeout(timer);
    }
  }

  private async post<T>(path: string, body: any): Promise<T> {
    const resp = await getBackendSrv().datasourceRequest({
      url: `${this.baseUrl}${path}`,
      method: 'POST',
      data: body,
    });
    return resp.data;
  }
}
