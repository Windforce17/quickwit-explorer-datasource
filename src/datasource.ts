import {
  DataQueryRequest,
  DataQueryResponse,
  DataSourceApi,
  DataSourceInstanceSettings,
  FieldType,
  MutableDataFrame,
  SupplementaryQueryType,
  LogsVolumeType,
} from '@grafana/data';
import { getBackendSrv, getTemplateSrv } from '@grafana/runtime';
import { cloneDeep } from 'lodash';
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
const TRACE_TIMEOUT_MS = 8000;

/** Safely extract error message from any error object */
function extractErrorMessage(e: any): string {
  if (!e) return 'Unknown error';
  if (typeof e === 'string') return e;
  if (e.data?.message) return e.data.message;
  if (e.data?.error) return typeof e.data.error === 'string' ? e.data.error : JSON.stringify(e.data.error);
  if (e.statusText) return `${e.status || ''} ${e.statusText}`.trim();
  if (e.message) return e.message;
  try { return JSON.stringify(e); } catch { return String(e); }
}

// ================================================================
//  UTILITY FUNCTIONS
// ================================================================

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

function detectTimestampField(indexes: QwIndex[], indexId: string): string {
  let idx = indexes.find((i) => i.index_config.index_id === indexId);
  if (!idx && indexId.includes('*')) {
    const prefix = indexId.replace(/\*/g, '');
    idx = indexes.find((i) => i.index_config.index_id.startsWith(prefix));
  }
  if (idx?.index_config?.doc_mapping?.timestamp_field) {
    return idx.index_config.doc_mapping.timestamp_field;
  }
  if (idx) {
    const allFields = flattenFieldMappings(idx.index_config.doc_mapping.field_mappings);
    const commonTsNames = ['timestamp', '_timestamp', '@timestamp', 'timestamp_nanos'];
    for (const name of commonTsNames) {
      if (allFields.includes(name)) return name;
    }
  }
  return 'timestamp';
}

/**
 * Recursively flatten field mappings, handling nested field_mappings
 * and object-type fields. Produces dot-notation paths like "httpRequest.clientIp".
 */
function flattenFieldMappings(mappings: QwFieldMapping[], prefix = ''): string[] {
  const fields: string[] = [];
  if (!mappings) return fields;

  for (const m of mappings) {
    const fullName = prefix ? `${prefix}.${m.name}` : m.name;

    if (m.field_mappings && m.field_mappings.length > 0) {
      // Recurse into sub-fields
      fields.push(...flattenFieldMappings(m.field_mappings, fullName));
    } else if (m.type === 'object') {
      // Object type without explicit sub-fields - add as-is
      fields.push(fullName);
    } else if (m.type === 'json') {
      // JSON type - add as-is, user may query sub-paths
      fields.push(fullName);
    } else {
      fields.push(fullName);
    }
  }
  return fields;
}

/**
 * Flatten a JSON object into dot-notation key-value pairs.
 * e.g. { httpRequest: { clientIp: "1.2.3.4" } } => { "httpRequest.clientIp": "1.2.3.4" }
 */
function flattenObject(obj: any, prefix = '', maxDepth = 10): Record<string, string> {
  const result: Record<string, string> = {};
  if (!obj || typeof obj !== 'object' || maxDepth <= 0) return result;

  for (const key of Object.keys(obj)) {
    const fullKey = prefix ? `${prefix}.${key}` : key;
    const val = obj[key];

    if (val !== null && typeof val === 'object' && !Array.isArray(val)) {
      Object.assign(result, flattenObject(val, fullKey, maxDepth - 1));
    } else if (Array.isArray(val)) {
      result[fullKey] = JSON.stringify(val);
    } else if (val !== undefined && val !== null) {
      result[fullKey] = String(val);
    }
  }
  return result;
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

function extractTimestamp(hit: any, tsFieldName?: string): number {
  // Try the known timestamp field first
  const fieldsToTry = tsFieldName
    ? [tsFieldName, 'timestamp', '_timestamp', '@timestamp', 'timestamp_nanos']
    : ['timestamp', '_timestamp', '@timestamp', 'timestamp_nanos'];

  for (const field of fieldsToTry) {
    const val = getNestedValue(hit, field);
    if (val !== undefined && val !== null) {
      const num = Number(val);
      if (!isNaN(num)) {
        if (num > 1e18) return Math.floor(num / 1e6); // nanoseconds
        if (num > 1e15) return Math.floor(num / 1e3); // microseconds
        if (num > 1e12) return num;                     // milliseconds
        return num * 1000;                               // seconds
      }
      const d = new Date(val);
      if (!isNaN(d.getTime())) return d.getTime();
    }
  }
  return Date.now();
}

function bucketKeyToMs(key: number): number {
  if (key > 4102444800000) return key / 1000;  // microseconds
  if (key < 4102444800) return key * 1000;       // seconds
  return key;                                     // milliseconds
}

/**
 * Parse a Lucene query string into individual filter clauses.
 */
function parseFilters(query: string): string[] {
  if (!query || query.trim() === '*' || query.trim() === '') return [];
  // Split on AND (case-insensitive), preserving quoted strings
  return query.split(/\s+AND\s+/i).map((f) => f.trim()).filter((f) => f && f !== '*');
}

/**
 * Build a Lucene query from filter clauses.
 */
function buildQuery(filters: string[]): string {
  if (filters.length === 0) return '*';
  return filters.join(' AND ');
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
  private tsFieldCache: Map<string, string> = new Map();

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
  //  DataSourceWithSupplementaryQueriesSupport (duck-typed)
  //  This enables full-range log volume histograms in Explore.
  // ================================================================

  getSupportedSupplementaryQueryTypes(): SupplementaryQueryType[] {
    return [SupplementaryQueryType.LogsVolume];
  }

  getSupplementaryQuery(
    options: { type: SupplementaryQueryType },
    originalQuery: QuickwitQuery
  ): QuickwitQuery | undefined {
    if (options.type !== SupplementaryQueryType.LogsVolume) return undefined;
    if (originalQuery.queryType && originalQuery.queryType !== QueryType.Logs) return undefined;

    return {
      ...originalQuery,
      refId: `log-volume-${originalQuery.refId}`,
      queryType: QueryType.Logs,
      _isLogsVolume: true,
    };
  }

  getSupplementaryRequest(
    type: SupplementaryQueryType,
    request: DataQueryRequest<QuickwitQuery>
  ): DataQueryRequest<QuickwitQuery> | undefined {
    if (type !== SupplementaryQueryType.LogsVolume) return undefined;

    const logsVolumeRequest = cloneDeep(request);
    const targets = logsVolumeRequest.targets
      .map((query) => this.getSupplementaryQuery({ type }, query))
      .filter((query): query is QuickwitQuery => !!query);

    if (!targets.length) return undefined;
    return { ...logsVolumeRequest, targets };
  }

  // ================================================================
  //  DataSourceWithToggleableQueryFiltersSupport (duck-typed)
  //  This enables +/- filter buttons on log field values in Explore.
  //
  //  Grafana calls toggleQueryFilter with a ToggleFilterAction:
  //    { type: 'FILTER_FOR' | 'FILTER_OUT', options: { key, value } }
  //  Grafana calls queryHasFilter with QueryFilterOptions:
  //    { key, value }
  // ================================================================

  toggleQueryFilter(
    query: QuickwitQuery,
    filter: { type: string; options: Record<string, string> }
  ): QuickwitQuery {
    const currentQuery = query.query || '*';
    const filters = parseFilters(currentQuery);
    const key = filter.options?.key;
    const value = filter.options?.value;

    if (!key || value === undefined) return query;

    // Escape value if it contains spaces or special chars
    const escapedValue = value.includes(' ') || value.includes('"') ? `"${value.replace(/"/g, '\\"')}"` : value;

    const isExclude = filter.type === 'FILTER_OUT';
    const positiveFilter = `${key}:${escapedValue}`;
    const negativeFilter = `NOT ${key}:${escapedValue}`;
    const newFilter = isExclude ? negativeFilter : positiveFilter;

    // Check if this exact filter already exists (toggle off)
    const existingIdx = filters.findIndex((f) => f === newFilter);
    if (existingIdx >= 0) {
      filters.splice(existingIdx, 1);
      return { ...query, query: buildQuery(filters) };
    }

    // Remove opposite filter if present
    const oppositeFilter = isExclude ? positiveFilter : negativeFilter;
    const oppositeIdx = filters.findIndex((f) => f === oppositeFilter);
    if (oppositeIdx >= 0) {
      filters.splice(oppositeIdx, 1);
    }

    filters.push(newFilter);
    return { ...query, query: buildQuery(filters) };
  }

  queryHasFilter(
    query: QuickwitQuery,
    filter: Record<string, string>
  ): boolean {
    const currentQuery = query.query || '*';
    const key = filter?.key;
    const value = filter?.value;
    if (!key || value === undefined) return false;

    const escapedValue = value.includes(' ') || value.includes('"') ? `"${value.replace(/"/g, '\\"')}"` : value;
    return currentQuery.includes(`${key}:${escapedValue}`);
  }

  // ================================================================
  //  DataSourceWithQueryModificationSupport (duck-typed)
  //  This enables "Add to query" / "Exclude from query" in log details.
  // ================================================================

  modifyQuery(query: QuickwitQuery, action: { type: string; options?: any }): QuickwitQuery {
    const currentQuery = query.query || '*';
    const filters = parseFilters(currentQuery);

    switch (action.type) {
      case 'ADD_FILTER': {
        const { key, value } = action.options || {};
        if (key && value !== undefined) {
          const newFilter = `${key}:${value}`;
          if (!filters.includes(newFilter)) {
            filters.push(newFilter);
          }
        }
        break;
      }
      case 'ADD_FILTER_OUT': {
        const { key, value } = action.options || {};
        if (key && value !== undefined) {
          const newFilter = `NOT ${key}:${value}`;
          // Remove any positive filter for same key:value
          const posIdx = filters.indexOf(`${key}:${value}`);
          if (posIdx >= 0) filters.splice(posIdx, 1);
          if (!filters.includes(newFilter)) {
            filters.push(newFilter);
          }
        }
        break;
      }
    }

    return { ...query, query: buildQuery(filters) };
  }

  getSupportedQueryModifications(): string[] {
    return ['ADD_FILTER', 'ADD_FILTER_OUT'];
  }

  // ================================================================
  //  MAIN QUERY DISPATCHER
  // ================================================================

  async query(options: DataQueryRequest<QuickwitQuery>): Promise<DataQueryResponse> {
    const promises = options.targets
      .filter((t) => !t.hide)
      .map((target) => {
        const q = { ...defaultQuery, ...target } as QuickwitQuery;

        // Handle supplementary logs volume query
        if (q._isLogsVolume) {
          return this.queryLogsVolume(q, options);
        }

        switch (q.queryType) {
          case QueryType.Traces:
            return this.queryTraceSearch(q, options);
          case QueryType.TraceId:
            return this.queryTraceById(q, options);
          case 'traceql' as any:
            // Grafana's built-in LogLineDetailsTrace panel sends queryType 'traceql'
            // (Tempo-style) with the trace ID in the `query` field.  Route it to our
            // Jaeger-based trace lookup so the embedded trace view works correctly.
            return this.queryTraceById(
              { ...q, traceId: q.query, queryType: QueryType.TraceId, index: this.traceIndex || q.index },
              options
            );
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
    let idx = indexes.find((i) => i.index_config.index_id === indexId);
    // Wildcard match
    if (!idx && indexId.includes('*')) {
      const prefix = indexId.replace(/\*/g, '');
      idx = indexes.find((i) => i.index_config.index_id.startsWith(prefix));
    }
    if (!idx) return [];

    const schemaFields = flattenFieldMappings(idx.index_config.doc_mapping.field_mappings);

    // For json/object fields without explicit sub-fields, try to discover
    // dynamic sub-fields by sampling one document from the index.
    const jsonFields = (idx.index_config.doc_mapping.field_mappings || [])
      .filter((m) => (m.type === 'json' || m.type === 'object') && (!m.field_mappings || m.field_mappings.length === 0));

    if (jsonFields.length > 0) {
      try {
        const sampleResp = await this.post<QwSearchResponse>(`/api/v1/${indexId}/search`, {
          query: '*',
          max_hits: 1,
        });
        if (sampleResp?.hits?.length) {
          const hit = sampleResp.hits[0];
          for (const jf of jsonFields) {
            const nested = hit[jf.name];
            if (nested && typeof nested === 'object') {
              const subFields = flattenObject(nested, jf.name);
              for (const subKey of Object.keys(subFields)) {
                if (!schemaFields.includes(subKey)) {
                  schemaFields.push(subKey);
                }
              }
            }
          }
        }
      } catch {
        // Sampling failed, continue with schema-only fields
      }
    }

    schemaFields.sort();
    this.fieldCache.set(indexId, { data: schemaFields, ts: now });
    return schemaFields;
  }

  /**
   * Get the timestamp field for an index, with caching.
   */
  private async getTimestampField(indexId: string): Promise<string> {
    if (this.tsFieldCache.has(indexId)) {
      return this.tsFieldCache.get(indexId)!;
    }
    const indexes = await this.getIndexes();
    const tsField = detectTimestampField(indexes, indexId);
    this.tsFieldCache.set(indexId, tsField);
    return tsField;
  }

  // ================================================================
  //  LOGS VOLUME (supplementary query for histogram)
  //  This is called by Grafana automatically for full-range histogram.
  // ================================================================

  private async queryLogsVolume(
    query: QuickwitQuery,
    options: DataQueryRequest<QuickwitQuery>
  ): Promise<MutableDataFrame[]> {
    const index = query.index || this.logIndex || this.defaultIndex;
    if (!index) return [];

    const fromMs = options.range.from.valueOf();
    const toMs = options.range.to.valueOf();
    const tsField = await this.getTimestampField(index);
    const interval = computeAutoInterval(fromMs, toMs);
    const lucene = getTemplateSrv().replace(query.query || '*', options.scopedVars);

    const body = {
      query: lucene,
      max_hits: 0,
      start_timestamp: Math.floor(fromMs / 1000),
      end_timestamp: Math.ceil(toMs / 1000),
      aggs: {
        time_histogram: {
          date_histogram: {
            field: tsField,
            fixed_interval: interval,
          },
        },
      },
    };

    try {
      const resp = await this.post<QwSearchResponse>(`/api/v1/${index}/search`, body);
      const buckets = resp?.aggregations?.time_histogram?.buckets || [];

      const times: number[] = [];
      const values: number[] = [];
      for (const b of buckets) {
        times.push(bucketKeyToMs(b.key));
        values.push(b.doc_count);
      }

      // If no buckets, return empty frame with correct meta
      if (times.length === 0) {
        times.push(fromMs);
        values.push(0);
      }

      const frame = new MutableDataFrame({
        refId: query.refId,
        meta: {
          preferredVisualisationType: 'graph',
          custom: {
            logsVolumeType: LogsVolumeType.FullRange,
            absoluteRange: { from: fromMs, to: toMs },
            datasourceName: this.name,
            sourceQuery: query,
          },
        },
        fields: [
          { name: 'Time', type: FieldType.time, values: times },
          {
            name: 'Volume',
            type: FieldType.number,
            values,
            config: {
              displayNameFromDS: 'Log Volume',
              color: { mode: 'fixed', fixedColor: 'green' },
            },
          },
        ],
      });

      return [frame];
    } catch (e) {
      console.error('Logs volume query failed:', e);
      return [];
    }
  }

  // ================================================================
  //  LOG QUERIES
  // ================================================================

  private async queryLogs(
    query: QuickwitQuery,
    options: DataQueryRequest<QuickwitQuery>
  ): Promise<MutableDataFrame[]> {
    const index = query.index || this.logIndex || this.defaultIndex;
    if (!index) return [];

    const fromMs = options.range.from.valueOf();
    const toMs = options.range.to.valueOf();
    const tsField = await this.getTimestampField(index);
    const lucene = getTemplateSrv().replace(query.query || '*', options.scopedVars);

    const body: Record<string, any> = {
      query: lucene,
      max_hits: query.size || 100,
      start_timestamp: Math.floor(fromMs / 1000),
      end_timestamp: Math.ceil(toMs / 1000),
      sort_by_field: tsField,
    };

    const resp = await this.post<QwSearchResponse>(`/api/v1/${index}/search`, body);
    const frames: MutableDataFrame[] = [];

    if (resp?.hits?.length) {
      const msgField = this.logMessageField;
      const lvlField = this.logLevelField;

      // Check if this index has trace_id field by looking at actual data
      const fields = await this.getFields(index);
      const hasTraceField = fields.includes('trace_id');
      const hasTraceIndex = !!this.traceIndex;

      // Flatten all hits first to discover all field names
      const allFlat: Array<Record<string, string>> = [];
      const allFieldNames = new Set<string>();
      const reservedFields = new Set(['timestamp', 'body', 'severity', 'id', 'traceID', 'spanID']);

      for (const hit of resp.hits) {
        const flat = flattenObject(hit);
        allFlat.push(flat);
        for (const key of Object.keys(flat)) {
          allFieldNames.add(key);
        }
      }

      // Build dynamic field columns (exclude reserved names and known fields)
      const extraFieldNames = Array.from(allFieldNames).filter(
        (f) => !reservedFields.has(f) && f !== tsField && f !== msgField && f !== lvlField
      ).sort();

      const frameFields: any[] = [
        { name: 'timestamp', type: FieldType.time },
        { name: 'body', type: FieldType.string },
        { name: 'severity', type: FieldType.string },
        { name: 'id', type: FieldType.string },
        { name: 'traceID', type: FieldType.string },
        { name: 'spanID', type: FieldType.string },
        { name: 'labels', type: FieldType.other },
      ];

      const frame = new MutableDataFrame({
        refId: query.refId,
        meta: {
          type: 'log-lines' as any,
          preferredVisualisationType: 'logs',
          custom: {
            // Hint for Grafana: only show these labels inline on log lines
            displayedFields: [lvlField].filter(Boolean),
          },
        },
        fields: frameFields,
      });

      for (let i = 0; i < resp.hits.length; i++) {
        const hit = resp.hits[i];
        const flat = allFlat[i];
        const ts = extractTimestamp(hit, tsField);
        const msg = getNestedValue(hit, msgField);
        const level = getNestedValue(hit, lvlField) || '';
        const traceId = getNestedValue(hit, 'trace_id') || '';
        const spanId = getNestedValue(hit, 'span_id') || '';

        // Include ALL fields in labels so Table view and field picker work correctly
        // Grafana uses labels to populate the left-side field list and Table columns
        const labels: Record<string, string> = { ...flat };
        // Remove fields that are already represented as dedicated frame columns
        delete labels[tsField];
        delete labels[msgField];

        frame.add({
          timestamp: ts,
          body: typeof msg === 'string' ? msg : JSON.stringify(hit),
          severity: String(level),
          id: `${ts}-${i}`,
          traceID: traceId,
          spanID: spanId,
          labels,
        });
      }

      // Add trace link if this index has trace_id AND a trace index is configured
      if (hasTraceField && hasTraceIndex) {
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

      frames.push(frame);
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
      const msg = extractErrorMessage(e);
      if (msg.includes('timeout') || msg.includes('aborted') || msg.includes('Abort')) {
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
      return [this.buildErrorFrame(query.refId, `Trace search failed: ${extractErrorMessage(e)}`)];
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

    // Trace → Log link
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

    // Trace ID → detail link
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
    } catch (e: any) {
      console.error('getServices failed:', extractErrorMessage(e));
      return [];
    }
  }

  async getOperations(service: string, index?: string): Promise<string[]> {
    const idx = index || this.traceIndex;
    if (!idx || !service) return [];
    try {
      const resp = await this.getWithTimeout<JaegerApiResponse<string[]>>(
        `/api/v1/${idx}/jaeger/api/services/${encodeURIComponent(service)}/operations`, TRACE_TIMEOUT_MS);
      return resp?.data || [];
    } catch (e: any) {
      console.error('getOperations failed:', extractErrorMessage(e));
      return [];
    }
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

    // Use new model if metrics[] and bucketAggs[] are present
    if (query.metrics && query.metrics.length > 0 && query.bucketAggs && query.bucketAggs.length > 0) {
      return this.queryMetricsNew(query, options);
    }

    // Legacy single-metric model
    return this.queryMetricsLegacy(query, options);
  }

  // ================================================================
  //  NEW METRICS MODEL: metrics[] + bucketAggs[]
  // ================================================================

  private async queryMetricsNew(
    query: QuickwitQuery,
    options: DataQueryRequest<QuickwitQuery>
  ): Promise<MutableDataFrame[]> {
    const index = query.index || this.logIndex || this.defaultIndex;
    if (!index) return [];

    const fromMs = options.range.from.valueOf();
    const toMs = options.range.to.valueOf();
    const tsField = await this.getTimestampField(index);
    const lucene = getTemplateSrv().replace(query.query || '*', options.scopedVars);
    const interval = query.groupByInterval || computeAutoInterval(fromMs, toMs);

    const metrics = query.metrics!;
    const bucketAggs = query.bucketAggs!;

    // Build metric aggregation objects
    const metricAggs: Record<string, any> = {};
    for (const m of metrics) {
      if (m.type === 'count') continue; // count is implicit via doc_count
      if (!m.field) continue;
      const aggKey = `metric_${m.id}`;
      if (m.type === 'percentiles') {
        const percents = m.settings?.percents?.map(Number) || [50, 90, 95, 99];
        metricAggs[aggKey] = { percentiles: { field: m.field, percents } };
      } else if (m.type === 'cardinality') {
        metricAggs[aggKey] = { cardinality: { field: m.field } };
      } else {
        metricAggs[aggKey] = { [m.type]: { field: m.field } };
      }
    }

    // Build nested bucket aggregations from inside out
    // The last bucket agg is the innermost; it gets the metric sub-aggs
    let currentAggs: Record<string, any> = { ...metricAggs };

    // Process bucketAggs from last to first (inside out)
    for (let i = bucketAggs.length - 1; i >= 0; i--) {
      const bucket = bucketAggs[i];
      const bucketKey = `bucket_${bucket.id}`;

      if (bucket.type === 'date_histogram') {
        const histInterval = bucket.settings?.interval || interval;
        const bucketDef: any = {
          date_histogram: {
            field: bucket.field || tsField,
            fixed_interval: histInterval,
          },
        };
        if (Object.keys(currentAggs).length > 0) {
          bucketDef.aggs = currentAggs;
        }
        currentAggs = { [bucketKey]: bucketDef };
      } else if (bucket.type === 'terms') {
        if (!bucket.field) continue;
        const size = parseInt(bucket.settings?.size || '10', 10);
        const order = bucket.settings?.order || 'desc';
        const orderBy = bucket.settings?.orderBy || '_count';
        const minDocCount = parseInt(bucket.settings?.min_doc_count || '1', 10);

        // Resolve orderBy: if it references a metric id, use the metric key
        let orderField = orderBy;
        if (orderBy !== '_count' && orderBy !== '_key') {
          // It's a metric id reference
          orderField = `metric_${orderBy}`;
        }

        const bucketDef: any = {
          terms: {
            field: bucket.field,
            size,
            min_doc_count: minDocCount,
            order: { [orderField]: order },
          },
        };
        if (bucket.settings?.missing) {
          bucketDef.terms.missing = bucket.settings.missing;
        }
        if (Object.keys(currentAggs).length > 0) {
          bucketDef.aggs = currentAggs;
        }
        currentAggs = { [bucketKey]: bucketDef };
      }
    }

    const body = {
      query: lucene,
      max_hits: 0,
      start_timestamp: Math.floor(fromMs / 1000),
      end_timestamp: Math.ceil(toMs / 1000),
      aggs: currentAggs,
    };

    try {
      const resp = await this.post<QwSearchResponse>(`/api/v1/${index}/search`, body);
      if (!resp?.aggregations) {
        return [this.buildErrorFrame(query.refId, 'No aggregation results returned.')];
      }

      // Parse the response based on bucket structure
      return this.parseNewMetricsResponse(resp.aggregations, bucketAggs, metrics, query, tsField);
    } catch (e: any) {
      return [this.buildErrorFrame(query.refId,
        `Aggregation failed: ${extractErrorMessage(e)}. Ensure fields are fast fields.`)];
    }
  }

  /**
   * Parse nested aggregation response into Grafana data frames.
   * Handles: date_histogram (time series), terms (grouped), and combinations.
   */
  private parseNewMetricsResponse(
    aggregations: Record<string, any>,
    bucketAggs: import('./types').BucketAggregation[],
    metrics: import('./types').MetricAggregation[],
    query: QuickwitQuery,
    tsField: string
  ): MutableDataFrame[] {
    const firstBucket = bucketAggs[0];
    const firstBucketKey = `bucket_${firstBucket.id}`;
    const firstBucketData = aggregations[firstBucketKey];

    if (!firstBucketData?.buckets?.length) {
      return [this.buildErrorFrame(query.refId, 'No aggregation buckets returned.')];
    }

    const buckets = firstBucketData.buckets;

    // Case 1: Only date_histogram (no terms) → time series
    if (bucketAggs.length === 1 && firstBucket.type === 'date_histogram') {
      return this.buildNewTimeSeriesFrames(buckets, metrics, query, null);
    }

    // Case 2: Only terms (no date_histogram) → table
    if (bucketAggs.length === 1 && firstBucket.type === 'terms') {
      return this.buildNewTableFrame(buckets, firstBucket, metrics, query);
    }

    // Case 3: terms → date_histogram → metrics (grouped time series)
    if (bucketAggs.length >= 2 && firstBucket.type === 'terms') {
      const secondBucket = bucketAggs[1];
      const secondBucketKey = `bucket_${secondBucket.id}`;
      const frames: MutableDataFrame[] = [];

      for (const termBucket of buckets) {
        const groupKey = String(termBucket.key);
        const innerBuckets = termBucket[secondBucketKey]?.buckets || [];

        if (secondBucket.type === 'date_histogram') {
          const groupFrames = this.buildNewTimeSeriesFrames(innerBuckets, metrics, query, groupKey);
          frames.push(...groupFrames);
        }
      }

      if (frames.length === 0) {
        return [this.buildErrorFrame(query.refId, 'No data for grouped time series.')];
      }
      return frames;
    }

    // Case 4: date_histogram → terms (time series with sub-groups) - less common
    // For now, treat as simple time series with the first metric
    return this.buildNewTimeSeriesFrames(buckets, metrics, query, null);
  }

  /**
   * Build time series frames from date_histogram buckets.
   * If groupKey is provided, it's used as a prefix for series names.
   */
  private buildNewTimeSeriesFrames(
    buckets: any[],
    metrics: import('./types').MetricAggregation[],
    query: QuickwitQuery,
    groupKey: string | null
  ): MutableDataFrame[] {
    const frames: MutableDataFrame[] = [];
    const times = buckets.map((b: any) => bucketKeyToMs(b.key));

    for (const m of metrics) {
      if (m.hide) continue;

      if (m.type === 'count') {
        const values = buckets.map((b: any) => b.doc_count);
        const name = groupKey ? `Count [${groupKey}]` : 'Count';
        frames.push(new MutableDataFrame({
          refId: query.refId,
          fields: [
            { name: 'Time', type: FieldType.time, values: [...times] },
            {
              name,
              type: FieldType.number,
              values,
              config: groupKey ? { displayNameFromDS: `Count: ${groupKey}` } : {},
            },
          ],
        }));
      } else if (m.type === 'percentiles') {
        const metricKey = `metric_${m.id}`;
        const percents = m.settings?.percents?.map(Number) || [50, 90, 95, 99];
        for (const p of percents) {
          const pKey = `${p}.0`;
          const values = buckets.map((b: any) => b[metricKey]?.values?.[pKey] ?? 0);
          const name = groupKey
            ? `p${p}(${m.field}) [${groupKey}]`
            : `p${p}(${m.field})`;
          frames.push(new MutableDataFrame({
            refId: query.refId,
            fields: [
              { name: 'Time', type: FieldType.time, values: [...times] },
              {
                name,
                type: FieldType.number,
                values,
                config: groupKey ? { displayNameFromDS: `p${p}(${m.field}): ${groupKey}` } : {},
              },
            ],
          }));
        }
      } else {
        const metricKey = `metric_${m.id}`;
        const values = buckets.map((b: any) => b[metricKey]?.value ?? 0);
        const label = `${m.type}(${m.field})`;
        const name = groupKey ? `${label} [${groupKey}]` : label;
        frames.push(new MutableDataFrame({
          refId: query.refId,
          fields: [
            { name: 'Time', type: FieldType.time, values: [...times] },
            {
              name,
              type: FieldType.number,
              values,
              config: groupKey ? { displayNameFromDS: `${label}: ${groupKey}` } : {},
            },
          ],
        }));
      }
    }

    return frames;
  }

  /**
   * Build a table frame from terms buckets with metric values.
   */
  private buildNewTableFrame(
    buckets: any[],
    termsBucket: import('./types').BucketAggregation,
    metrics: import('./types').MetricAggregation[],
    query: QuickwitQuery
  ): MutableDataFrame[] {
    const keys = buckets.map((b: any) => String(b.key));
    const fields: any[] = [
      { name: termsBucket.field || 'Group', type: FieldType.string, values: keys },
    ];

    for (const m of metrics) {
      if (m.hide) continue;

      if (m.type === 'count') {
        fields.push({
          name: 'Count',
          type: FieldType.number,
          values: buckets.map((b: any) => b.doc_count),
        });
      } else if (m.type === 'percentiles') {
        const metricKey = `metric_${m.id}`;
        const percents = m.settings?.percents?.map(Number) || [50, 90, 95, 99];
        for (const p of percents) {
          const pKey = `${p}.0`;
          fields.push({
            name: `p${p}(${m.field})`,
            type: FieldType.number,
            values: buckets.map((b: any) => b[metricKey]?.values?.[pKey] ?? 0),
          });
        }
      } else {
        const metricKey = `metric_${m.id}`;
        fields.push({
          name: `${m.type}(${m.field})`,
          type: FieldType.number,
          values: buckets.map((b: any) => b[metricKey]?.value ?? 0),
        });
      }
    }

    return [new MutableDataFrame({
      refId: query.refId,
      meta: { preferredVisualisationType: 'table' },
      fields,
    })];
  }

  // ================================================================
  //  LEGACY METRICS MODEL (backward compatibility)
  // ================================================================

  private async queryMetricsLegacy(
    query: QuickwitQuery,
    options: DataQueryRequest<QuickwitQuery>
  ): Promise<MutableDataFrame[]> {
    const index = query.index || this.logIndex || this.defaultIndex;
    if (!index) return [];

    const fromMs = options.range.from.valueOf();
    const toMs = options.range.to.valueOf();
    const tsField = await this.getTimestampField(index);
    const lucene = getTemplateSrv().replace(query.query || '*', options.scopedVars);
    const interval = query.groupByInterval || computeAutoInterval(fromMs, toMs);
    const aggType = query.metricType || MetricAggType.Count;
    const displayMode = query.metricDisplayMode || 'timeSeries';
    const hasGroupBy = !!query.groupBy;
    const isNumericAgg = aggType !== MetricAggType.Count && aggType !== MetricAggType.Terms;

    const aggs: Record<string, any> = {};

    if (aggType === MetricAggType.Terms) {
      const termsField = query.termsField || query.metricField;
      if (!termsField) {
        return [this.buildErrorFrame(query.refId, 'Terms aggregation requires a field.')];
      }
      aggs.terms_agg = {
        terms: {
          field: termsField,
          size: query.termsSize || 10,
          order: { _count: query.metricSortOrder || 'desc' },
        },
      };
    } else if (hasGroupBy && displayMode === 'table') {
      const orderKey = isNumericAgg ? 'sub_metric' : '_count';
      const orderDir = query.metricSortOrder || 'desc';
      aggs.group_terms = {
        terms: {
          field: query.groupBy,
          size: query.termsSize || 20,
          order: { [orderKey]: orderDir },
        },
      };
      if (isNumericAgg && query.metricField) {
        if (aggType === MetricAggType.Percentiles) {
          aggs.group_terms.aggs = {
            sub_metric: {
              percentiles: { field: query.metricField, percents: [50, 90, 95, 99] },
            },
          };
          aggs.group_terms.terms.order = { _count: orderDir };
        } else {
          aggs.group_terms.aggs = {
            sub_metric: { [aggType]: { field: query.metricField } },
          };
        }
      }
    } else if (hasGroupBy) {
      const subAggs: Record<string, any> = {
        time_histogram: {
          date_histogram: { field: tsField, fixed_interval: interval },
        },
      };
      if (isNumericAgg && query.metricField) {
        if (aggType === MetricAggType.Percentiles) {
          subAggs.time_histogram.aggs = {
            metric: { percentiles: { field: query.metricField, percents: [50, 90, 95, 99] } },
          };
        } else {
          subAggs.time_histogram.aggs = {
            metric: { [aggType]: { field: query.metricField } },
          };
        }
      }
      aggs.group_terms = {
        terms: { field: query.groupBy, size: query.termsSize || 10, order: { _count: 'desc' } },
        aggs: subAggs,
      };
    } else {
      aggs.time_histogram = {
        date_histogram: { field: tsField, fixed_interval: interval },
      };
      if (isNumericAgg && query.metricField) {
        if (aggType === MetricAggType.Percentiles) {
          aggs.time_histogram.aggs = {
            metric: { percentiles: { field: query.metricField, percents: [50, 90, 95, 99] } },
          };
        } else {
          aggs.time_histogram.aggs = {
            metric: { [aggType]: { field: query.metricField } },
          };
        }
      }
    }

    const body = {
      query: lucene,
      max_hits: 0,
      start_timestamp: Math.floor(fromMs / 1000),
      end_timestamp: Math.ceil(toMs / 1000),
      aggs,
    };

    try {
      const resp = await this.post<QwSearchResponse>(`/api/v1/${index}/search`, body);

      if (aggType === MetricAggType.Terms && resp?.aggregations?.terms_agg?.buckets) {
        return this.buildTermsFrame(resp.aggregations.terms_agg.buckets, query);
      }

      if (hasGroupBy && resp?.aggregations?.group_terms?.buckets) {
        const groupBuckets = resp.aggregations.group_terms.buckets;
        if (displayMode === 'table') {
          return this.buildGroupedMetricTable(groupBuckets, query);
        } else {
          return this.buildGroupedTimeSeries(groupBuckets, query);
        }
      }

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
        if (isNumericAgg && b.metric) {
          values.push(b.metric.value ?? 0);
        } else {
          values.push(b.doc_count);
        }
      }

      const label = aggType === MetricAggType.Count ? 'Count' : `${aggType}(${query.metricField || ''})`;
      return [new MutableDataFrame({
        refId: query.refId,
        fields: [
          { name: 'Time', type: FieldType.time, values: times },
          { name: label, type: FieldType.number, values },
        ],
      })];
    } catch (e: any) {
      return [this.buildErrorFrame(query.refId,
        `Aggregation failed: ${extractErrorMessage(e)}. Ensure "${tsField}" is a fast field.`)];
    }
  }

  private buildTermsFrame(buckets: any[], query: QuickwitQuery): MutableDataFrame[] {
    const keys: string[] = [];
    const counts: number[] = [];
    for (const b of buckets) {
      keys.push(String(b.key));
      counts.push(b.doc_count);
    }
    return [new MutableDataFrame({
      refId: query.refId,
      meta: { preferredVisualisationType: 'table' },
      fields: [
        { name: query.termsField || 'Value', type: FieldType.string, values: keys },
        { name: 'Count', type: FieldType.number, values: counts },
      ],
    })];
  }

  private buildGroupedMetricTable(groupBuckets: any[], query: QuickwitQuery): MutableDataFrame[] {
    const aggType = query.metricType || MetricAggType.Count;
    const isNumericAgg = aggType !== MetricAggType.Count && aggType !== MetricAggType.Terms;
    const keys: string[] = [];
    const counts: number[] = [];
    const metricValues: number[] = [];
    const p50Vals: number[] = [];
    const p90Vals: number[] = [];
    const p95Vals: number[] = [];
    const p99Vals: number[] = [];

    for (const b of groupBuckets) {
      keys.push(String(b.key));
      counts.push(b.doc_count);
      if (isNumericAgg && b.sub_metric) {
        if (aggType === MetricAggType.Percentiles) {
          const pvals = b.sub_metric?.values || {};
          p50Vals.push(pvals['50.0'] ?? 0);
          p90Vals.push(pvals['90.0'] ?? 0);
          p95Vals.push(pvals['95.0'] ?? 0);
          p99Vals.push(pvals['99.0'] ?? 0);
        } else {
          metricValues.push(b.sub_metric.value ?? 0);
        }
      }
    }

    const fields: any[] = [
      { name: query.groupBy || 'Group', type: FieldType.string, values: keys },
      { name: 'Count', type: FieldType.number, values: counts },
    ];
    if (isNumericAgg && query.metricField) {
      if (aggType === MetricAggType.Percentiles) {
        fields.push({ name: `p50(${query.metricField})`, type: FieldType.number, values: p50Vals });
        fields.push({ name: `p90(${query.metricField})`, type: FieldType.number, values: p90Vals });
        fields.push({ name: `p95(${query.metricField})`, type: FieldType.number, values: p95Vals });
        fields.push({ name: `p99(${query.metricField})`, type: FieldType.number, values: p99Vals });
      } else {
        fields.push({ name: `${aggType}(${query.metricField})`, type: FieldType.number, values: metricValues });
      }
    }
    return [new MutableDataFrame({
      refId: query.refId,
      meta: { preferredVisualisationType: 'table' },
      fields,
    })];
  }

  private buildGroupedTimeSeries(groupBuckets: any[], query: QuickwitQuery): MutableDataFrame[] {
    const frames: MutableDataFrame[] = [];
    const aggType = query.metricType || MetricAggType.Count;
    const isNumericAgg = aggType !== MetricAggType.Count && aggType !== MetricAggType.Terms;

    for (const group of groupBuckets) {
      const groupKey = String(group.key);
      const timeBuckets = group.time_histogram?.buckets || [];
      const times: number[] = [];
      const values: number[] = [];
      for (const b of timeBuckets) {
        times.push(bucketKeyToMs(b.key));
        if (isNumericAgg && b.metric) {
          values.push(b.metric.value ?? 0);
        } else {
          values.push(b.doc_count);
        }
      }
      if (times.length > 0) {
        const seriesName = isNumericAgg
          ? `${aggType}(${query.metricField}) [${groupKey}]`
          : `count [${groupKey}]`;
        frames.push(new MutableDataFrame({
          refId: query.refId,
          fields: [
            { name: 'Time', type: FieldType.time, values: times },
            {
              name: seriesName,
              type: FieldType.number,
              values,
              config: { displayNameFromDS: `${query.groupBy}: ${groupKey}` },
            },
          ],
        }));
      }
    }
    if (frames.length === 0) {
      return [this.buildErrorFrame(query.refId, `No data for group by "${query.groupBy}".`)];
    }
    return frames;
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
    return [new MutableDataFrame({
      refId: query.refId,
      fields: [
        { name: 'Time', type: FieldType.time, values: times },
        { name: `p50(${field})`, type: FieldType.number, values: p50 },
        { name: `p90(${field})`, type: FieldType.number, values: p90 },
        { name: `p95(${field})`, type: FieldType.number, values: p95 },
        { name: `p99(${field})`, type: FieldType.number, values: p99 },
      ],
    })];
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
        // @ts-ignore - signal is supported but not in the type definition
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
