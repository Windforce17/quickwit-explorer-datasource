import {
  DataQueryRequest,
  DataQueryResponse,
  DataSourceApi,
  DataSourceInstanceSettings,
  DataSourceWithSupplementaryQueriesSupport,
  FieldType,
  LogLevel,
  MutableDataFrame,
  SupplementaryQueryOptions,
  SupplementaryQueryType,
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
  defaultQuery,
} from './types';

/** Default timeout for trace lookups (ms) */
const TRACE_TIMEOUT_MS = 5000;

/**
 * Compute a reasonable histogram interval string based on the time range.
 * Aims for roughly 50-80 buckets.
 */
function computeAutoInterval(fromMs: number, toMs: number): string {
  const rangeSec = (toMs - fromMs) / 1000;
  if (rangeSec <= 300) return '5s';        // <= 5min
  if (rangeSec <= 1800) return '15s';      // <= 30min
  if (rangeSec <= 7200) return '1m';       // <= 2h
  if (rangeSec <= 21600) return '5m';      // <= 6h
  if (rangeSec <= 86400) return '15m';     // <= 1d
  if (rangeSec <= 604800) return '1h';     // <= 7d
  if (rangeSec <= 2592000) return '6h';    // <= 30d
  return '1d';
}

/**
 * Detect the timestamp field name from index metadata.
 */
function detectTimestampField(indexes: QwIndex[], indexId: string): string {
  const idx = indexes.find((i) => i.index_config.index_id === indexId);
  if (idx?.index_config?.doc_mapping?.timestamp_field) {
    return idx.index_config.doc_mapping.timestamp_field;
  }
  return 'timestamp';
}

export class QuickwitExplorerDatasource
  extends DataSourceApi<QuickwitQuery, QuickwitOptions>
  implements DataSourceWithSupplementaryQueriesSupport<QuickwitQuery>
{
  baseUrl: string;
  quickwitUrl: string;
  defaultIndex: string;
  logMessageField: string;
  logLevelField: string;
  traceIndex: string;
  logIndex: string;

  // Cache for index list
  private indexCache: { data: QwIndex[]; ts: number } | null = null;
  private readonly INDEX_CACHE_TTL = 30000; // 30s

  // Cache for fields per index
  private fieldCache: Map<string, { data: string[]; ts: number }> = new Map();
  private readonly FIELD_CACHE_TTL = 60000; // 60s

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
  //  SUPPLEMENTARY QUERIES (Logs Volume Histogram)
  // ================================================================

  getSupportedSupplementaryQueryTypes(): SupplementaryQueryType[] {
    return [SupplementaryQueryType.LogsVolume];
  }

  getSupplementaryQuery(
    options: SupplementaryQueryOptions,
    query: QuickwitQuery
  ): QuickwitQuery | undefined {
    if (!this.getSupportedSupplementaryQueryTypes().includes(options.type)) {
      return undefined;
    }

    // Only provide volume for log queries
    if (query.queryType && query.queryType !== QueryType.Logs) {
      return undefined;
    }

    switch (options.type) {
      case SupplementaryQueryType.LogsVolume:
        return {
          ...query,
          refId: `logs-volume-${query.refId}`,
          queryType: QueryType.Metrics,
          // We use a special marker so queryMetrics knows this is a volume query
          metricType: '__logs_volume__',
        };
      default:
        return undefined;
    }
  }

  getSupplementaryRequest(
    type: SupplementaryQueryType,
    request: DataQueryRequest<QuickwitQuery>
  ): DataQueryRequest<QuickwitQuery> | undefined {
    if (!this.getSupportedSupplementaryQueryTypes().includes(type)) {
      return undefined;
    }

    const targets = request.targets
      .map((query) => this.getSupplementaryQuery({ type }, query))
      .filter((query): query is QuickwitQuery => !!query);

    if (!targets.length) {
      return undefined;
    }

    return { ...request, targets };
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
    if (!indexId) {
      return [];
    }
    const now = Date.now();
    const cached = this.fieldCache.get(indexId);
    if (cached && now - cached.ts < this.FIELD_CACHE_TTL) {
      return cached.data;
    }

    const indexes = await this.getIndexes();
    const idx = indexes.find((i) => i.index_config.index_id === indexId);
    if (!idx) {
      return [];
    }
    const fields = flattenFieldMappings(idx.index_config.doc_mapping.field_mappings);
    this.fieldCache.set(indexId, { data: fields, ts: now });
    return fields;
  }

  // ================================================================
  //  LOG QUERIES
  // ================================================================

  private async queryLogs(
    query: QuickwitQuery,
    options: DataQueryRequest<QuickwitQuery>
  ): Promise<MutableDataFrame[]> {
    const index = query.index || this.logIndex || this.defaultIndex;
    if (!index) {
      return [];
    }

    const fromMs = options.range.from.valueOf();
    const toMs = options.range.to.valueOf();
    const from = fromMs / 1000;
    const to = toMs / 1000;
    const lucene = getTemplateSrv().replace(query.query || '*', options.scopedVars);

    // Detect timestamp field from index metadata
    const indexes = await this.getIndexes();
    const tsField = detectTimestampField(indexes, index);

    const body: Record<string, any> = {
      query: lucene,
      max_hits: query.size || 100,
      start_timestamp: Math.floor(from),
      end_timestamp: Math.ceil(to),
      sort_by: `${tsField}:desc`,
    };

    const resp = await this.post<QwSearchResponse>(`/api/v1/${index}/search`, body);

    const frames: MutableDataFrame[] = [];

    // Build logs frame
    if (resp?.hits?.length) {
      const msgField = this.logMessageField;
      const lvlField = this.logLevelField;
      const hasTraceIndex = !!this.traceIndex;

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

      for (const hit of resp.hits) {
        const ts = extractTimestamp(hit);
        const msg = getNestedValue(hit, msgField);
        const level = getNestedValue(hit, lvlField) || '';
        const traceId = getNestedValue(hit, 'trace_id') || '';
        const spanId = getNestedValue(hit, 'span_id') || '';

        frame.add({
          timestamp: ts,
          body: typeof msg === 'string' ? msg : JSON.stringify(hit),
          severity: String(level),
          id: `${ts}-${traceId}-${spanId}`,
          traceID: traceId,
          spanID: spanId,
          labels: hit,
        });
      }

      // Only add trace link if a trace index is configured
      if (hasTraceIndex) {
        const traceIdField = frame.fields.find((f) => f.name === 'traceID');
        if (traceIdField) {
          traceIdField.config = {
            ...traceIdField.config,
            links: [
              {
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
              },
            ],
          };
        }
      }

      // Add quick-filter data links on severity field
      const severityField = frame.fields.find((f) => f.name === 'severity');
      if (severityField) {
        severityField.config = {
          ...severityField.config,
          links: [
            {
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
            },
          ],
        };
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
      return [this.buildTraceErrorFrame(query.refId, 'No trace index configured. Set a Trace Index in the data source settings.')];
    }

    const traceId = getTemplateSrv().replace(query.traceId || '', options.scopedVars);
    if (!traceId) {
      return [];
    }

    try {
      const resp = await this.getWithTimeout<JaegerApiResponse<JaegerTrace[]>>(
        `/api/v1/${index}/jaeger/api/traces/${traceId}`,
        TRACE_TIMEOUT_MS
      );
      if (!resp?.data?.length) {
        return [this.buildTraceErrorFrame(query.refId, `Trace ${traceId} not found in index "${index}".`)];
      }
      return [this.jaegerTraceToFrame(resp.data[0], query.refId)];
    } catch (e: any) {
      const msg = e?.message || String(e);
      if (msg.includes('timeout') || msg.includes('aborted')) {
        return [this.buildTraceErrorFrame(query.refId, `Trace lookup timed out after ${TRACE_TIMEOUT_MS / 1000}s. The trace may not exist in index "${index}".`)];
      }
      return [this.buildTraceErrorFrame(query.refId, `Trace lookup failed: ${msg}`)];
    }
  }

  private buildTraceErrorFrame(refId: string, message: string): MutableDataFrame {
    const frame = new MutableDataFrame({
      refId,
      meta: {
        notices: [{ severity: 'warning' as any, text: message }],
      },
      fields: [
        { name: 'message', type: FieldType.string, values: [message] },
      ],
    });
    return frame;
  }

  private async queryTraceSearch(
    query: QuickwitQuery,
    options: DataQueryRequest<QuickwitQuery>
  ): Promise<MutableDataFrame[]> {
    const index = query.index || this.traceIndex;
    if (!index) {
      return [this.buildTraceErrorFrame(query.refId, 'No trace index configured.')];
    }

    if (query.traceId) {
      return this.queryTraceById(
        { ...query, queryType: QueryType.TraceId },
        options
      );
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
    if (query.minDuration) {
      params.set('minDuration', query.minDuration);
    }
    if (query.maxDuration) {
      params.set('maxDuration', query.maxDuration);
    }

    const from = options.range.from.valueOf() * 1000; // ms -> us
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
        return [this.buildTraceErrorFrame(query.refId, 'No traces found matching the search criteria.')];
      }

      const table = this.buildTraceSearchTable(resp.data, query.refId);
      return [table];
    } catch (e: any) {
      const msg = e?.message || String(e);
      return [this.buildTraceErrorFrame(query.refId, `Trace search failed: ${msg}`)];
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
      const process = trace.processes[span.processID] || {
        serviceName: 'unknown',
        tags: [],
      };
      const parentRef = span.references?.find((r) => r.refType === 'CHILD_OF');

      frame.add({
        traceID: span.traceID,
        spanID: span.spanID,
        parentSpanID: parentRef?.spanID || '',
        operationName: span.operationName,
        serviceName: process.serviceName,
        serviceTags: process.tags || [],
        startTime: span.startTime / 1000, // us -> ms
        duration: span.duration / 1000,   // us -> ms
        logs: (span.logs || []).map((l) => ({
          timestamp: l.timestamp / 1000,
          fields: l.fields,
        })),
        tags: span.tags || [],
        warnings: span.warnings || [],
      });
    }

    // Add log link on each span if log index is configured
    if (this.logIndex) {
      const traceIdField = frame.fields.find((f) => f.name === 'traceID');
      if (traceIdField) {
        traceIdField.config = {
          ...traceIdField.config,
          links: [
            {
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
            },
          ],
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
      if (!trace.spans?.length) {
        continue;
      }

      const rootSpan = trace.spans.reduce((a, b) =>
        a.startTime < b.startTime ? a : b
      );
      const process = trace.processes[rootSpan.processID];
      const maxEnd = trace.spans.reduce(
        (max, s) => Math.max(max, s.startTime + s.duration),
        0
      );

      frame.add({
        'Trace ID': trace.traceID,
        'Trace Name': rootSpan.operationName,
        Service: process?.serviceName || 'unknown',
        'Start Time': Math.floor(rootSpan.startTime / 1000),
        'Duration (ms)': Math.round((maxEnd - rootSpan.startTime) / 1000),
        Spans: trace.spans.length,
      });
    }

    // Add data link on Trace ID to open trace view
    const traceIdField = frame.fields.find((f) => f.name === 'Trace ID');
    if (traceIdField) {
      traceIdField.config = {
        links: [
          {
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
          },
        ],
      };
    }

    // Add quick-filter on Service column
    const serviceField = frame.fields.find((f) => f.name === 'Service');
    if (serviceField) {
      serviceField.config = {
        links: [
          {
            title: 'Filter traces by service: ${__value.raw}',
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
          },
        ],
      };
    }

    return frame;
  }

  // ================================================================
  //  TRACE SERVICES & OPERATIONS (for query editor dropdowns)
  // ================================================================

  async getServices(index?: string): Promise<string[]> {
    const idx = index || this.traceIndex;
    if (!idx) {
      return [];
    }
    try {
      const resp = await this.getWithTimeout<JaegerApiResponse<string[]>>(
        `/api/v1/${idx}/jaeger/api/services`,
        TRACE_TIMEOUT_MS
      );
      return resp?.data || [];
    } catch {
      return [];
    }
  }

  async getOperations(service: string, index?: string): Promise<string[]> {
    const idx = index || this.traceIndex;
    if (!idx || !service) {
      return [];
    }
    try {
      const resp = await this.getWithTimeout<JaegerApiResponse<string[]>>(
        `/api/v1/${idx}/jaeger/api/services/${encodeURIComponent(service)}/operations`,
        TRACE_TIMEOUT_MS
      );
      return resp?.data || [];
    } catch {
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
    // Determine which index to use
    const index = query.index || this.logIndex || this.defaultIndex;
    if (!index) {
      return [];
    }

    const fromMs = options.range.from.valueOf();
    const toMs = options.range.to.valueOf();
    const from = fromMs / 1000;
    const to = toMs / 1000;
    const lucene = getTemplateSrv().replace(query.query || '*', options.scopedVars);

    // Detect timestamp field from index metadata
    const indexes = await this.getIndexes();
    const tsField = detectTimestampField(indexes, index);

    // Compute interval
    const interval = query.groupByInterval || computeAutoInterval(fromMs, toMs);
    const groupByField = query.groupBy || tsField;

    const aggs: Record<string, any> = {
      time_histogram: {
        date_histogram: {
          field: groupByField,
          fixed_interval: interval,
        },
      },
    };

    const body = {
      query: lucene,
      max_hits: 0,
      start_timestamp: Math.floor(from),
      end_timestamp: Math.ceil(to),
      aggs,
    };

    try {
      const resp = await this.post<QwSearchResponse>(`/api/v1/${index}/search`, body);

      if (!resp?.aggregations?.time_histogram?.buckets) {
        // If no aggregation results, return empty frame
        return [new MutableDataFrame({
          refId: query.refId,
          fields: [
            { name: 'Time', type: FieldType.time, values: [] },
            { name: 'Count', type: FieldType.number, values: [] },
          ],
        })];
      }

      const buckets = resp.aggregations.time_histogram.buckets as any[];
      const times: number[] = [];
      const values: number[] = [];

      for (const b of buckets) {
        // Quickwit date_histogram returns key in milliseconds
        let keyMs = b.key;
        // Safety check: if key looks like microseconds (> year 2100 in ms), convert
        if (keyMs > 4102444800000) {
          keyMs = keyMs / 1000;
        }
        // If key looks like seconds
        if (keyMs < 4102444800) {
          keyMs = keyMs * 1000;
        }
        times.push(keyMs);
        values.push(b.doc_count);
      }

      // For logs volume supplementary query, use special meta
      const isLogsVolume = query.metricType === '__logs_volume__';

      const frame = new MutableDataFrame({
        refId: query.refId,
        meta: isLogsVolume
          ? {
              custom: {
                logsVolumeType: 'Full',
              },
            }
          : undefined,
        fields: [
          { name: 'Time', type: FieldType.time, values: times },
          {
            name: isLogsVolume ? 'Volume' : 'Count',
            type: FieldType.number,
            values,
            config: isLogsVolume
              ? {
                  displayNameFromDS: 'Log volume',
                  color: { mode: 'fixed', fixedColor: 'green' },
                }
              : {},
          },
        ],
      });

      return [frame];
    } catch (e: any) {
      console.error('Metrics query failed:', e);
      return [new MutableDataFrame({
        refId: query.refId,
        meta: {
          notices: [{ severity: 'error' as any, text: `Aggregation query failed: ${e?.message || e}. Ensure the timestamp field is a "fast" field in Quickwit.` }],
        },
        fields: [
          { name: 'Time', type: FieldType.time, values: [] },
          { name: 'Count', type: FieldType.number, values: [] },
        ],
      })];
    }
  }

  // ================================================================
  //  HEALTH CHECK
  // ================================================================

  async testDatasource(): Promise<any> {
    try {
      const indexes = await this.getIndexes();
      return {
        status: 'success',
        message: `Connected to Quickwit successfully. Found ${indexes.length} indexes.`,
      };
    } catch (e: any) {
      return {
        status: 'error',
        message: `Connection failed: ${e?.message || e}`,
      };
    }
  }

  // ================================================================
  //  HTTP HELPERS (via Grafana proxy)
  // ================================================================

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
        // @ts-ignore - signal is supported but not in type defs
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

// ================================================================
//  UTILITY FUNCTIONS
// ================================================================

function flattenFieldMappings(mappings: QwFieldMapping[], prefix = ''): string[] {
  const fields: string[] = [];
  for (const m of mappings) {
    const fullName = prefix ? `${prefix}.${m.name}` : m.name;
    if (m.field_mappings) {
      fields.push(...flattenFieldMappings(m.field_mappings, fullName));
    } else {
      fields.push(fullName);
    }
  }
  return fields;
}

function extractTimestamp(hit: any): number {
  const tsFields = [
    'timestamp',
    '_timestamp',
    'timestamp_nanos',
    '@timestamp',
    'span_start_timestamp_nanos',
  ];
  for (const field of tsFields) {
    const val = getNestedValue(hit, field);
    if (val !== undefined && val !== null) {
      const num = Number(val);
      if (!isNaN(num)) {
        if (num > 1e18) return Math.floor(num / 1e6); // nanos -> ms
        if (num > 1e15) return Math.floor(num / 1e3); // micros -> ms
        if (num > 1e12) return num;                     // already ms
        return num * 1000;                              // seconds -> ms
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
