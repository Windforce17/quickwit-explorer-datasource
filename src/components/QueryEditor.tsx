import React, { useState, useEffect, useLayoutEffect, useCallback, useRef, useMemo } from 'react';
import { QueryEditorProps, SelectableValue } from '@grafana/data';
import {
  InlineField,
  Input,
  Select,
  InlineFieldRow,
  RadioButtonGroup,
  Button,
  IconButton,
  useTheme2,
  Collapse,
} from '@grafana/ui';
import { QuickwitExplorerDatasource } from '../datasource';
import {
  QuickwitQuery, QuickwitOptions, QueryType, MetricAggType, MetricDisplayMode,
  MetricAggregation, MetricAggregationType, BucketAggregation, BucketAggregationType,
  defaultQuery, defaultMetrics, defaultBucketAggs,
} from '../types';

type Props = QueryEditorProps<QuickwitExplorerDatasource, QuickwitQuery, QuickwitOptions>;

const queryTypeOptions = [
  { label: 'Logs', value: QueryType.Logs, description: 'Search log entries' },
  { label: 'Traces', value: QueryType.Traces, description: 'Search and view traces' },
  { label: 'Trace ID', value: QueryType.TraceId, description: 'Look up a specific trace by ID' },
  { label: 'Metrics', value: QueryType.Metrics, description: 'Aggregation queries' },
];

const sizeOptions: Array<SelectableValue<number>> = [
  { label: '10', value: 10 },
  { label: '50', value: 50 },
  { label: '100', value: 100 },
  { label: '500', value: 500 },
  { label: '1000', value: 1000 },
];

const traceLimitOptions: Array<SelectableValue<number>> = [
  { label: '10', value: 10 },
  { label: '20', value: 20 },
  { label: '50', value: 50 },
  { label: '100', value: 100 },
  { label: '200', value: 200 },
];

const metricTypeOptions: Array<SelectableValue<MetricAggregationType>> = [
  { label: 'Count', value: 'count' },
  { label: 'Average', value: 'avg' },
  { label: 'Sum', value: 'sum' },
  { label: 'Min', value: 'min' },
  { label: 'Max', value: 'max' },
  { label: 'Percentiles', value: 'percentiles' },
  { label: 'Unique Count', value: 'cardinality' },
];

const bucketTypeOptions: Array<SelectableValue<BucketAggregationType>> = [
  { label: 'Terms', value: 'terms' },
  { label: 'Date Histogram', value: 'date_histogram' },
];

const orderOptions: Array<SelectableValue<string>> = [
  { label: 'Top', value: 'desc' },
  { label: 'Bottom', value: 'asc' },
];

const sizeSelectOptions: Array<SelectableValue<string>> = [
  { label: '5', value: '5' },
  { label: '10', value: '10' },
  { label: '15', value: '15' },
  { label: '20', value: '20' },
  { label: '50', value: '50' },
  { label: '100', value: '100' },
  { label: 'No limit', value: '0' },
];

// Legacy options for backward compat
const legacyMetricAggOptions: Array<SelectableValue<string>> = [
  { label: 'Count', value: MetricAggType.Count },
  { label: 'Average', value: MetricAggType.Avg },
  { label: 'Sum', value: MetricAggType.Sum },
  { label: 'Min', value: MetricAggType.Min },
  { label: 'Max', value: MetricAggType.Max },
  { label: 'Percentiles', value: MetricAggType.Percentiles },
  { label: 'Top Values', value: MetricAggType.Terms },
];

const sortOrderOptions: Array<SelectableValue<string>> = [
  { label: 'Desc', value: 'desc' },
  { label: 'Asc', value: 'asc' },
];

const displayModeOptions: Array<SelectableValue<string>> = [
  { label: 'Time Series', value: MetricDisplayMode.TimeSeries },
  { label: 'Table', value: MetricDisplayMode.Table },
];

const LUCENE_OPERATORS = ['AND', 'OR', 'NOT', 'TO'];

// ================================================================
//  QUERY INPUT WITH AUTOCOMPLETE
// ================================================================

interface QueryInputProps {
  value: string;
  placeholder: string;
  fields: string[];
  onChange: (val: string) => void;
  onRunQuery: () => void;
  rows?: number;
}

function QueryInput({ value, placeholder, fields, onChange, onRunQuery, rows }: QueryInputProps) {
  const theme = useTheme2();
  const [suggestions, setSuggestions] = useState<string[]>([]);
  const [showSuggestions, setShowSuggestions] = useState(false);
  const [selectedIdx, setSelectedIdx] = useState(0);
  const [cursorPos, setCursorPos] = useState(0);
  const inputRef = useRef<HTMLTextAreaElement>(null);

  const allCompletions = useMemo(() => {
    const fieldWithColon = fields.map((f) => `${f}:`);
    return [...fieldWithColon, ...LUCENE_OPERATORS];
  }, [fields]);

  const getCurrentToken = useCallback(
    (text: string, pos: number) => {
      const before = text.substring(0, pos);
      const startMatch = before.match(/[^\s()\[\]]*$/);
      const tokenBefore = startMatch ? startMatch[0] : '';
      const tokenStart = pos - tokenBefore.length;
      const after = text.substring(pos);
      const endMatch = after.match(/^[^\s()\[\]]*/);
      const tokenAfter = endMatch ? endMatch[0] : '';
      return { token: tokenBefore + tokenAfter, start: tokenStart, end: pos + tokenAfter.length };
    }, []
  );

  const updateSuggestions = useCallback(
    (text: string, pos: number) => {
      if (!fields.length) { setSuggestions([]); return; }
      const { token } = getCurrentToken(text, pos);
      if (!token || token.length < 1) { setSuggestions([]); setShowSuggestions(false); return; }
      const lower = token.toLowerCase();
      const matches = allCompletions.filter((c) => c.toLowerCase().startsWith(lower) && c.toLowerCase() !== lower);
      if (matches.length > 0 && matches.length <= 20) {
        setSuggestions(matches); setSelectedIdx(0); setShowSuggestions(true);
      } else {
        setSuggestions([]); setShowSuggestions(false);
      }
    }, [fields, allCompletions, getCurrentToken]
  );

  const applySuggestion = useCallback(
    (suggestion: string) => {
      const { start, end } = getCurrentToken(value, cursorPos);
      const newValue = value.substring(0, start) + suggestion + value.substring(end);
      onChange(newValue);
      setShowSuggestions(false);
      const newPos = start + suggestion.length;
      setTimeout(() => {
        if (inputRef.current) {
          inputRef.current.selectionStart = newPos;
          inputRef.current.selectionEnd = newPos;
        }
      }, 0);
    }, [value, cursorPos, getCurrentToken, onChange]
  );

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (showSuggestions && suggestions.length > 0) {
      if (e.key === 'ArrowDown') { e.preventDefault(); setSelectedIdx((p) => Math.min(p + 1, suggestions.length - 1)); return; }
      if (e.key === 'ArrowUp') { e.preventDefault(); setSelectedIdx((p) => Math.max(p - 1, 0)); return; }
      if (e.key === 'Tab' || e.key === 'Enter') {
        if (suggestions[selectedIdx]) { e.preventDefault(); applySuggestion(suggestions[selectedIdx]); return; }
      }
      if (e.key === 'Escape') { e.preventDefault(); setShowSuggestions(false); return; }
    }
    if (e.key === 'Enter' && (e.shiftKey || e.ctrlKey)) { e.preventDefault(); setShowSuggestions(false); onRunQuery(); }
  };

  // Track desired cursor position for restoration after re-render
  const cursorRestoreRef = useRef<number | null>(null);

  const handleChange = (e: React.ChangeEvent<HTMLTextAreaElement>) => {
    const newVal = e.target.value;
    const pos = e.target.selectionStart || 0;
    cursorRestoreRef.current = pos;
    onChange(newVal);
    setCursorPos(pos);
    updateSuggestions(newVal, pos);
  };

  // Restore cursor position after React re-renders the controlled textarea
  useLayoutEffect(() => {
    if (cursorRestoreRef.current !== null && inputRef.current) {
      inputRef.current.selectionStart = cursorRestoreRef.current;
      inputRef.current.selectionEnd = cursorRestoreRef.current;
      cursorRestoreRef.current = null;
    }
  });

  const handleBlur = () => {
    setTimeout(() => setShowSuggestions(false), 200);
    onRunQuery();
  };

  return (
    <div style={{ position: 'relative', width: '100%' }}>
      <textarea
        ref={inputRef}
        value={value}
        placeholder={placeholder}
        onChange={handleChange}
        onKeyDown={handleKeyDown}
        onBlur={handleBlur}
        onClick={(e) => setCursorPos((e.target as HTMLTextAreaElement).selectionStart || 0)}
        rows={rows || 2}
        style={{
          width: '100%', fontFamily: 'monospace', fontSize: 12, padding: '6px 8px',
          border: `1px solid ${theme.colors.border.medium}`, borderRadius: 4,
          background: theme.colors.background.canvas, color: theme.colors.text.primary,
          resize: 'vertical', lineHeight: 1.5,
        }}
      />
      {showSuggestions && suggestions.length > 0 && (
        <div style={{
          position: 'absolute', top: '100%', left: 0, right: 0, zIndex: 1000,
          maxHeight: 200, overflowY: 'auto', background: theme.colors.background.primary,
          border: `1px solid ${theme.colors.border.medium}`, borderRadius: 4, boxShadow: theme.shadows.z2,
        }}>
          {suggestions.map((s, i) => (
            <div key={s}
              style={{
                padding: '4px 8px', cursor: 'pointer', fontSize: 12, fontFamily: 'monospace',
                background: i === selectedIdx ? theme.colors.action.hover : 'transparent',
              }}
              onMouseDown={(e) => { e.preventDefault(); applySuggestion(s); }}
              onMouseEnter={() => setSelectedIdx(i)}
            >
              {s.endsWith(':') ? (
                <><span style={{ color: theme.colors.text.link }}>{s.slice(0, -1)}</span><span style={{ color: theme.colors.text.secondary }}>:</span></>
              ) : (
                <span style={{ color: theme.colors.warning.text, fontWeight: 'bold' }}>{s}</span>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

// ================================================================
//  QUICK FILTER CHIPS
// ================================================================

interface QuickFilterProps {
  query: string;
  fields: string[];
  onChange: (newQuery: string) => void;
  onRunQuery: () => void;
}

function QuickFilterBar({ query, fields, onChange, onRunQuery }: QuickFilterProps) {
  const theme = useTheme2();
  const filters = useMemo(() => {
    if (!query || query === '*') return [];
    return query.split(/\s+AND\s+/i).map((f) => f.trim()).filter((f) => f && f !== '*');
  }, [query]);

  const removeFilter = (idx: number) => {
    const newFilters = filters.filter((_, i) => i !== idx);
    const newQuery = newFilters.length > 0 ? newFilters.join(' AND ') : '*';
    onChange(newQuery);
    onRunQuery();
  };

  const addFilter = (field: string) => {
    const newFilter = `${field}:`;
    const current = query && query !== '*' ? query : '';
    onChange(current ? `${current} AND ${newFilter}` : newFilter);
  };

  if (filters.length === 0 && fields.length === 0) return null;

  const chipStyle: React.CSSProperties = {
    display: 'inline-flex', alignItems: 'center', padding: '2px 8px', margin: '2px 4px 2px 0',
    borderRadius: 12, fontSize: 11, fontFamily: 'monospace',
  };

  const popularFields = fields.slice(0, 8);

  return (
    <div style={{ padding: '4px 0', display: 'flex', flexWrap: 'wrap', alignItems: 'center', gap: 2 }}>
      {filters.map((f, i) => (
        <span key={i} style={{ ...chipStyle, background: theme.colors.info.transparent, color: theme.colors.info.text, border: `1px solid ${theme.colors.info.border}` }}>
          {f}
          <span onClick={() => removeFilter(i)} style={{ marginLeft: 4, cursor: 'pointer', fontWeight: 'bold' }}>&times;</span>
        </span>
      ))}
      {popularFields.map((f) => (
        <span key={f} onClick={() => addFilter(f)}
          style={{ ...chipStyle, background: theme.colors.background.secondary, color: theme.colors.text.secondary, cursor: 'pointer', border: `1px solid ${theme.colors.border.weak}` }}
          title={`Add filter: ${f}`}
        >
          + {f}
        </span>
      ))}
    </div>
  );
}

// ================================================================
//  METRIC ROW EDITOR
// ================================================================

interface MetricRowProps {
  metric: MetricAggregation;
  index: number;
  fieldOptions: Array<SelectableValue<string>>;
  onUpdate: (m: MetricAggregation) => void;
  onRemove: () => void;
  onToggleHide: () => void;
  canRemove: boolean;
}

function MetricRow({ metric, index, fieldOptions, onUpdate, onRemove, onToggleHide, canRemove }: MetricRowProps) {
  const theme = useTheme2();
  const needsField = metric.type !== 'count';

  const label = index === 0 ? 'Metric' : 'Then';

  return (
    <InlineFieldRow>
      <InlineField label={label} labelWidth={10}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
          <IconButton
            name={metric.hide ? 'eye-slash' : 'eye'}
            size="sm"
            tooltip={metric.hide ? 'Show metric' : 'Hide metric'}
            onClick={onToggleHide}
          />
          {canRemove && (
            <IconButton name="trash-alt" size="sm" tooltip="Remove metric" onClick={onRemove} />
          )}
        </div>
      </InlineField>
      <InlineField>
        <Select
          width={18}
          options={metricTypeOptions}
          value={metric.type}
          onChange={(v) => onUpdate({ ...metric, type: v.value || 'count' })}
        />
      </InlineField>
      {needsField && (
        <InlineField>
          <Select
            width={25}
            options={fieldOptions}
            value={metric.field || ''}
            onChange={(v) => onUpdate({ ...metric, field: v.value || '' })}
            placeholder="Select field..."
            isSearchable
            allowCustomValue
          />
        </InlineField>
      )}
      {metric.type === 'percentiles' && (
        <InlineField label="Percents" labelWidth={8}>
          <Input
            width={20}
            value={(metric.settings?.percents || ['50', '90', '95', '99']).join(', ')}
            placeholder="50, 90, 95, 99"
            onChange={(e: React.ChangeEvent<HTMLInputElement>) => {
              const percents = e.target.value.split(',').map((s) => s.trim()).filter(Boolean);
              onUpdate({ ...metric, settings: { ...metric.settings, percents } });
            }}
          />
        </InlineField>
      )}
    </InlineFieldRow>
  );
}

// ================================================================
//  BUCKET AGG ROW EDITOR
// ================================================================

interface BucketRowProps {
  bucket: BucketAggregation;
  index: number;
  fieldOptions: Array<SelectableValue<string>>;
  metrics: MetricAggregation[];
  onUpdate: (b: BucketAggregation) => void;
  onRemove: () => void;
  canRemove: boolean;
  onRunQuery: () => void;
}

function BucketRow({ bucket, index, fieldOptions, metrics, onUpdate, onRemove, canRemove, onRunQuery }: BucketRowProps) {
  const [showSettings, setShowSettings] = useState(false);
  const label = index === 0 ? 'Group By' : 'Then By';

  // Build order-by options: _count, _key, or any metric
  const orderByOptions: Array<SelectableValue<string>> = [
    { label: 'Doc Count', value: '_count' },
    { label: 'Term value', value: '_key' },
    ...metrics
      .filter((m) => m.type !== 'count')
      .map((m) => ({
        label: `${m.type.charAt(0).toUpperCase() + m.type.slice(1)} ${m.field || ''}`,
        value: m.id,
      })),
  ];

  const settings = bucket.settings || {};

  // Build summary string for collapsed settings
  const summaryParts: string[] = [];
  if (bucket.type === 'terms') {
    const size = settings.size || '10';
    if (size !== '0') summaryParts.push(`Top ${size}`);
    else summaryParts.push('No limit');
    if (settings.min_doc_count && settings.min_doc_count !== '1') {
      summaryParts.push(`Min Doc Count: ${settings.min_doc_count}`);
    }
    if (settings.orderBy && settings.orderBy !== '_count') {
      const orderLabel = orderByOptions.find((o) => o.value === settings.orderBy)?.label || settings.orderBy;
      summaryParts.push(`Order by: ${orderLabel} (${settings.order || 'desc'})`);
    }
  } else if (bucket.type === 'date_histogram') {
    if (settings.interval) summaryParts.push(`Interval: ${settings.interval}`);
    else summaryParts.push('Interval: auto');
  }
  const summary = summaryParts.join(', ');

  return (
    <div style={{ marginBottom: 4 }}>
      <InlineFieldRow>
        <InlineField label={label} labelWidth={10}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
            {canRemove && (
              <IconButton name="trash-alt" size="sm" tooltip="Remove group by" onClick={onRemove} />
            )}
          </div>
        </InlineField>
        <InlineField>
          <Select
            width={16}
            options={bucketTypeOptions}
            value={bucket.type}
            onChange={(v) => onUpdate({ ...bucket, type: v.value || 'date_histogram', settings: {} })}
          />
        </InlineField>
        {bucket.type === 'terms' && (
          <InlineField>
            <Select
              width={25}
              options={fieldOptions}
              value={bucket.field || ''}
              onChange={(v) => { onUpdate({ ...bucket, field: v.value || '' }); onRunQuery(); }}
              placeholder="Select Field"
              isSearchable
              allowCustomValue
            />
          </InlineField>
        )}
        {bucket.type === 'date_histogram' && (
          <InlineField>
            <Input
              width={12}
              value={settings.interval || ''}
              placeholder="auto"
              onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                onUpdate({ ...bucket, settings: { ...settings, interval: e.target.value } })
              }
              onBlur={onRunQuery}
            />
          </InlineField>
        )}
        <div
          style={{ display: 'flex', alignItems: 'center', cursor: 'pointer', fontSize: 12, marginLeft: 8, color: '#888' }}
          onClick={() => setShowSettings(!showSettings)}
        >
          <span style={{ marginRight: 4 }}>{showSettings ? '▼' : '▶'}</span>
          <span>{summary || 'Options'}</span>
        </div>
      </InlineFieldRow>

      {showSettings && bucket.type === 'terms' && (
        <div style={{ marginLeft: 80, padding: '8px 12px', borderLeft: '2px solid #444', marginBottom: 8 }}>
          <InlineFieldRow>
            <InlineField label="Order" labelWidth={12}>
              <Select
                width={12}
                options={orderOptions}
                value={settings.order || 'desc'}
                onChange={(v) => { onUpdate({ ...bucket, settings: { ...settings, order: v.value as any } }); onRunQuery(); }}
              />
            </InlineField>
            <InlineField label="Size" labelWidth={6}>
              <Select
                width={12}
                options={sizeSelectOptions}
                value={settings.size || '10'}
                onChange={(v) => { onUpdate({ ...bucket, settings: { ...settings, size: v.value || '10' } }); onRunQuery(); }}
                allowCustomValue
              />
            </InlineField>
          </InlineFieldRow>
          <InlineFieldRow>
            <InlineField label="Min Doc Count" labelWidth={14}>
              <Input
                width={8}
                type="number"
                value={settings.min_doc_count || '1'}
                onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                  onUpdate({ ...bucket, settings: { ...settings, min_doc_count: e.target.value } })
                }
                onBlur={onRunQuery}
              />
            </InlineField>
            <InlineField label="Order By" labelWidth={10}>
              <Select
                width={25}
                options={orderByOptions}
                value={settings.orderBy || '_count'}
                onChange={(v) => { onUpdate({ ...bucket, settings: { ...settings, orderBy: v.value || '_count' } }); onRunQuery(); }}
              />
            </InlineField>
          </InlineFieldRow>
          <InlineFieldRow>
            <InlineField label="Missing" labelWidth={12} tooltip="Value to use for documents missing this field">
              <Input
                width={15}
                value={settings.missing || ''}
                placeholder="(none)"
                onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                  onUpdate({ ...bucket, settings: { ...settings, missing: e.target.value } })
                }
                onBlur={onRunQuery}
              />
            </InlineField>
          </InlineFieldRow>
        </div>
      )}

      {showSettings && bucket.type === 'date_histogram' && (
        <div style={{ marginLeft: 80, padding: '8px 12px', borderLeft: '2px solid #444', marginBottom: 8 }}>
          <InlineFieldRow>
            <InlineField label="Interval" labelWidth={12}>
              <Input
                width={12}
                value={settings.interval || ''}
                placeholder="auto"
                onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                  onUpdate({ ...bucket, settings: { ...settings, interval: e.target.value } })
                }
                onBlur={onRunQuery}
              />
            </InlineField>
            <InlineField label="Min Doc Count" labelWidth={14}>
              <Input
                width={8}
                type="number"
                value={settings.min_doc_count_hist || '0'}
                onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                  onUpdate({ ...bucket, settings: { ...settings, min_doc_count_hist: e.target.value } })
                }
                onBlur={onRunQuery}
              />
            </InlineField>
          </InlineFieldRow>
        </div>
      )}
    </div>
  );
}

// ================================================================
//  MAIN QUERY EDITOR
// ================================================================

export function QueryEditor(props: Props) {
  const { datasource, query, onChange, onRunQuery } = props;
  const q = { ...defaultQuery, ...query } as QuickwitQuery;

  const [indexOptions, setIndexOptions] = useState<Array<SelectableValue<string>>>([]);
  const [fields, setFields] = useState<string[]>([]);
  const [services, setServices] = useState<Array<SelectableValue<string>>>([]);
  const [operations, setOperations] = useState<Array<SelectableValue<string>>>([]);
  const [servicesLoading, setServicesLoading] = useState(false);

  const update = (patch: Partial<QuickwitQuery>) => onChange({ ...q, ...patch } as QuickwitQuery);
  const updateAndRun = (patch: Partial<QuickwitQuery>) => { onChange({ ...q, ...patch } as QuickwitQuery); setTimeout(onRunQuery, 50); };

  // Auto-set default index on first mount if not set
  useEffect(() => {
    if (!q.index) {
      const defaultIdx = datasource.logIndex || datasource.defaultIndex || '';
      if (defaultIdx) {
        onChange({ ...q, index: defaultIdx } as QuickwitQuery);
      }
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Load indexes
  useEffect(() => {
    datasource.searchIndexes('').then((ids) => {
      setIndexOptions(ids.map((id) => ({ label: id, value: id })));
    });
  }, [datasource]);

  // Load fields when index changes
  const activeIndex = q.index || datasource.logIndex || datasource.defaultIndex;
  useEffect(() => {
    if (activeIndex) {
      datasource.getFields(activeIndex).then(setFields);
    }
  }, [activeIndex, datasource]);

  // Load services for trace queries
  useEffect(() => {
    if (q.queryType === QueryType.Traces) {
      setServicesLoading(true);
      datasource.getServices(q.index || datasource.traceIndex).then((svcs) => {
        setServices([{ label: 'All', value: '' }, ...svcs.map((s) => ({ label: s, value: s }))]);
      }).catch(() => {
        setServices([{ label: 'All', value: '' }]);
      }).finally(() => {
        setServicesLoading(false);
      });
    }
  }, [q.queryType, q.index, datasource]);

  // Load operations when service changes
  useEffect(() => {
    if (q.queryType === QueryType.Traces && q.serviceName) {
      datasource.getOperations(q.serviceName, q.index || datasource.traceIndex).then((ops) => {
        setOperations([{ label: 'All', value: '' }, ...ops.map((o) => ({ label: o, value: o }))]);
      });
    } else {
      setOperations([]);
    }
  }, [q.queryType, q.serviceName, q.index, datasource]);

  // Build field options for Select dropdowns
  const fieldOptions: Array<SelectableValue<string>> = useMemo(
    () => fields.map((f) => ({ label: f, value: f })),
    [fields]
  );

  // ==================== Metrics State Helpers ====================

  // Determine if using new model or legacy
  const useNewModel = !!(q.metrics && q.metrics.length > 0);

  // Initialize metrics/bucketAggs if switching to Metrics for the first time
  const ensureMetricsModel = () => {
    if (!q.metrics || q.metrics.length === 0) {
      update({ metrics: [...defaultMetrics], bucketAggs: [...defaultBucketAggs] });
    }
  };

  // Generate next ID for metrics/buckets
  const nextId = (): string => {
    const allIds = [
      ...(q.metrics || []).map((m) => parseInt(m.id, 10)),
      ...(q.bucketAggs || []).map((b) => parseInt(b.id, 10)),
    ];
    return String(Math.max(0, ...allIds) + 1);
  };

  // Metric CRUD
  const updateMetric = (idx: number, m: MetricAggregation) => {
    const newMetrics = [...(q.metrics || [])];
    newMetrics[idx] = m;
    updateAndRun({ metrics: newMetrics });
  };

  const addMetric = () => {
    const newMetrics = [...(q.metrics || []), { id: nextId(), type: 'count' as MetricAggregationType }];
    update({ metrics: newMetrics });
  };

  const removeMetric = (idx: number) => {
    const newMetrics = (q.metrics || []).filter((_, i) => i !== idx);
    if (newMetrics.length === 0) newMetrics.push({ id: nextId(), type: 'count' });
    updateAndRun({ metrics: newMetrics });
  };

  const toggleHideMetric = (idx: number) => {
    const newMetrics = [...(q.metrics || [])];
    newMetrics[idx] = { ...newMetrics[idx], hide: !newMetrics[idx].hide };
    updateAndRun({ metrics: newMetrics });
  };

  // Bucket CRUD
  const updateBucket = (idx: number, b: BucketAggregation) => {
    const newBuckets = [...(q.bucketAggs || [])];
    newBuckets[idx] = b;
    update({ bucketAggs: newBuckets });
  };

  const addBucket = () => {
    const newBuckets = [...(q.bucketAggs || []), { id: nextId(), type: 'terms' as BucketAggregationType, field: '' }];
    update({ bucketAggs: newBuckets });
  };

  const removeBucket = (idx: number) => {
    const newBuckets = (q.bucketAggs || []).filter((_, i) => i !== idx);
    if (newBuckets.length === 0) newBuckets.push({ id: nextId(), type: 'date_histogram' });
    updateAndRun({ bucketAggs: newBuckets });
  };

  // Legacy metrics helpers
  const needsMetricField = q.metricType && ![MetricAggType.Count, MetricAggType.Terms].includes(q.metricType as MetricAggType);
  const isTermsAgg = q.metricType === MetricAggType.Terms;

  return (
    <div style={{ width: '100%' }}>
      {/* ============ QUERY TYPE + INDEX ============ */}
      <InlineFieldRow>
        <InlineField label="Type" labelWidth={8}>
          <RadioButtonGroup
            options={queryTypeOptions}
            value={q.queryType || QueryType.Logs}
            onChange={(v) => {
              const patch: Partial<QuickwitQuery> = { queryType: v };
              if ((v === QueryType.Traces || v === QueryType.TraceId) && datasource.traceIndex) {
                patch.index = datasource.traceIndex;
              } else if (v === QueryType.Logs || v === QueryType.Metrics) {
                if (q.index === datasource.traceIndex) {
                  patch.index = datasource.logIndex || datasource.defaultIndex || '';
                }
              }
              if (v === QueryType.Metrics) {
                // Ensure new model is initialized
                if (!q.metrics || q.metrics.length === 0) {
                  patch.metrics = [...defaultMetrics];
                  patch.bucketAggs = [...defaultBucketAggs];
                }
              }
              updateAndRun(patch);
            }}
            size="sm"
          />
        </InlineField>

        <InlineField label="Index" labelWidth={8} grow>
          <Select
            options={indexOptions}
            value={q.index || activeIndex || ''}
            onChange={(v) => updateAndRun({ index: v.value || '' })}
            placeholder="Select or search index..."
            isClearable={true}
            isSearchable={true}
            allowCustomValue={true}
            onInputChange={(val) => {
              if (val) {
                datasource.searchIndexes(val).then((ids) => {
                  setIndexOptions(ids.map((id) => ({ label: id, value: id })));
                });
              }
            }}
          />
        </InlineField>
      </InlineFieldRow>

      {/* ============ LOG QUERY ============ */}
      {(q.queryType === QueryType.Logs || !q.queryType) && (
        <>
          <InlineFieldRow>
            <InlineField label="Query" labelWidth={8} grow>
              <QueryInput
                value={q.query || ''}
                placeholder="Lucene query (e.g. severity_text:ERROR AND service.name:api)"
                fields={fields}
                onChange={(val) => update({ query: val })}
                onRunQuery={onRunQuery}
                rows={2}
              />
            </InlineField>
          </InlineFieldRow>

          <QuickFilterBar
            query={q.query || ''}
            fields={fields}
            onChange={(newQuery) => update({ query: newQuery })}
            onRunQuery={onRunQuery}
          />

          <InlineFieldRow>
            <InlineField label="Limit" labelWidth={8}>
              <Select
                width={12}
                options={sizeOptions}
                value={q.size || 100}
                onChange={(v) => updateAndRun({ size: v.value || 100 })}
              />
            </InlineField>
          </InlineFieldRow>
        </>
      )}

      {/* ============ TRACE ID LOOKUP ============ */}
      {q.queryType === QueryType.TraceId && (
        <InlineFieldRow>
          <InlineField label="Trace ID" labelWidth={10} grow>
            <Input
              value={q.traceId || ''}
              placeholder="Enter a trace ID"
              onChange={(e: React.ChangeEvent<HTMLInputElement>) => update({ traceId: e.target.value })}
              onBlur={onRunQuery}
              onKeyDown={(e: React.KeyboardEvent) => { if (e.key === 'Enter') onRunQuery(); }}
            />
          </InlineField>
        </InlineFieldRow>
      )}

      {/* ============ TRACE SEARCH ============ */}
      {q.queryType === QueryType.Traces && (
        <>
          <InlineFieldRow>
            <InlineField label="Service" labelWidth={10}>
              <Select width={25} options={services} value={q.serviceName || ''}
                onChange={(v) => updateAndRun({ serviceName: v.value || '' })}
                placeholder="Select service..." isClearable isSearchable isLoading={servicesLoading} />
            </InlineField>
            <InlineField label="Operation" labelWidth={10}>
              <Select width={25} options={operations} value={q.operationName || ''}
                onChange={(v) => updateAndRun({ operationName: v.value || '' })}
                placeholder="Select operation..." isClearable disabled={!q.serviceName} />
            </InlineField>
          </InlineFieldRow>
          <InlineFieldRow>
            <InlineField label="Tags" labelWidth={8} grow>
              <Input value={q.query || ''} placeholder='e.g. http.status_code=500 error=true'
                onChange={(e: React.ChangeEvent<HTMLInputElement>) => update({ query: e.target.value })}
                onBlur={onRunQuery} />
            </InlineField>
          </InlineFieldRow>
          <InlineFieldRow>
            <InlineField label="Min Duration" labelWidth={12}>
              <Input width={12} value={q.minDuration || ''} placeholder="100ms"
                onChange={(e: React.ChangeEvent<HTMLInputElement>) => update({ minDuration: e.target.value })}
                onBlur={onRunQuery} />
            </InlineField>
            <InlineField label="Max Duration" labelWidth={12}>
              <Input width={12} value={q.maxDuration || ''} placeholder="5s"
                onChange={(e: React.ChangeEvent<HTMLInputElement>) => update({ maxDuration: e.target.value })}
                onBlur={onRunQuery} />
            </InlineField>
            <InlineField label="Limit" labelWidth={8}>
              <Select width={10} options={traceLimitOptions} value={q.traceLimit || 20}
                onChange={(v) => updateAndRun({ traceLimit: v.value || 20 })} />
            </InlineField>
            <Button variant="primary" size="sm" onClick={onRunQuery} style={{ marginLeft: 8, alignSelf: 'center' }}>
              Search
            </Button>
          </InlineFieldRow>
        </>
      )}

      {/* ============ METRICS (NEW MODEL) ============ */}
      {q.queryType === QueryType.Metrics && useNewModel && (
        <>
          {/* Lucene Query */}
          <InlineFieldRow>
            <InlineField label="Query" labelWidth={10} grow>
              <QueryInput
                value={q.query || ''}
                placeholder="Lucene filter (e.g. span_name:browser_manager.*)"
                fields={fields}
                onChange={(val) => update({ query: val })}
                onRunQuery={onRunQuery}
                rows={1}
              />
            </InlineField>
          </InlineFieldRow>

          {/* Metric Rows */}
          {(q.metrics || []).map((m, i) => (
            <MetricRow
              key={m.id}
              metric={m}
              index={i}
              fieldOptions={fieldOptions}
              onUpdate={(updated) => updateMetric(i, updated)}
              onRemove={() => removeMetric(i)}
              onToggleHide={() => toggleHideMetric(i)}
              canRemove={(q.metrics || []).length > 1}
            />
          ))}
          <InlineFieldRow>
            <InlineField label="" labelWidth={10}>
              <Button variant="secondary" size="sm" icon="plus" onClick={addMetric}>
                Add Metric
              </Button>
            </InlineField>
          </InlineFieldRow>

          {/* Bucket Agg Rows (Group By) */}
          {(q.bucketAggs || []).map((b, i) => (
            <BucketRow
              key={b.id}
              bucket={b}
              index={i}
              fieldOptions={fieldOptions}
              metrics={q.metrics || []}
              onUpdate={(updated) => updateBucket(i, updated)}
              onRemove={() => removeBucket(i)}
              canRemove={(q.bucketAggs || []).length > 1}
              onRunQuery={onRunQuery}
            />
          ))}
          <InlineFieldRow>
            <InlineField label="" labelWidth={10}>
              <Button variant="secondary" size="sm" icon="plus" onClick={addBucket}>
                Add Group By
              </Button>
            </InlineField>
          </InlineFieldRow>
        </>
      )}

      {/* ============ METRICS (LEGACY MODEL - backward compat) ============ */}
      {q.queryType === QueryType.Metrics && !useNewModel && (
        <>
          <InlineFieldRow>
            <InlineField label="Query" labelWidth={8} grow>
              <QueryInput
                value={q.query || ''}
                placeholder="Lucene filter (e.g. span_name:browser_manager.*)"
                fields={fields}
                onChange={(val) => update({ query: val })}
                onRunQuery={onRunQuery}
                rows={1}
              />
            </InlineField>
          </InlineFieldRow>

          <InlineFieldRow>
            <InlineField label="Agg Type" labelWidth={10}>
              <Select
                width={20}
                options={legacyMetricAggOptions}
                value={q.metricType || MetricAggType.Count}
                onChange={(v) => updateAndRun({ metricType: v.value || MetricAggType.Count })}
              />
            </InlineField>
            {needsMetricField && (
              <InlineField label="Field" labelWidth={6}>
                <Select
                  width={25}
                  options={fieldOptions}
                  value={q.metricField || ''}
                  onChange={(v) => updateAndRun({ metricField: v.value || '' })}
                  placeholder="Select field..."
                  isSearchable
                  allowCustomValue
                />
              </InlineField>
            )}
            {isTermsAgg && (
              <InlineField label="Field" labelWidth={6}>
                <Select
                  width={25}
                  options={fieldOptions}
                  value={q.termsField || ''}
                  onChange={(v) => updateAndRun({ termsField: v.value || '' })}
                  placeholder="Select field..."
                  isSearchable
                  allowCustomValue
                />
              </InlineField>
            )}
          </InlineFieldRow>

          {!isTermsAgg && (
            <InlineFieldRow>
              <InlineField label="Group By" labelWidth={10}>
                <Select
                  width={25}
                  options={[{ label: '-- None --', value: '' }, ...fieldOptions]}
                  value={q.groupBy || ''}
                  onChange={(v) => updateAndRun({ groupBy: v.value || '' })}
                  placeholder="None"
                  isSearchable
                  isClearable
                  allowCustomValue
                />
              </InlineField>
              {q.groupBy && (
                <InlineField label="Display" labelWidth={8}>
                  <Select
                    width={16}
                    options={displayModeOptions}
                    value={q.metricDisplayMode || MetricDisplayMode.TimeSeries}
                    onChange={(v) => updateAndRun({ metricDisplayMode: (v.value as MetricDisplayMode) || MetricDisplayMode.TimeSeries })}
                  />
                </InlineField>
              )}
              {q.groupBy && (
                <InlineField label="Top" labelWidth={4}>
                  <Input
                    width={8}
                    type="number"
                    value={q.termsSize || 10}
                    onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                      update({ termsSize: parseInt(e.target.value, 10) || 10 })
                    }
                    onBlur={onRunQuery}
                  />
                </InlineField>
              )}
            </InlineFieldRow>
          )}

          {(isTermsAgg || (q.groupBy && q.metricDisplayMode === MetricDisplayMode.Table)) && (
            <InlineFieldRow>
              <InlineField label="Sort" labelWidth={8}>
                <Select
                  width={12}
                  options={sortOrderOptions}
                  value={q.metricSortOrder || 'desc'}
                  onChange={(v) => updateAndRun({ metricSortOrder: (v.value as 'asc' | 'desc') || 'desc' })}
                />
              </InlineField>
              {isTermsAgg && (
                <InlineField label="Top" labelWidth={4}>
                  <Input
                    width={8}
                    type="number"
                    value={q.termsSize || 10}
                    onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                      update({ termsSize: parseInt(e.target.value, 10) || 10 })
                    }
                    onBlur={onRunQuery}
                  />
                </InlineField>
              )}
            </InlineFieldRow>
          )}

          {!isTermsAgg && !(q.groupBy && q.metricDisplayMode === MetricDisplayMode.Table) && (
            <InlineFieldRow>
              <InlineField label="Interval" labelWidth={10}>
                <Input
                  width={12}
                  value={q.groupByInterval || ''}
                  placeholder="auto"
                  onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                    update({ groupByInterval: e.target.value })
                  }
                  onBlur={onRunQuery}
                />
              </InlineField>
            </InlineFieldRow>
          )}

          {/* Button to upgrade to new model */}
          <InlineFieldRow>
            <InlineField label="" labelWidth={10}>
              <Button variant="secondary" size="sm" icon="arrow-up" onClick={ensureMetricsModel}>
                Switch to Advanced Mode
              </Button>
            </InlineField>
          </InlineFieldRow>
        </>
      )}
    </div>
  );
}
