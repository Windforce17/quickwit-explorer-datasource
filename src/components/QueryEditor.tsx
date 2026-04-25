import React, { useState, useEffect, useCallback, useRef, useMemo } from 'react';
import { QueryEditorProps, SelectableValue } from '@grafana/data';
import {
  InlineField,
  Input,
  Select,
  InlineFieldRow,
  RadioButtonGroup,
  Button,
  useTheme2,
} from '@grafana/ui';
import { QuickwitExplorerDatasource } from '../datasource';
import { QuickwitQuery, QuickwitOptions, QueryType, MetricAggType, defaultQuery } from '../types';

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

const metricAggOptions: Array<SelectableValue<string>> = [
  { label: 'Count', value: MetricAggType.Count, description: 'Document count over time' },
  { label: 'Average', value: MetricAggType.Avg, description: 'Average of a numeric field' },
  { label: 'Sum', value: MetricAggType.Sum, description: 'Sum of a numeric field' },
  { label: 'Min', value: MetricAggType.Min, description: 'Minimum value of a field' },
  { label: 'Max', value: MetricAggType.Max, description: 'Maximum value of a field' },
  { label: 'Percentiles', value: MetricAggType.Percentiles, description: 'p50/p90/p95/p99' },
  { label: 'Top Values', value: MetricAggType.Terms, description: 'Top N values of a field' },
];

const sortOrderOptions: Array<SelectableValue<string>> = [
  { label: 'Desc', value: 'desc' },
  { label: 'Asc', value: 'asc' },
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

  const handleChange = (e: React.ChangeEvent<HTMLTextAreaElement>) => {
    const newVal = e.target.value;
    const pos = e.target.selectionStart || 0;
    onChange(newVal);
    setCursorPos(pos);
    updateSuggestions(newVal, pos);
  };

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

  // Show popular fields as quick-add buttons
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
              // Auto-switch index when changing query type
              if ((v === QueryType.Traces || v === QueryType.TraceId) && datasource.traceIndex) {
                patch.index = datasource.traceIndex;
              } else if (v === QueryType.Logs || v === QueryType.Metrics) {
                // Switch back to log/default index
                if (q.index === datasource.traceIndex) {
                  patch.index = datasource.logIndex || datasource.defaultIndex || '';
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
          <InlineField label="Trace ID" labelWidth={8} grow>
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
            <InlineField label="Service" labelWidth={8}>
              <Select width={25} options={services} value={q.serviceName || ''}
                onChange={(v) => updateAndRun({ serviceName: v.value || '' })}
                placeholder="Select service..." isClearable isSearchable isLoading={servicesLoading} />
            </InlineField>
            <InlineField label="Operation" labelWidth={8}>
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
            <InlineField label="Min Dur" labelWidth={8}>
              <Input width={12} value={q.minDuration || ''} placeholder="100ms"
                onChange={(e: React.ChangeEvent<HTMLInputElement>) => update({ minDuration: e.target.value })}
                onBlur={onRunQuery} />
            </InlineField>
            <InlineField label="Max Dur" labelWidth={8}>
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

      {/* ============ METRICS ============ */}
      {q.queryType === QueryType.Metrics && (
        <>
          <InlineFieldRow>
            <InlineField label="Query" labelWidth={8} grow>
              <QueryInput
                value={q.query || ''}
                placeholder="Lucene filter (e.g. * for all)"
                fields={fields}
                onChange={(val) => update({ query: val })}
                onRunQuery={onRunQuery}
                rows={1}
              />
            </InlineField>
          </InlineFieldRow>

          <InlineFieldRow>
            <InlineField label="Agg Type" labelWidth={8}>
              <Select
                width={20}
                options={metricAggOptions}
                value={q.metricType || MetricAggType.Count}
                onChange={(v) => updateAndRun({ metricType: v.value || MetricAggType.Count })}
              />
            </InlineField>

            {/* Field selector for avg/sum/min/max/percentiles */}
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

            {/* Terms field selector */}
            {isTermsAgg && (
              <>
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
              </>
            )}
          </InlineFieldRow>

          {/* Group By field for Count - allows counting by a specific field */}
          {q.metricType === MetricAggType.Count && (
            <InlineFieldRow>
              <InlineField label="Group By" labelWidth={10} tooltip="Optional: group count by a field (uses terms aggregation)">
                <Select
                  width={25}
                  options={[{ label: '-- None (time histogram) --', value: '' }, ...fieldOptions]}
                  value={q.groupBy || ''}
                  onChange={(v) => updateAndRun({ groupBy: v.value || '' })}
                  placeholder="None (time histogram)"
                  isSearchable
                  isClearable
                  allowCustomValue
                />
              </InlineField>
            </InlineFieldRow>
          )}

          {/* Interval for time-based aggregations */}
          {!isTermsAgg && (
            <InlineFieldRow>
              <InlineField label="Interval" labelWidth={8}>
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

          {/* Sort order for terms */}
          {isTermsAgg && (
            <InlineFieldRow>
              <InlineField label="Sort" labelWidth={8}>
                <Select
                  width={12}
                  options={sortOrderOptions}
                  value={q.metricSortOrder || 'desc'}
                  onChange={(v) => updateAndRun({ metricSortOrder: (v.value as 'asc' | 'desc') || 'desc' })}
                />
              </InlineField>
            </InlineFieldRow>
          )}
        </>
      )}
    </div>
  );
}
