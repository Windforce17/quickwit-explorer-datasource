import React, { useState, useEffect, useCallback, useRef, useMemo } from 'react';
import { QueryEditorProps, SelectableValue } from '@grafana/data';
import {
  InlineField,
  Input,
  Select,
  AsyncSelect,
  InlineFieldRow,
  RadioButtonGroup,
  Button,
  Tooltip,
  Icon,
  useTheme2,
} from '@grafana/ui';
import { QuickwitExplorerDatasource } from '../datasource';
import { QuickwitQuery, QuickwitOptions, QueryType, defaultQuery } from '../types';

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

// Lucene operators for autocomplete
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
  const inputRef = useRef<HTMLTextAreaElement | HTMLInputElement>(null);
  const suggestionsRef = useRef<HTMLDivElement>(null);

  // Build all completions: fields + operators
  const allCompletions = useMemo(() => {
    const fieldWithColon = fields.map((f) => `${f}:`);
    return [...fieldWithColon, ...LUCENE_OPERATORS];
  }, [fields]);

  const getCurrentToken = useCallback(
    (text: string, pos: number): { token: string; start: number; end: number } => {
      // Find the token around cursor position
      const before = text.substring(0, pos);
      const after = text.substring(pos);

      // Find start of current token (delimiters: space, (, ), [, ])
      const startMatch = before.match(/[^\s()\[\]]*$/);
      const tokenBefore = startMatch ? startMatch[0] : '';
      const tokenStart = pos - tokenBefore.length;

      // Find end of current token
      const endMatch = after.match(/^[^\s()\[\]]*/);
      const tokenAfter = endMatch ? endMatch[0] : '';

      return {
        token: tokenBefore + tokenAfter,
        start: tokenStart,
        end: pos + tokenAfter.length,
      };
    },
    []
  );

  const updateSuggestions = useCallback(
    (text: string, pos: number) => {
      if (!fields.length) {
        setSuggestions([]);
        return;
      }

      const { token } = getCurrentToken(text, pos);
      if (!token || token.length < 1) {
        setSuggestions([]);
        setShowSuggestions(false);
        return;
      }

      const lower = token.toLowerCase();
      const matches = allCompletions.filter((c) => c.toLowerCase().startsWith(lower) && c.toLowerCase() !== lower);

      if (matches.length > 0 && matches.length <= 20) {
        setSuggestions(matches);
        setSelectedIdx(0);
        setShowSuggestions(true);
      } else {
        setSuggestions([]);
        setShowSuggestions(false);
      }
    },
    [fields, allCompletions, getCurrentToken]
  );

  const applySuggestion = useCallback(
    (suggestion: string) => {
      const { start, end } = getCurrentToken(value, cursorPos);
      const newValue = value.substring(0, start) + suggestion + value.substring(end);
      onChange(newValue);
      setShowSuggestions(false);

      // Move cursor to end of inserted text
      const newPos = start + suggestion.length;
      setTimeout(() => {
        if (inputRef.current) {
          (inputRef.current as HTMLTextAreaElement).selectionStart = newPos;
          (inputRef.current as HTMLTextAreaElement).selectionEnd = newPos;
        }
      }, 0);
    },
    [value, cursorPos, getCurrentToken, onChange]
  );

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (showSuggestions && suggestions.length > 0) {
      if (e.key === 'ArrowDown') {
        e.preventDefault();
        setSelectedIdx((prev) => Math.min(prev + 1, suggestions.length - 1));
        return;
      }
      if (e.key === 'ArrowUp') {
        e.preventDefault();
        setSelectedIdx((prev) => Math.max(prev - 1, 0));
        return;
      }
      if (e.key === 'Tab' || e.key === 'Enter') {
        if (suggestions[selectedIdx]) {
          e.preventDefault();
          applySuggestion(suggestions[selectedIdx]);
          return;
        }
      }
      if (e.key === 'Escape') {
        e.preventDefault();
        setShowSuggestions(false);
        return;
      }
    }

    // Ctrl/Shift+Enter to run query
    if (e.key === 'Enter' && (e.shiftKey || e.ctrlKey)) {
      e.preventDefault();
      setShowSuggestions(false);
      onRunQuery();
    }
  };

  const handleChange = (e: React.ChangeEvent<HTMLTextAreaElement | HTMLInputElement>) => {
    const newVal = e.target.value;
    const pos = e.target.selectionStart || 0;
    onChange(newVal);
    setCursorPos(pos);
    updateSuggestions(newVal, pos);
  };

  const handleBlur = () => {
    // Delay to allow click on suggestion
    setTimeout(() => setShowSuggestions(false), 200);
    onRunQuery();
  };

  const handleClick = (e: React.MouseEvent<HTMLTextAreaElement | HTMLInputElement>) => {
    const pos = (e.target as HTMLTextAreaElement).selectionStart || 0;
    setCursorPos(pos);
  };

  const containerStyle: React.CSSProperties = { position: 'relative', width: '100%' };

  const suggestionsStyle: React.CSSProperties = {
    position: 'absolute',
    top: '100%',
    left: 0,
    right: 0,
    zIndex: 1000,
    maxHeight: 200,
    overflowY: 'auto',
    background: theme.colors.background.primary,
    border: `1px solid ${theme.colors.border.medium}`,
    borderRadius: 4,
    boxShadow: theme.shadows.z2,
  };

  const suggestionItemStyle = (isSelected: boolean): React.CSSProperties => ({
    padding: '4px 8px',
    cursor: 'pointer',
    fontSize: 12,
    fontFamily: 'monospace',
    background: isSelected ? theme.colors.action.hover : 'transparent',
  });

  return (
    <div style={containerStyle}>
      <textarea
        ref={inputRef as React.RefObject<HTMLTextAreaElement>}
        value={value}
        placeholder={placeholder}
        onChange={handleChange}
        onKeyDown={handleKeyDown}
        onBlur={handleBlur}
        onClick={handleClick}
        rows={rows || 2}
        style={{
          width: '100%',
          fontFamily: 'monospace',
          fontSize: 12,
          padding: '6px 8px',
          border: `1px solid ${theme.colors.border.medium}`,
          borderRadius: 4,
          background: theme.colors.background.canvas,
          color: theme.colors.text.primary,
          resize: 'vertical',
          lineHeight: 1.5,
        }}
      />
      {showSuggestions && suggestions.length > 0 && (
        <div ref={suggestionsRef} style={suggestionsStyle}>
          {suggestions.map((s, i) => (
            <div
              key={s}
              style={suggestionItemStyle(i === selectedIdx)}
              onMouseDown={(e) => {
                e.preventDefault();
                applySuggestion(s);
              }}
              onMouseEnter={() => setSelectedIdx(i)}
            >
              {s.endsWith(':') ? (
                <>
                  <span style={{ color: theme.colors.text.link }}>{s.slice(0, -1)}</span>
                  <span style={{ color: theme.colors.text.secondary }}>:</span>
                </>
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
  // Parse existing filters from query
  const filters = useMemo(() => {
    if (!query || query === '*') return [];
    // Simple parse: split by AND, trim
    return query
      .split(/\s+AND\s+/i)
      .map((f) => f.trim())
      .filter((f) => f && f !== '*');
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
    const newQuery = current ? `${current} AND ${newFilter}` : newFilter;
    onChange(newQuery);
  };

  if (filters.length === 0 && fields.length === 0) return null;

  const chipStyle: React.CSSProperties = {
    display: 'inline-flex',
    alignItems: 'center',
    padding: '2px 8px',
    margin: '2px 4px 2px 0',
    borderRadius: 12,
    fontSize: 11,
    fontFamily: 'monospace',
    background: theme.colors.background.secondary,
    border: `1px solid ${theme.colors.border.weak}`,
    cursor: 'default',
  };

  const removeStyle: React.CSSProperties = {
    marginLeft: 4,
    cursor: 'pointer',
    color: theme.colors.text.secondary,
    fontWeight: 'bold',
    fontSize: 13,
    lineHeight: 1,
  };

  const addBtnStyle: React.CSSProperties = {
    display: 'inline-flex',
    alignItems: 'center',
    padding: '2px 6px',
    margin: '2px 4px 2px 0',
    borderRadius: 12,
    fontSize: 11,
    background: 'transparent',
    border: `1px dashed ${theme.colors.border.medium}`,
    cursor: 'pointer',
    color: theme.colors.text.secondary,
  };

  // Show top 8 fields as quick-add buttons
  const quickFields = fields.slice(0, 8);

  return (
    <div style={{ padding: '4px 0', display: 'flex', flexWrap: 'wrap', alignItems: 'center', gap: 2 }}>
      {/* Active filters as removable chips */}
      {filters.map((f, i) => (
        <span key={i} style={chipStyle}>
          {f}
          <span style={removeStyle} onClick={() => removeFilter(i)} title="Remove filter">
            &times;
          </span>
        </span>
      ))}

      {/* Quick-add field buttons */}
      {quickFields.length > 0 && (
        <>
          {filters.length > 0 && (
            <span style={{ color: theme.colors.text.disabled, fontSize: 11, margin: '0 4px' }}>|</span>
          )}
          <Tooltip content="Click a field to add it as a filter" placement="top">
            <span style={{ fontSize: 11, color: theme.colors.text.secondary, marginRight: 4 }}>
              <Icon name="plus-circle" size="sm" /> Add:
            </span>
          </Tooltip>
          {quickFields.map((f) => (
            <span key={f} style={addBtnStyle} onClick={() => addFilter(f)} title={`Add ${f} filter`}>
              {f}
            </span>
          ))}
        </>
      )}
    </div>
  );
}

// ================================================================
//  MAIN QUERY EDITOR
// ================================================================

export function QueryEditor({ query, onChange, onRunQuery, datasource }: Props) {
  const q = { ...defaultQuery, ...query } as QuickwitQuery;

  const [services, setServices] = useState<Array<SelectableValue<string>>>([]);
  const [operations, setOperations] = useState<Array<SelectableValue<string>>>([]);
  const [fields, setFields] = useState<string[]>([]);

  // Load fields for the current index (used for autocomplete and quick filters)
  useEffect(() => {
    const index = q.index || (q.queryType === QueryType.Traces ? datasource.traceIndex : datasource.logIndex) || datasource.defaultIndex;
    if (index) {
      datasource.getFields(index).then(setFields).catch(() => setFields([]));
    } else {
      setFields([]);
    }
  }, [q.index, q.queryType, datasource]);

  // Load services when in trace mode
  useEffect(() => {
    if (q.queryType === QueryType.Traces) {
      const idx = q.index || datasource.traceIndex;
      datasource.getServices(idx).then((svcs) => {
        setServices([
          { label: '-- All Services --', value: '' },
          ...svcs.map((s) => ({ label: s, value: s })),
        ]);
      });
    }
  }, [q.queryType, q.index, datasource]);

  // Load operations when service changes
  useEffect(() => {
    if (q.queryType === QueryType.Traces && q.serviceName) {
      const idx = q.index || datasource.traceIndex;
      datasource.getOperations(q.serviceName, idx).then((ops) => {
        setOperations([
          { label: '-- All Operations --', value: '' },
          ...ops.map((o) => ({ label: o, value: o })),
        ]);
      });
    } else {
      setOperations([]);
    }
  }, [q.serviceName, q.queryType, q.index, datasource]);

  const loadIndexes = useCallback(
    async (inputValue: string): Promise<Array<SelectableValue<string>>> => {
      const indexes = await datasource.searchIndexes(inputValue);
      return indexes.map((id) => ({ label: id, value: id }));
    },
    [datasource]
  );

  const update = (patch: Partial<QuickwitQuery>) => {
    onChange({ ...q, ...patch });
  };

  const updateAndRun = (patch: Partial<QuickwitQuery>) => {
    onChange({ ...q, ...patch });
    onRunQuery();
  };

  return (
    <div style={{ width: '100%' }}>
      {/* Row 1: Query Type + Index */}
      <InlineFieldRow>
        <InlineField label="Type" labelWidth={10}>
          <RadioButtonGroup
            options={queryTypeOptions}
            value={q.queryType || QueryType.Logs}
            onChange={(val) => update({ queryType: val })}
            size="md"
          />
        </InlineField>

        <InlineField label="Index" labelWidth={8} grow>
          <AsyncSelect
            width={30}
            defaultOptions={true}
            loadOptions={loadIndexes}
            value={q.index ? { label: q.index, value: q.index } : null}
            onChange={(v) => updateAndRun({ index: v?.value || '' })}
            placeholder="Search indexes..."
            allowCustomValue={true}
            isClearable={true}
          />
        </InlineField>
      </InlineFieldRow>

      {/* ============ LOGS ============ */}
      {q.queryType === QueryType.Logs && (
        <>
          <InlineFieldRow>
            <InlineField label="Query" labelWidth={10} grow>
              <QueryInput
                value={q.query || ''}
                placeholder="Lucene query (e.g. severity_text:ERROR AND service.name:my-service)"
                fields={fields}
                onChange={(val) => update({ query: val })}
                onRunQuery={onRunQuery}
                rows={2}
              />
            </InlineField>
          </InlineFieldRow>

          {/* Quick filter bar */}
          <QuickFilterBar
            query={q.query || ''}
            fields={fields}
            onChange={(newQuery) => update({ query: newQuery })}
            onRunQuery={onRunQuery}
          />

          <InlineFieldRow>
            <InlineField label="Limit" labelWidth={10}>
              <Select
                width={15}
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
              placeholder="Enter a trace ID to view its spans"
              onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                update({ traceId: e.target.value })
              }
              onBlur={onRunQuery}
              onKeyDown={(e: React.KeyboardEvent) => {
                if (e.key === 'Enter') {
                  onRunQuery();
                }
              }}
            />
          </InlineField>
        </InlineFieldRow>
      )}

      {/* ============ TRACE SEARCH ============ */}
      {q.queryType === QueryType.Traces && (
        <>
          <InlineFieldRow>
            <InlineField label="Service" labelWidth={10}>
              <Select
                width={25}
                options={services}
                value={q.serviceName || ''}
                onChange={(v) => updateAndRun({ serviceName: v.value || '' })}
                placeholder="Select service..."
                isClearable={true}
                isLoading={services.length <= 1}
              />
            </InlineField>

            <InlineField label="Operation" labelWidth={10}>
              <Select
                width={25}
                options={operations}
                value={q.operationName || ''}
                onChange={(v) => updateAndRun({ operationName: v.value || '' })}
                placeholder="Select operation..."
                isClearable={true}
                disabled={!q.serviceName}
              />
            </InlineField>
          </InlineFieldRow>

          <InlineFieldRow>
            <InlineField label="Tags" labelWidth={10} grow>
              <Input
                value={q.query || ''}
                placeholder='Key=value tag filters (e.g. http.status_code=500 error=true)'
                onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                  update({ query: e.target.value })
                }
                onBlur={onRunQuery}
              />
            </InlineField>
          </InlineFieldRow>

          <InlineFieldRow>
            <InlineField label="Min Duration" labelWidth={10}>
              <Input
                width={15}
                value={q.minDuration || ''}
                placeholder="e.g. 100ms"
                onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                  update({ minDuration: e.target.value })
                }
                onBlur={onRunQuery}
              />
            </InlineField>

            <InlineField label="Max Duration" labelWidth={10}>
              <Input
                width={15}
                value={q.maxDuration || ''}
                placeholder="e.g. 5s"
                onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                  update({ maxDuration: e.target.value })
                }
                onBlur={onRunQuery}
              />
            </InlineField>

            <InlineField label="Limit" labelWidth={8}>
              <Select
                width={12}
                options={traceLimitOptions}
                value={q.traceLimit || 20}
                onChange={(v) => updateAndRun({ traceLimit: v.value || 20 })}
              />
            </InlineField>

            <Button variant="primary" size="sm" onClick={onRunQuery} style={{ marginLeft: 8, alignSelf: 'center' }}>
              Search Traces
            </Button>
          </InlineFieldRow>
        </>
      )}

      {/* ============ METRICS ============ */}
      {q.queryType === QueryType.Metrics && (
        <>
          <InlineFieldRow>
            <InlineField label="Query" labelWidth={10} grow>
              <QueryInput
                value={q.query || ''}
                placeholder="Lucene query (e.g. * for all documents)"
                fields={fields}
                onChange={(val) => update({ query: val })}
                onRunQuery={onRunQuery}
                rows={2}
              />
            </InlineField>
          </InlineFieldRow>

          <InlineFieldRow>
            <InlineField label="Group By" labelWidth={10}>
              <Input
                width={20}
                value={q.groupBy || ''}
                placeholder="_timestamp"
                onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                  update({ groupBy: e.target.value })
                }
                onBlur={onRunQuery}
              />
            </InlineField>

            <InlineField label="Interval" labelWidth={10}>
              <Input
                width={15}
                value={q.groupByInterval || ''}
                placeholder="30s"
                onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                  update({ groupByInterval: e.target.value })
                }
                onBlur={onRunQuery}
              />
            </InlineField>
          </InlineFieldRow>
        </>
      )}
    </div>
  );
}
