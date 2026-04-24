import React, { useState, useEffect, useCallback } from 'react';
import { QueryEditorProps, SelectableValue } from '@grafana/data';
import {
  InlineField,
  Input,
  Select,
  AsyncSelect,
  InlineFieldRow,
  RadioButtonGroup,
  TextArea,
  Button,
  HorizontalGroup,
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

export function QueryEditor({ query, onChange, onRunQuery, datasource }: Props) {
  const q = { ...defaultQuery, ...query } as QuickwitQuery;

  const [services, setServices] = useState<Array<SelectableValue<string>>>([]);
  const [operations, setOperations] = useState<Array<SelectableValue<string>>>([]);

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
              <TextArea
                value={q.query || ''}
                placeholder="Lucene query (e.g. severity_text:ERROR AND service.name:my-service)"
                onChange={(e: React.ChangeEvent<HTMLTextAreaElement>) =>
                  update({ query: e.target.value })
                }
                onBlur={onRunQuery}
                onKeyDown={(e: React.KeyboardEvent) => {
                  if (e.key === 'Enter' && (e.shiftKey || e.ctrlKey)) {
                    e.preventDefault();
                    onRunQuery();
                  }
                }}
                rows={2}
              />
            </InlineField>
          </InlineFieldRow>
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
              <TextArea
                value={q.query || ''}
                placeholder="Lucene query (e.g. * for all documents)"
                onChange={(e: React.ChangeEvent<HTMLTextAreaElement>) =>
                  update({ query: e.target.value })
                }
                onBlur={onRunQuery}
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
