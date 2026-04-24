import React, { useCallback, useState } from 'react';
import { DataSourcePluginOptionsEditorProps, SelectableValue } from '@grafana/data';
import { InlineField, Input, FieldSet, AsyncSelect, Alert } from '@grafana/ui';
import { getBackendSrv } from '@grafana/runtime';
import { QuickwitOptions, QuickwitSecureJsonData, QwIndex } from '../types';

type Props = DataSourcePluginOptionsEditorProps<QuickwitOptions, QuickwitSecureJsonData>;

export function ConfigEditor(props: Props) {
  const { options, onOptionsChange } = props;
  const { jsonData } = options;
  const [indexLoadError, setIndexLoadError] = useState<string>('');

  const onJsonDataChange = useCallback(
    (key: keyof QuickwitOptions, value: any) => {
      onOptionsChange({
        ...options,
        jsonData: { ...jsonData, [key]: value },
      });
    },
    [options, jsonData, onOptionsChange]
  );

  /**
   * Load indexes from Quickwit via Grafana proxy.
   * After saving the datasource, the proxy route becomes available.
   * Before saving, we try a direct fetch as fallback.
   */
  const loadIndexes = useCallback(
    async (inputValue: string): Promise<Array<SelectableValue<string>>> => {
      setIndexLoadError('');

      // Try via Grafana proxy first (works after datasource is saved)
      const proxyUrl = options.url ? `${options.url}/qw/api/v1/indexes` : '';
      const directUrl = jsonData.quickwitUrl
        ? `${jsonData.quickwitUrl.replace(/\/$/, '')}/api/v1/indexes`
        : '';

      let indexes: QwIndex[] = [];

      if (proxyUrl) {
        try {
          const resp = await getBackendSrv().datasourceRequest({
            url: proxyUrl,
            method: 'GET',
          });
          indexes = resp.data || [];
        } catch (e: any) {
          // Proxy not available yet, try direct
          if (directUrl) {
            try {
              const resp = await getBackendSrv().datasourceRequest({
                url: directUrl,
                method: 'GET',
              });
              indexes = resp.data || [];
            } catch (e2: any) {
              setIndexLoadError('Cannot load indexes. Save the datasource first, then reload.');
            }
          } else {
            setIndexLoadError('Enter the Quickwit URL and save first to browse indexes.');
          }
        }
      }

      const pattern = inputValue.toLowerCase();
      return indexes
        .map((idx) => idx.index_config.index_id)
        .filter((id) => !pattern || id.toLowerCase().includes(pattern))
        .sort()
        .map((id) => ({ label: id, value: id }));
    },
    [options.url, jsonData.quickwitUrl]
  );

  return (
    <>
      <FieldSet label="Connection">
        <InlineField
          label="Quickwit URL"
          labelWidth={20}
          tooltip="Base URL of your Quickwit instance (e.g. http://qw-quickwit-searcher.quickwit:7280). This URL is used by Grafana server to proxy requests."
        >
          <Input
            width={60}
            value={jsonData.quickwitUrl || ''}
            placeholder="http://qw-quickwit-searcher.quickwit:7280"
            onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
              onJsonDataChange('quickwitUrl', e.target.value)
            }
          />
        </InlineField>
      </FieldSet>

      <FieldSet label="Index Configuration">
        {indexLoadError && (
          <Alert title="Index Loading" severity="info">
            {indexLoadError}
          </Alert>
        )}

        <InlineField
          label="Default Index"
          labelWidth={20}
          tooltip="Default index for queries. Type to search available indexes."
        >
          <AsyncSelect
            width={40}
            defaultOptions={true}
            loadOptions={loadIndexes}
            value={
              jsonData.defaultIndex
                ? { label: jsonData.defaultIndex, value: jsonData.defaultIndex }
                : null
            }
            onChange={(v) => onJsonDataChange('defaultIndex', v?.value || '')}
            placeholder="Search indexes..."
            allowCustomValue={true}
            isClearable={true}
          />
        </InlineField>

        <InlineField
          label="Log Index"
          labelWidth={20}
          tooltip="Index containing log data (used for log queries and trace-to-log correlation)"
        >
          <AsyncSelect
            width={40}
            defaultOptions={true}
            loadOptions={loadIndexes}
            value={
              jsonData.logIndex
                ? { label: jsonData.logIndex, value: jsonData.logIndex }
                : null
            }
            onChange={(v) => onJsonDataChange('logIndex', v?.value || '')}
            placeholder="e.g. otel-logs-v0_7"
            allowCustomValue={true}
            isClearable={true}
          />
        </InlineField>

        <InlineField
          label="Trace Index"
          labelWidth={20}
          tooltip="Index containing trace/span data (used for trace queries and log-to-trace correlation)"
        >
          <AsyncSelect
            width={40}
            defaultOptions={true}
            loadOptions={loadIndexes}
            value={
              jsonData.traceIndex
                ? { label: jsonData.traceIndex, value: jsonData.traceIndex }
                : null
            }
            onChange={(v) => onJsonDataChange('traceIndex', v?.value || '')}
            placeholder="e.g. otel-traces-v0_7"
            allowCustomValue={true}
            isClearable={true}
          />
        </InlineField>
      </FieldSet>

      <FieldSet label="Log Field Mapping">
        <InlineField
          label="Message Field"
          labelWidth={20}
          tooltip="Field name containing the log message body"
        >
          <Input
            width={40}
            value={jsonData.logMessageField || ''}
            placeholder="body"
            onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
              onJsonDataChange('logMessageField', e.target.value)
            }
          />
        </InlineField>

        <InlineField
          label="Level Field"
          labelWidth={20}
          tooltip="Field name containing the log severity level"
        >
          <Input
            width={40}
            value={jsonData.logLevelField || ''}
            placeholder="severity_text"
            onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
              onJsonDataChange('logLevelField', e.target.value)
            }
          />
        </InlineField>
      </FieldSet>
    </>
  );
}
