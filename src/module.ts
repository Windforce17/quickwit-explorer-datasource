import { DataSourcePlugin } from '@grafana/data';
import { QuickwitExplorerDatasource } from './datasource';
import { ConfigEditor } from './components/ConfigEditor';
import { QueryEditor } from './components/QueryEditor';
import { QuickwitQuery, QuickwitOptions } from './types';

export const plugin = new DataSourcePlugin<QuickwitExplorerDatasource, QuickwitQuery, QuickwitOptions>(
  QuickwitExplorerDatasource
)
  .setConfigEditor(ConfigEditor)
  .setQueryEditor(QueryEditor);
