# Quickwit Explorer - Grafana Data Source Plugin

A production-grade Grafana data source plugin for [Quickwit](https://quickwit.io/) with **automatic index discovery**, **built-in trace view**, and **log-trace correlation**.

## Features

| Feature | Description |
|---|---|
| **Index Auto-Discovery** | Automatically fetches all available indexes from Quickwit and provides a searchable dropdown in both the config editor and query editor |
| **Log Query** | Full Lucene query support with configurable result limits (10–1000), automatic timestamp detection, and log severity mapping |
| **Built-in Trace View** | Native Grafana trace visualization (waterfall/span view) via Quickwit's Jaeger-compatible API — no separate Jaeger data source needed |
| **Trace Search** | Search traces by service name, operation, tags, and duration filters with a results table |
| **Log → Trace Correlation** | Clicking a `trace_id` in log results opens the corresponding trace detail view directly |
| **Trace → Log Correlation** | Navigate from trace spans back to related log entries |
| **Metric Aggregation** | Date histogram aggregation queries for time-series visualization |
| **Grafana 11+ / 12 Compatible** | Built with the latest Grafana Plugin SDK, tested on Grafana 11.x and 12.x |

## Requirements

- **Grafana** >= 11.0.0 (tested up to 12.x)
- **Quickwit** >= 0.7.0 (for Jaeger-compatible API support)
- Quickwit must have trace data indexed in an OTEL-compatible index (e.g. `otel-traces-v0_7`)

## Installation

### Method 1: Environment Variable (Recommended for Docker/K8s)

Set the `GF_INSTALL_PLUGINS` environment variable:

```bash
GF_INSTALL_PLUGINS="https://your-host/quickwit-explorer-datasource-1.0.0.zip;quickwit-explorer-datasource"
```

### Method 2: Manual Installation

1. Download `quickwit-explorer-datasource-1.0.0.zip`
2. Extract to your Grafana plugins directory:

```bash
mkdir -p /var/lib/grafana/plugins/quickwit-explorer-datasource
unzip quickwit-explorer-datasource-1.0.0.zip -d /var/lib/grafana/plugins/quickwit-explorer-datasource
```

3. Allow unsigned plugins in `grafana.ini`:

```ini
[plugins]
allow_loading_unsigned_plugins = quickwit-explorer-datasource
```

4. Restart Grafana.

### Method 3: Helm Chart (Kubernetes)

```yaml
# values.yaml
grafana:
  env:
    GF_PLUGINS_ALLOW_LOADING_UNSIGNED_PLUGINS: "quickwit-explorer-datasource"
  
  extraInitContainers:
    - name: install-quickwit-explorer
      image: busybox
      command: ['sh', '-c', 'wget -O /tmp/plugin.zip https://your-host/quickwit-explorer-datasource-1.0.0.zip && unzip /tmp/plugin.zip -d /var/lib/grafana/plugins/quickwit-explorer-datasource']
      volumeMounts:
        - name: grafana-plugins
          mountPath: /var/lib/grafana/plugins
```

## Configuration

### Step 1: Add Data Source

In Grafana, go to **Configuration → Data Sources → Add data source** and search for **Quickwit Explorer**.

### Step 2: Configure Connection

| Setting | Value | Description |
|---|---|---|
| **Quickwit URL** | `http://qw-quickwit-searcher.quickwit:7280` | Base URL of your Quickwit searcher instance |

### Step 3: Configure Indexes

All index fields support **auto-complete search**. Start typing to filter available indexes.

| Setting | Example | Description |
|---|---|---|
| **Default Index** | `otel-logs-v0_7` | Fallback index when no index is specified in a query |
| **Log Index** | `otel-logs-v0_7` | Index for log queries and trace-to-log correlation |
| **Trace Index** | `otel-traces-v0_7` | Index for trace queries (must have Jaeger API enabled) |

### Step 4: Configure Log Fields

| Setting | Default | Description |
|---|---|---|
| **Message Field** | `body` | Field containing the log message text |
| **Level Field** | `severity_text` | Field containing the log severity level |

### Step 5: Save & Test

Click **Save & Test**. You should see: `Connected to Quickwit successfully. Found N indexes.`

## Usage

### Log Queries

1. In **Explore** or a Dashboard panel, select the Quickwit Explorer data source
2. Set **Type** to **Logs**
3. Select an **Index** from the dropdown (or type to search)
4. Enter a **Lucene query** (e.g. `severity_text:ERROR AND service.name:my-service`)
5. Set the result **Limit** (default: 100, max: 1000)

Log results will display in Grafana's native log view with:
- Timestamp
- Log body/message
- Severity level coloring
- Clickable `traceID` links that open the trace detail view

### Trace Search

1. Set **Type** to **Traces**
2. Select a **Service** from the dropdown (auto-populated from Quickwit's Jaeger API)
3. Optionally filter by **Operation**, **Tags**, **Min/Max Duration**
4. Click **Search Traces**

Results appear in a table with columns: Trace ID, Trace Name, Service, Start Time, Duration, Span Count.

Click any **Trace ID** to open the full trace waterfall view.

### Trace ID Lookup

1. Set **Type** to **Trace ID**
2. Enter a known trace ID
3. Press Enter

The full trace waterfall view (span hierarchy, timing, tags, logs) will render using Grafana's built-in trace visualization.

### Metric Aggregation

1. Set **Type** to **Metrics**
2. Enter a Lucene query
3. Set **Group By** field (default: `_timestamp`) and **Interval** (e.g. `30s`, `1m`, `1h`)

Results display as a time-series chart.

## Log ↔ Trace Correlation

This plugin automatically creates **data links** between logs and traces:

- **Log → Trace**: When viewing logs, any row with a `trace_id` field will show a clickable link that opens the corresponding trace in the trace view
- **Trace → Log**: From the trace search results table, you can navigate to related logs

No manual data link configuration is required.

## Architecture

This is a **frontend-only** Grafana data source plugin. It uses Grafana's built-in **data source proxy** (`routes` in `plugin.json`) to forward requests to Quickwit, which means:

- No Go backend binary required
- No additional ports or services to manage
- Quickwit URL is configured server-side (not exposed to the browser)
- Works behind corporate firewalls where Grafana server can reach Quickwit but browsers cannot

### API Endpoints Used

| Quickwit API | Purpose |
|---|---|
| `GET /api/v1/indexes` | Index discovery and auto-complete |
| `POST /api/v1/{index}/search` | Log and metric queries |
| `GET /api/v1/{index}/jaeger/api/traces/{traceId}` | Trace detail lookup |
| `GET /api/v1/{index}/jaeger/api/traces?service=...` | Trace search |
| `GET /api/v1/{index}/jaeger/api/services` | Service name discovery |
| `GET /api/v1/{index}/jaeger/api/services/{service}/operations` | Operation discovery |

## Troubleshooting

### "Cannot load indexes" in Config Editor

The index auto-complete requires the data source to be saved first. Enter the Quickwit URL, click **Save & Test**, then the index dropdowns will populate.

### Trace view shows empty

- Ensure your Quickwit version is >= 0.7.0 (Jaeger API support)
- Verify the trace index name is correct (check with `curl http://quickwit:7280/api/v1/indexes`)
- Ensure trace data exists in the selected time range

### 403 or proxy errors

- Check that `allow_loading_unsigned_plugins` includes `quickwit-explorer-datasource`
- Verify the Quickwit URL is reachable from the Grafana server (not just from your browser)

## Development

```bash
# Install dependencies
npm install

# Development build with watch
npx webpack -c ./webpack.config.ts --env development --watch

# Production build
npx webpack -c ./webpack.config.ts --env production

# Package for distribution
cd dist && zip -r ../quickwit-explorer-datasource-1.0.0.zip .
```

## License

Apache License 2.0
