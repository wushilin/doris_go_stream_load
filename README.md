# Doris Stream Load Go Library

`dorisstreamload` is a Go library for streaming CSV rows or JSON objects into Apache Doris.

The public model is intentionally small:

- two modes: `csv` and `json`
- string input only
- `Send(...)` for one item
- `SendBatch(...)` for many items
- batching, retries, queueing, label polling, and delivery tracking are handled inside the library

## Install

```sh
go get <module-path>
```

Replace `<module-path>` with the module path for this repo.

## Quick Start

### CSV example

```go
client, err := dorisstreamload.NewClient(dorisstreamload.Config{
	StreamLoadURL:       "http://doris.example.com/api/analytics/events/_stream_load",
	Columns:             []string{"event_time", "user_id", "event_name"},
	Mode:                dorisstreamload.ModeCSV,
	Validation:          dorisstreamload.ValidateSyntax,
	AuthenticationType:  dorisstreamload.AuthenticationBasic,
	AuthenticationToken: "stream_user:stream_password",
	BatchBytes:          4 * 1024 * 1024,
	Linger:              200 * time.Millisecond,
	DorisUploadWorkers:  4,
})
if err != nil {
	panic(err)
}
defer client.Close()

handle, err := client.Send(`2026-04-28T10:00:00Z,42,login`)
if err != nil {
	panic(err)
}

result := handle.Wait()
if result.Err != nil {
	panic(result.Err)
}
```

### JSON example

```go
client, err := dorisstreamload.NewClient(dorisstreamload.Config{
	StreamLoadURL:       "http://doris.example.com/api/analytics/events/_stream_load",
	Columns:             []string{"event_time", "user_id", "payload"},
	Mode:                dorisstreamload.ModeJSON,
	Validation:          dorisstreamload.ValidateSyntax,
	AuthenticationType:  dorisstreamload.AuthenticationBasic,
	AuthenticationToken: "stream_user:stream_password",
	BatchBytes:          4 * 1024 * 1024,
	Linger:              300 * time.Millisecond,
	DorisUploadWorkers:  2,
})
if err != nil {
	panic(err)
}
defer client.Close()

handle, err := client.SendBatch([]string{
	`{"event_time":"2026-04-28T10:00:00Z","user_id":42,"payload":"a"}`,
	`{"event_time":"2026-04-28T10:00:01Z","user_id":43,"payload":"b"}`,
})
if err != nil {
	panic(err)
}

if result := handle.Wait(); result.Err != nil {
	panic(result.Err)
}
```

### Callback example

```go
_, err := client.SendBatchWithCallback(
	func(result dorisstreamload.DeliveryResult) {
		if result.Err != nil {
			log.Printf("delivery failed after %d attempt(s): %v", result.Attempts, result.Err)
			return
		}
		log.Printf("label=%s delivered", result.Response.Label)
	},
	[]string{
		`1,alice,ok`,
		`2,bob,ok`,
	},
)
if err != nil {
	panic(err)
}
```

## Public API

Primary methods:

- `Send(record string) (*Handle, error)`
- `SendBatch(records []string) (*Handle, error)`
- `SendWithCallback(callback DeliveryCallback, record string) (*Handle, error)`
- `SendBatchWithCallback(callback DeliveryCallback, records []string) (*Handle, error)`
- `SendBatchContext(ctx context.Context, records []string) (*Handle, error)`
- `Close() error`

## Input Model

Each submitted string is one logical item.

In `ModeCSV`:

- one string = one CSV row

In `ModeJSON`:

- one string = one JSON object

Examples:

```go
client.Send(`1,alice,ok`)
client.Send(`{"id":1,"name":"alice"}`)
```

`SendBatch([]string{...})` is one submitted batch containing multiple logical items.

`SendBatch(...)` returns one shared `Handle`, because that submitted batch stays whole and concludes as one Doris load outcome.

The library may merge several submitted batches into one outbound Doris stream-load request, but it does not split one submitted batch across multiple Doris requests.

## Modes

### `ModeCSV`

Each item is one CSV row.

Coalesced outbound body shape:

```text
row1
row2
row3
```

### `ModeJSON`

Each item is one JSON object.

Coalesced outbound body shape:

```json
[{"id":1},{"id":2},{"id":3}]
```

## Validation

Validation mode is controlled by `Config.Validation`.

Options:

- `ValidateNone`
- `ValidateSyntax`
- `ValidateStrict`

Default:

```go
Validation: dorisstreamload.ValidateSyntax
```

### `ValidateNone`

The library trusts the caller input.

- CSV: no CSV parsing
- JSON: no JSON parsing

### `ValidateSyntax`

Checks structural correctness before queue admission.

CSV checks:

- input is not blank
- parses as exactly one CSV row
- field count matches `len(Columns)`
- malformed quoting and escaping are rejected

JSON checks:

- input is not blank
- valid JSON
- top-level value is a JSON object

### `ValidateStrict`

Includes syntax validation. For JSON, it also requires:

- every configured column is present
- no extra keys are present

For CSV, `ValidateStrict` currently behaves the same as `ValidateSyntax`.

## Config

### Required fields

You must set:

- `Columns`
- `Mode`
- one of:
  - `StreamLoadURL` like `http://host:port/api/<db>/<table>/_stream_load`
  - or `Endpoint` + `Database` + `Table`

`Endpoint` is validated as a Doris FE base URL and must look like:

- `http://host:port`
- `https://host:port`

It must not include a path, query string, fragment, or embedded user info.

### Authentication

Optional authentication fields:

- `AuthenticationType`
- `AuthenticationToken`

Supported authentication types:

- `AuthenticationNone`
- `AuthenticationBasic`

For basic auth:

```go
AuthenticationType:  dorisstreamload.AuthenticationBasic,
AuthenticationToken: "user:password",
```

### Batching and queueing

`BatchBytes` and `Linger` work together like Kafka's `batch.size` and `linger.ms`: the batcher
accumulates items until the payload reaches `BatchBytes` bytes **or** the batch has been open for
`Linger` — whichever comes first.  Tune both together to trade latency for throughput.

A submitted batch that is larger than `BatchBytes` is rejected at intake with `ErrSendTooLarge`
rather than being sent as an oversized request.

#### `BatchBytes`

Maximum size (bytes) for one outbound Doris request body.  Also the admission limit per `Send`/`SendBatch` call.

Default: `90 MiB`

One submitted batch is never split across multiple Doris requests.

#### `Linger`

Maximum age of an open outbound batch before it is sealed and dispatched, even when not full.

Default: `5ms`

The timer starts when the first item enters the batch; new items joining before the deadline do not
extend it.

#### `MaxQueueSize`

Maximum number of submitted batches held in the intake queue.

Default: `100 000`

Each `Send(...)` or `SendBatch(...)` occupies one slot regardless of how many records it contains.

#### `MaxQueueWaitTime`

How long `Send(...)` and `SendBatch(...)` will block waiting for a free queue slot.

Default: `0` (wait indefinitely)

When the deadline expires the call returns `ErrQueueFull`.

#### `MaxUploadQueueSize`

Depth of the internal channel between the batcher and the upload workers.

Default: `1`

### Upload workers

#### `DorisUploadWorkers`

Number of concurrent goroutines sending batches to Doris.

Default: `1`

### Retry and upload timing

#### `DorisUploadRequestTimeout`

HTTP deadline for a single upload attempt or a single label-poll request.

Default: `300s`

Values shorter than `10s` are rejected during `NewClient(...)` to prevent pairing large uploads with
an unrealistically short timeout.

#### `DorisUploadTimeout`

Total time budget for deciding whether to attempt another upload after a retriable failure.

Default: `300s`

This does not interrupt an upload already in progress; it only governs whether a new attempt is started.

Internal upload retry backoff is fixed by the library:

- `1s`
- `2s`
- `4s`
- `4s` thereafter

#### `StatusPollTimeout`

Maximum time to spend in one label-polling phase after an ambiguous outcome.

Default:

- `300s`

Internal poll backoff is fixed by the library:

- `500ms`
- exponential growth
- capped at `4s`

### Fake sender

#### `FakeSend`

If `true`, bypasses real HTTP upload and returns success from the fake sender.

Default:

- `false`

#### `FakeSendDelay`

Artificial delay before fake success.

Default:

- `500ms`

### Logging

Optional logging fields:

- `Logger`
- `LogLevel`

Log levels:

- `LogLevelError`
- `LogLevelInfo`
- `LogLevelDebug`

### CSV formatting

Optional CSV fields:

- `CSVSeparator`
- `CSVQuote`

Defaults:

- separator: `,`
- quote: `"`

## Handles and Results

Every accepted logical item gets one `Handle`.

Supported handle methods:

- `Wait() DeliveryResult`
- `WaitContext(ctx context.Context) (DeliveryResult, error)`
- `Done() <-chan struct{}`
- `IsDone() bool`
- `Result() (DeliveryResult, bool)`

`DeliveryResult` contains:

| Field | Meaning |
|---|---|
| `Err` | `nil` on success |
| `Attempts` | Number of upload attempts used |
| `StatusCode` | Final HTTP status, or `0` on transport error |
| `Response` | Parsed Doris stream-load response when available |
| `StartedAt` | Time first attempt began |
| `FinishedAt` | Time final conclusion was reached |

## Client Stats

Call:

```go
stats := client.Stats()
```

The snapshot includes:

- worker state:
  - `TotalWorkers`
  - `IdleWorkers`
  - `BusyWorkers`
- job outcomes:
  - `TotalLoadJobs`
  - `ErrorJobs`
  - `ErrorRate`
- load time summary:
  - `AverageLoadTime`
  - `P50LoadTime`
  - `P90LoadTime`
  - `P99LoadTime`
  - `P999LoadTime`
- retry summary:
  - `AverageRetries`
- throughput summary:
  - `TotalBytesSent`
  - `AverageLoadSize`
  - `AverageBytesRate`
  - `RecordsSent`
  - `AverageRecordsRate`
  - `TotalUploadAttempts`

Rates are lifetime averages since client creation.

## Callbacks

Callbacks run once per submitted batch, not once per logical item.

That means:

- `SendWithCallback(...)` fires once
- `SendBatchWithCallback(...)` also fires once

The returned handle for that submitted batch and the callback both observe the same final `DeliveryResult`.

## Errors

Sentinel errors:

- `ErrClientClosed`
- `ErrQueueFull`
- `ErrSendTooLarge`

## LoaderConfig

`Config` is the runtime struct used directly in Go code. `LoaderConfig` is its JSON-serializable
counterpart — load it from a file, convert to `Config`, and pass to `NewClient`.

```go
lc, err := dorisstreamload.LoadLoaderConfig("loader.json")
cfg, err := lc.Config()
client, err := dorisstreamload.NewClient(cfg)
```

Helpers:

- `LoadLoaderConfig(path string) (LoaderConfig, error)`
- `SaveLoaderConfig(path string, cfg LoaderConfig) error`
- `LoaderConfigFromConfig(Config) LoaderConfig`
- `(LoaderConfig).Config() (Config, error)`

Duration fields use Go duration strings: `"500ms"`, `"2s"`, `"5m"`.

### Connection

| JSON key | Type | Description |
|---|---|---|
| `stream_load_url` | string | Full stream load URL, e.g. `http://host:8030/api/db/table/_stream_load` |
| `endpoint` | string | Doris FE base URL, e.g. `http://host:8030` (used with `database` + `table`) |
| `database` | string | Target database (required when using `endpoint`) |
| `table` | string | Target table (required when using `endpoint`) |
| `columns` | []string | Column names in the order they appear in each record |
| `mode` | string | `"csv"` or `"json"` |
| `headers` | object | Extra HTTP headers forwarded to Doris on every request |

### Authentication

| JSON key | Type | Description |
|---|---|---|
| `authentication_type` | string | `""` (none) or `"basic"` |
| `authentication_token` | string | `"user:password"` for basic auth |

### Batching and queueing

| JSON key | Type | Default | Description |
|---|---|---|---|
| `batch_bytes` | int | 94371840 (90 MiB) | Max outbound request body size; also the per-send admission limit |
| `linger` | duration | `"5ms"` | Max age of an open batch before it is sealed |
| `max_queue_size` | int | 100000 | Max submitted batches held in the intake queue |
| `max_queue_wait_time` | duration | `"0"` (forever) | How long `Send` blocks waiting for a free queue slot; `0` means wait indefinitely |
| `max_upload_queue_size` | int | 1 | Depth of the internal channel between batcher and upload workers |
| `doris_upload_workers` | int | 1 | Number of concurrent goroutines uploading batches to Doris |
| `validation` | string | `"syntax"` | Input validation: `"none"`, `"syntax"`, or `"strict"` |

### Retry and upload timing

| JSON key | Type | Default | Description |
|---|---|---|---|
| `doris_upload_timeout` | duration | `"300s"` | Total time budget for deciding whether additional upload attempts should happen after a retriable outcome |
| `doris_upload_request_timeout` | duration | `"300s"` | HTTP deadline for one upload request or one label-poll request (min `"10s"`) |
| `status_poll_timeout` | duration | `"300s"` | Max time spent polling label state after an ambiguous outcome |

### Labelling and callbacks

| JSON key | Type | Default | Description |
|---|---|---|---|
| `label_prefix` | string | `"go_stream_load"` | Prefix for generated Doris load labels |
| `callback_timeout` | duration | `"100ms"` | Max time allowed for a delivery callback to run before a slow-callback warning is logged |
| `slow_callback_warn` | duration | `"10ms"` | Threshold at which a slow callback warning is emitted |

### CSV formatting

| JSON key | Type | Default | Description |
|---|---|---|---|
| `csv_separator` | string | `","` | Field separator character |
| `csv_quote` | string | `"\""` | Quote character |

### Logging

| JSON key | Type | Default | Description |
|---|---|---|---|
| `log_level` | int | `1` (Info) | `0` = Error, `1` = Info, `2` = Debug |

### Testing

| JSON key | Type | Default | Description |
|---|---|---|---|
| `fake_send` | bool | `false` | Bypass real HTTP; always returns success |
| `fake_send_delay` | duration | `"500ms"` | Artificial delay injected by the fake sender |
| `fake_send_delay_set` | bool | `false` | Set to `true` when `fake_send_delay` is explicitly `"0s"` to distinguish from unset |

## Shutdown

`Close()`:

- stops intake
- drains queued work
- waits for in-flight deliveries

After `Close()`:

- new sends return `ErrClientClosed`
- already accepted handles still complete

## Design Notes

This section is lower priority than the usage sections above. It explains why the library looks the way it does.

### Why only strings

The library accepts only strings so the transport boundary stays obvious:

- caller owns serialization
- library owns queueing, batching, retry, polling, and delivery tracking

### Why only two modes

Only these modes exist:

- `csv`
- `json`

This keeps the model small and predictable:

- CSV item -> one row
- JSON item -> one object

### Why batches are preserved

The intake queue stores submitted batches as units.

That means:

- queue admission is atomic for a submitted batch
- one submitted batch is never split across multiple Doris requests
- callbacks can naturally be defined per submitted batch

### How coalescing works

Coalescing is always enabled.

- CSV items are collected as `[]string` and joined with `\n` once
- JSON items are collected as `[]string` and wrapped into one array once

The library does not repeatedly concatenate strings while building a batch.

### Progress and backpressure

Normal flow:

1. caller submits a batch
2. intake queue accepts it
3. batcher accumulates outbound work
4. worker uploads to Doris
5. handles and callbacks are completed

If progress appears stalled, the usual cause is downstream backpressure:

- workers blocked in upload
- workers blocked in retry / polling
- dispatch channel full
- batcher cannot hand off work
- intake queue stops draining

That is usually not a queue deadlock; it is a pipeline backpressure problem.
``
