# OTel migration: Phase 2 notes

Phase 2 adds `OtelSpanRecorder` (`src/recorder/otel-span-recorder.ts`), a
second `SpanRecorder` that records through the OpenTelemetry API. It is
**opt-in**: the registry recorder stays the default until Phase 5.

```ts
const { ObserveModule, ObserveInstrument } = createObserveModule({
  opentelemetry: true, // or { tracerProvider }
});
```

The application owns the SDK (provider, context manager, exporter, resource)
and registers it before the Nest app is created. `@opentelemetry/api` is an
**optional** peer dependency, loaded through `loadOptionalPeer` only when
`opentelemetry` is set. A missing package fails at `createObserveModule` with
a message that names it. The plan said "peer dependency". It is optional
here because the registry is still the default, and making it required can
wait for Phase 5.

## Mapping

| Observe concept                     | OTel                                                                                                                                                       |
| ----------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| HTTP operation                      | SERVER span `METHOD /route`, with `http.request.method`, `http.route`, `url.path`, `url.query`, `http.response.status_code`                                |
| GraphQL (standalone or in HTTP)     | `graphql.operation.type` / `graphql.operation.name` (from `Query.orders`) and `graphql.document`. Standalone GraphQL is a SERVER span named `Query.orders` |
| Kafka/RMQ/NATS/MQTT/Redis transport | CONSUMER span `process <pattern>`, with `messaging.system` and `messaging.destination.name`                                                                |
| TCP, gRPC, custom transport         | SERVER span, with `rpc.system` and `rpc.method`                                                                                                            |
| WS message                          | SERVER span `gateway:pattern`                                                                                                                              |
| Job run                             | CONSUMER span `process <queue>`, always a new trace, with `messaging.destination.name`, `messaging.message.id` and `nestjs.observe.job.*`                  |
| Provider method step                | INTERNAL span `Class.method`, with `code.function.name`                                                                                                    |
| Outgoing step (`openStep`)          | INTERNAL span `driver.method` plus the driver's tags                                                                                                       |
| `setAttributes` tags                | Span attributes on the operation span                                                                                                                      |
| correlation id                      | `nestjs.observe.correlation_id`. Phase 3 decides whether log correlation moves to the OTel trace id.                                                       |
| `userId`                            | `enduser.id`                                                                                                                                               |

Attribute names outside semconv are exported as `ObserveAttributes`.

### Errors

- A thrown error is recorded as an `exception` event. Message and stack go
  through the module's `LogRedactor`, with the same `redaction` options as
  the registry. The span also gets `error.type` and status ERROR. The SDK's
  `recordException` is not used because it would bypass redaction.
- The root-error rule from the registry carries over. When a step directly
  under the operation throws, the operation is classified:
  - `IntrinsicException` maps to its own 4xx code, or to 400 if it has none.
  - Any other error maps to 500.
  - The classification overrides a transport code below 400.
- The classified code is in `nestjs.observe.status_code`, and
  `http.response.status_code` stays the wire value. A failing GraphQL
  resolver therefore reads 200 on the wire and 500 classified.
- Operation status follows semconv. HTTP is ERROR only for 5xx; a 4xx leaves
  it unset. Other transports are ERROR when an error escaped or the job
  failed. `nestjs.observe.error.handled` keeps the handled/unhandled split.
- `captureError` adds an exception event and the tags to the current span,
  and marks that span ERROR.

### Context and GraphQL

Steps run inside `context.with`, so instrumentations nested under them (pg,
undici and so on) parent correctly. The recorder also keeps its own
`AsyncLocalStorage` cell, which is option (a) from the Phase 0 findings:

- `runStep` and `runOperation` run inside a fresh cell.
- `enterStep` swaps the span inside the current cell, as the registry
  recorder rewrites its caller key. When there is no cell (standalone
  GraphQL, after the agent ran `enterWith`), it enters a new one.
- The current span is resolved like this: the cell wins while OTel still
  reports the span that was active when the cell was set. Once OTel reports
  a different span, something inner was opened through `context.with`, and
  that span wins. With no cell, the operation in the async store wins.
- So the recorder also works without a registered context manager, at the
  cost of not seeing foreign spans.

Operations nest under a span another instrumentation made current, such as
an `instrumentation-http` server span. They never nest under one of this
recorder's own spans, because only a leaked context could hold one. Jobs
always start a new trace. Proper propagation (extract/inject, links) and
de-duplicating SERVER spans are Phase 3.

### Sampling

`tracesSampleRate` (number and function form) and the `ignore` hooks are
decided before the operation span exists. A sampled-out or ignored operation
runs under a valid but **unsampled** span context. Nothing is recorded,
downstream services are told "not sampled", and a parent-based sampler (the
SDK default) drops everything beneath it. If the application's own sampler
drops the root, the operation is treated as unrecorded as well.

The plan's "map to a head `Sampler`" is done as this pre-span decision
rather than as an exported `Sampler` class. The function form needs Nest
request objects that an SDK-level sampler never sees.

## Deliberate differences from the registry recorder

1. **Root span level.** There is now a SERVER/CONSUMER span above the first
   handler step. This was expected (findings, "There is no root span
   today").
2. **`captureRequest` is not shipped.** Request headers and bodies are not
   copied into span attributes. That is a privacy decision to make
   deliberately, not by default. It can be revisited in Phase 3 or 4.
3. **`abandon` ends the span** with `nestjs.observe.abandoned=true` instead
   of dropping it. Children may already be exported, and dropping the root
   would orphan them.
4. **`activeSpan()` outside any step returns the operation span**, not
   `undefined`. `TracerService.activeSpan()` therefore tags the root instead
   of throwing.
5. **`skipSpans`, `spanCollapse` and `sourceContext` do not apply.** They
   need the finished snapshot tree (v2 or never, per the plan).
   `serviceId`, `serviceVersion` and the worker stay because metrics and
   forwarded logs still go to the collector until Phase 4.
6. **GraphQL over HTTP has no Nest route**, so its root is named `POST`
   (semconv: method only when there is no route). The operation is
   identified by the `graphql.operation.*` attributes.

## Verification

- Unit: 747 → **765 passed**, 1 skipped (+18 in `otel-span-recorder.spec.ts`:
  the 8 parity behaviours from the registry spec plus errors, context and
  naming). Two mutations were checked: removing cell resolution and not
  storing the operation each fail a test.
- Integration (Redis only, as before): **36 files passed, 3 skipped. 272
  passed, 58 skipped.** That is the old 268 plus 4 in
  `otel-collection.int-spec.ts`, which boots a real Express + Apollo app with
  `opentelemetry` on and asserts:
  - span kinds and the parentage controller → service
  - HTTP 500 with a redacted exception
  - the resolver chain under the GraphQL step
  - a GraphQL 200 classified as 500
  - nothing reaching the snapshot buffer
- Lint: 0 errors, 52 warnings (unchanged). Typecheck and build are clean.

## Benchmark

This is a rough microbenchmark, not a promise. It measures 20,000 sync
`runStep` calls in one operation on Node 22, with
`AsyncLocalStorageContextManager` and a `SimpleSpanProcessor` writing to an
in-memory exporter:

| Recorder | µs per step (3 runs) |
| -------- | -------------------- |
| registry | 13.1 / 15.1 / 11.7   |
| otel     | 21.7 / 20.4 / 23.8   |

The OTel figure includes SDK span creation and synchronous export, which the
registry defers to its worker. A `BatchSpanProcessor` would be closer to the
registry's shape. Either way, the plan's "hot-path win" from dropping the
`Map` copy does not show up yet. The extra cost is the OTel context plus
the cell write, as the findings predicted. This needs re-measuring under a
realistic load before Phase 5 makes OTel the default.

## Ready for Phase 3

Phase 3 covers:

- propagation (`extract` for HTTP, RPC and Bull; `inject` on enqueue)
- links from a job to its enqueuer
- coexistence with `instrumentation-http`
- moving log correlation onto the span context
