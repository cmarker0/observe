# Re-basing @nestjs/observe on OpenTelemetry

The estimate is roughly 4–5 weeks for one engineer. Phase 0 results and
corrections are in `phase-0-findings.md`.

## Phase 0: Spike (1–2 days)

- Read `OperationTraceRegistry` and the protocol agents closely. The whole
  plan depends on how entangled the registry is with the agents.
- Fork the package and get `vitest.int` running as the behavioural baseline.
- Decide the keep/drop list (below).

## Phase 1: Extract a seam (2–3 days)

- Define a `SpanRecorder` interface covering the root operation (start/end),
  steps (start/end), manual spans and error recording.
- Make the HTTP, GraphQL, RPC, Bull, schedule and WS agents and
  `createInstanceDecorator` depend only on that interface.
- Port the existing registry behind it first, so tests stay green. Then you
  can swap implementations without touching the agents.

## Phase 2: OTel recorder (about a week)

- Make `@opentelemetry/api` a peer dependency. The app owns the SDK, exporter
  and resource, so `serviceId` and `serviceVersion` disappear.
- Replace the AsyncLocalStorage `Map` copying in `createInstanceDecorator`
  with `tracer.startActiveSpan()` and OTel context. Benchmark this first: see
  the GraphQL caveat in the findings.
- Provider method spans become INTERNAL spans named `Class.method`, with
  `code.*` attributes.
- Root spans become SERVER or CONSUMER spans with semconv attributes
  (`http.request.method`, `http.route`, `rpc.*`, `messaging.*`, `graphql.*`).
- Errors use `recordException` plus `setStatus`, with the existing redaction
  applied to message and stack.
- Keep `skipInstrumentation`, the `ignore` hooks and the `setAttributes`
  hooks, since they prevent spans before they exist.
- Map `tracesSampleRate` (number _and_ function form) to a head `Sampler`.

## Phase 3: Propagation and coexistence (3–4 days)

- Add `propagation.extract` for HTTP, RPC and Bull, and `inject` when
  enqueuing jobs.
- Check for an already-active span before creating the root. If
  `instrumentation-http` is running, create the root as a child or INTERNAL
  span so you don't get duplicate server spans.
- Derive log correlation (`traceIdKey`, the logger patcher) from the active
  span context. `x-request-id` can become a span attribute.

## Phase 4: Metrics and logs (3–4 days)

- Map runtime metrics to observable gauges on the Meter API with semconv
  names, or just recommend `instrumentation-runtime-node`.
- For logs, start with trace correlation only. Add OTel Logs API emission
  later.

## Phase 5: Hardening (about a week)

- Port the integration tests to assert span trees with
  `InMemorySpanExporter`. Trees gain a root level, so rewrite the assertions
  for the new shape rather than copying them.
- Add propagation tests and one end-to-end run against a real OTel Collector.
- Benchmark overhead against the old agent and against
  `instrumentation-nestjs-core`.
- Keep the existing guards for proxies that throw on property access
  (nestjs-cls).

## Keep / drop

- **Keep:**
  - the `instrument` hook
  - the instance decorator
  - the protocol agents
  - GraphQL parsing
  - redaction
  - the manual span API, re-based on `startActiveSpan`
  - `tracesSampleRate`, as a `Sampler`
- **Drop:**
  - the worker thread, shared buffer and wire contract
  - the degraded-ingest protocol
  - credentials and endpoint
  - profiling
  - source-context shipping
- **Drop the outgoing patches** and recommend the official pg, undici and
  http instrumentations. This avoids duplicate spans and a maintenance
  burden.
- **Defer to v2:** `skipSpans` and `spanCollapse`. They need the whole tree
  after it finishes, which standard processors don't give you. For v1, use
  Collector tail-sampling or filter processors. A v2 option is a custom span
  processor that buffers per local root.

## Main risks

- **GraphQL context.** Its driver hooks return into execution instead of
  wrapping it, and OTel has no `context.attach`. See the findings. This
  replaces "registry coupling", which Phase 0 found to be mostly internal.
- **Packaging and start-up order.** The package is ESM-only, and the OTel SDK
  must start before Nest bootstraps.
- **Maintenance.** You'd be carrying a fork of a 0.x package. Consider
  proposing the `SpanRecorder` seam upstream to the maintainers.
