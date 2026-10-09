# OTel migration: Phase 3 notes

Phase 3 adds trace-context propagation and coexistence with other
instrumentations to the opt-in `OtelSpanRecorder`. The registry recorder
(still the default) is unchanged: its `injectContext` writes nothing and it
ignores the inbound carrier.

## The seam

`SpanRecorder` gained two things:

- `OperationStart.carrier`: the inbound fields a caller's context may ride
  in. Agents pass it without interpreting it.
- `injectContext(carrier, step?)`: writes the current context into an
  outgoing carrier. With `step`, the context is that step's, so the receiver
  nests under the outgoing call rather than beside it.

Propagation uses the global propagator (the SDK registers W3C
`traceparent`/`tracestate` and baggage by default), or
`opentelemetry: { propagator }`.

## Extract

| Inbound      | Carrier                                                                                             |
| ------------ | --------------------------------------------------------------------------------------------------- |
| HTTP         | request headers                                                                                     |
| Microservice | packet metadata (`ctx.getMetadata()`), else Kafka message headers (Buffer values are read as UTF-8) |
| gRPC         | `call.metadata` (read through `getMap()`)                                                           |
| Job run      | `opts.observeTraceContext`, stamped at enqueue                                                      |

A valid remote context becomes the parent of a request's root span. A job
run instead starts a trace of its own with a **link** to the enqueuing span:
the enqueuer may be long finished, a retry runs again, and a request trace
that grows by every job it ever queued is unreadable. That keeps the
Phase 2 rule that jobs never inherit a parent.

Carriers come off the wire (headers, packets, job options in Redis). Only
the propagator's own validation admits them, and a carrier that throws on
access is ignored.

WebSocket messages and standalone GraphQL have no carrier and start a trace
of their own, as before.

## Inject

| Outgoing                         | Where                                                                                            |
| -------------------------------- | ------------------------------------------------------------------------------------------------ |
| `fetch`/undici (`outgoing.http`) | request headers, with the outgoing call's own span as the parent                                 |
| `node:http` (`outgoing.http`)    | request headers, with the **calling** span as the parent: the call's span opens after headers go |
| Microservice client              | packet metadata, through the existing `setOnDispatchHook`                                        |
| `Queue.add` / `addBulk`          | `opts.observeTraceContext`, beside the existing `opts.observeTraceId`                            |

Outgoing HTTP follows `outgoing.http.propagateTraceId`, the same rule as
`x-request-id`. Fields the caller already set are never overwritten.
Repeatable jobs are still not stamped.

## Coexistence with other instrumentations

When a span another instrumentation opened is current as the operation
starts (an `instrumentation-http` SERVER span, a kafkajs CONSUMER span), that
span already extracted the caller's context and already is the entry point.
The operation then:

- nests under it as an **INTERNAL** span, keeping its name and attributes,
  instead of a second SERVER/CONSUMER span;
- does not extract again;
- when ignored or sampled out, leaves OTel's context alone. That span stays
  current and stays what is propagated, and only this recorder records
  nothing.

The outgoing side has no such check: a span cannot be seen before the
request exists. `instrumentation-http`/`-undici` and `outgoing.http` both make
CLIENT-side spans and both inject, so run one of them. That is documented on
the `opentelemetry` option.

## Log correlation: decided

**Default: logs correlate on the OTel trace id.** As each operation starts,
the recorder writes its trace id under `traceIdKey`. Everything that reads
that key follows:

- the JSON logger patcher's `traceId` and the text logger's `Trace ID:`
  suffix
- the stdout forwarder (`traceId`, plus `spanId` from `currentSpanId()`)
- `TracerService.currentTraceId()`
- `x-request-id` on outgoing calls and `observeTraceId` on enqueued jobs

A log line therefore joins its spans in any OTel backend with no extra field.
The id `traceIdGenerator` produced (an adopted `x-request-id` or a UUIDv7) is
kept on the span as `nestjs.observe.correlation_id`. Sampled-out operations
still get a trace id: the caller's when there is one, otherwise a random
one. So their logs still group, and still join upstream spans.

This changes what log queries match **in OTel mode only**: a query by an
inbound `x-request-id` now has to go through the span attribute. To keep the
old behaviour, set `opentelemetry: { logCorrelation: "correlation-id" }`.
Logs and spans then join only through `nestjs.observe.correlation_id`.

A job run correlates on its own trace id. The enqueuer's id, inherited
through `observeTraceId`, is its `nestjs.observe.correlation_id`.

## Verification

- Unit: 765 → **773 passed**, 1 skipped. That is +8 in
  `otel-span-recorder.spec.ts`:

  - continue a remote trace, and correlate logs on it
  - `correlation-id` mode
  - a malformed carrier
  - gRPC `Metadata` and Kafka Buffer headers
  - sampled-out operations keep the caller's trace id and propagate `-00`
  - an unrecorded operation under a foreign span
  - an outgoing step's own context injected
  - an enqueue → run round trip through `JobTraceRunner` with a link

  The existing foreign-parent test now also asserts INTERNAL. Six mutations
  each fail a test: kind, link, step lookup, inherited trace id, the foreign
  branch and Buffer decoding.

- Integration (Redis only): **37 files passed, 3 skipped. 275 passed, 58
  skipped.**
  - `otel-collection.int-spec.ts` (+2): an inbound `traceparent` is
    continued, and `currentTraceId()` returns its trace id. A `fetch` from
    one handler to another route stays in one trace, with the downstream
    SERVER span under the outgoing call. Removing injection fails it.
  - `otel-job-propagation.int-spec.ts` (new): a real BullMQ job carries the
    context through Redis and links back to the request.
- Lint: 0 errors, 52 warnings (unchanged). Typecheck and build are clean.

## Left for later

- No int test runs alongside `@opentelemetry/instrumentation-http`. The
  INTERNAL rule is unit-tested with a foreign span. Phase 5's end-to-end run
  against a real Collector is the place for it.
- `http.route` is not copied onto a foreign SERVER span (that needs
  `@opentelemetry/core`'s RPC metadata, not just the API).
- Kafka/RMQ/NATS producers outside Nest's `ClientProxy` are not injected
  into. RMQ and NATS consumers read only packet metadata.
- The `node:http` client's downstream parent is the calling span (see above).
