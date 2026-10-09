import { IntrinsicException } from "@nestjs/common";
import type * as Otel from "@opentelemetry/api";
import { AsyncLocalStorage } from "async_hooks";
import { randomBytes } from "crypto";
import type {
  LogCorrelation,
  PropagatorSource,
  TracerSource,
} from "../interfaces/observe-options.interface.js";
import type { TraceSamplerService } from "../services/trace-sampler.service.js";
import { LogRedactor } from "../utils/log-redactor.js";
import {
  OperationEnd,
  OperationHandle,
  OperationStart,
  SpanRecorder,
  SpanTagSink,
  StepHandle,
  StepName,
  Tags,
} from "./span-recorder.js";

/** The `@opentelemetry/api` module, handed in so the peer stays optional. */
export type OpenTelemetryApi = typeof Otel;

export const TRACER_NAME = "@nestjs/observe";

/** Attributes this package adds that no semantic convention covers. */
export const ObserveAttributes = {
  CORRELATION_ID: "nestjs.observe.correlation_id",
  OPERATION_ID: "nestjs.observe.operation_id",
  PROTOCOL: "nestjs.observe.protocol",
  /**
   * The status code the operation is classified under - the transport's own,
   * or one stood in for a root step that threw (see `classify`). Differs from
   * `http.response.status_code` for GraphQL, which answers 200 regardless.
   */
  STATUS_CODE: "nestjs.observe.status_code",
  /** `true` when the failure was raised on purpose (an `IntrinsicException`). */
  ERROR_HANDLED: "nestjs.observe.error.handled",
  /** The client went away before a response was written. */
  ABANDONED: "nestjs.observe.abandoned",
  JOB_NAME: "nestjs.observe.job.name",
  JOB_ATTEMPTS_MADE: "nestjs.observe.job.attempts_made",
  JOB_MAX_ATTEMPTS: "nestjs.observe.job.max_attempts",
  JOB_WAIT_DURATION: "nestjs.observe.job.wait_duration_ms",
} as const;

/** Mirrors `OperationTraceRegistry`: Nest's own handled/unhandled line. */
const UNHANDLED_ERROR_STATUS_CODE = 500;
const HANDLED_ERROR_STATUS_CODE = 400;

/** Microservice transports that deliver messages rather than calls. */
const MESSAGING_SYSTEMS: Record<string, string> = {
  KAFKA: "kafka",
  RMQ: "rabbitmq",
  NATS: "nats",
  MQTT: "mqtt",
  REDIS: "redis",
};

const GRAPHQL_ROOT_TYPES: Record<string, string> = {
  Query: "query",
  Mutation: "mutation",
  Subscription: "subscription",
};

const OPERATION_KEY = Symbol("nestjs.observe.otel.operation");

type Store = Map<unknown, unknown>;

/** How the recorder is set up - see `CreateObserveModuleOptions.opentelemetry`. */
export interface OtelRecorderSettings {
  tracerProvider?: TracerSource;
  propagator?: PropagatorSource;
  /** The async-store key log lines and enqueued jobs correlate on. */
  traceIdKey?: string;
  logCorrelation?: LogCorrelation;
}

type Carrier = Record<string, unknown>;

/**
 * Reads a carrier as transports hand it over: Node folds repeated headers
 * into arrays, Kafka header values are Buffers.
 */
const carrierGetter: Otel.TextMapGetter<Carrier> = {
  keys: (carrier) => Object.keys(carrier),
  get: (carrier, key) => fieldValue(carrier[key]),
};

const carrierSetter: Otel.TextMapSetter<Carrier> = {
  set: (carrier, key, value) => {
    carrier[key] = value;
  },
};

/**
 * What is current for this recorder, kept beside OTel's own context.
 *
 * OTel context only moves through `context.with`, which wraps; the GraphQL
 * driver hooks return instead, so a resolver step has to be made current
 * another way. A cell is that way: `runStep` and `runOperation` run inside a
 * fresh one, and `enterStep` swaps the span in the current one (or enters a
 * new one) the way the registry recorder rewrites its caller key.
 *
 * `base` is the OTel span that was active when `span` became current. While
 * OTel still reports that span, nothing more inner has been opened through
 * `context.with`, so the cell is the better answer; once it reports another,
 * someone - this recorder or another instrumentation - opened a span inside,
 * and OTel's answer wins.
 */
interface Cell {
  span: Otel.Span;
  base: Otel.Span | undefined;
}

/**
 * `SpanRecorder` over the OpenTelemetry API. Spans go to whatever tracer
 * provider the application registered - its SDK, exporter and resource; this
 * package only creates them.
 *
 * - An operation is a SERVER span (CONSUMER for jobs and message transports)
 *   carrying semantic-convention attributes for its protocol.
 * - A step is an INTERNAL span named `Class.method`.
 * - A thrown error is recorded as an `exception` event, redacted the way the
 *   registry redacts error payloads, and sets the span's status.
 * - `tracesSampleRate` is applied before the operation's span exists: a
 *   sampled-out operation runs under a non-recording, unsampled span context,
 *   so a parent-based sampler (the SDK default) drops everything beneath it.
 */
export class OtelSpanRecorder extends SpanRecorder {
  private readonly slot = new AsyncLocalStorage<Cell>();
  /** Operation root spans, for the root-error rule. */
  private readonly operations = new WeakMap<Otel.Span, OtelOperation>();
  /** Spans this recorder started; OTel spans do not expose their name. */
  private readonly names = new WeakMap<Otel.Span, string>();
  /** The span behind each open `StepHandle`, for `injectContext`. */
  private readonly steps = new WeakMap<StepHandle, Otel.Span>();
  private readonly tracer: Otel.Tracer;
  private readonly propagator: PropagatorSource;
  private sampler: TraceSamplerService | undefined;
  private redactor: LogRedactor | null = new LogRedactor();

  constructor(
    private readonly als: AsyncLocalStorage<any>,
    private readonly api: OpenTelemetryApi,
    private readonly settings: OtelRecorderSettings = {},
  ) {
    super();
    // The global provider is a proxy until the SDK registers, so a tracer
    // taken here still reaches an SDK started after the module is created.
    // The global propagator is read per call for the same reason.
    this.tracer = (settings.tracerProvider ?? api.trace).getTracer(
      TRACER_NAME,
    ) as Otel.Tracer;
    this.propagator = settings.propagator ?? api.propagation;
  }

  attach(redactor: LogRedactor | null, sampler?: TraceSamplerService) {
    this.redactor = redactor;
    this.sampler = sampler;
  }

  runOperation<T>(
    start: OperationStart,
    fn: (operation: OperationHandle | undefined) => T,
  ): T {
    const { api } = this;
    const store = this.als.getStore() as Store | undefined;
    store?.delete(OPERATION_KEY);

    // A span another instrumentation made current for this same inbound call
    // - instrumentation-http's SERVER span, a kafkajs CONSUMER span - already
    // continued the caller's trace and already is the operation's entry
    // point. The operation nests under it as INTERNAL rather than repeat it.
    // Never one of this recorder's own: only a context leaked from an earlier
    // operation could hold one of those.
    const active = api.trace.getSpan(api.context.active());
    const foreign =
      start.kind !== "job" && active !== undefined && !this.names.has(active)
        ? active
        : undefined;

    // Otherwise the caller's context, if it sent one. A request continues
    // that trace. A job starts its own and links back: the enqueuer may have
    // finished long ago, a retry runs again, and whatever the worker loop
    // happens to carry is not its cause.
    const remote = foreign ? undefined : this.extract(start.carrier);
    const parentContext = foreign
      ? api.context.active()
      : start.kind === "job"
        ? api.ROOT_CONTEXT
        : remote?.context ?? api.ROOT_CONTEXT;

    if (start.record === false || !this.shouldCapture(start)) {
      if (foreign) {
        return this.beneathForeign(foreign, store, () => fn(undefined));
      }
      const inherited =
        start.kind === "job" ? undefined : remote?.spanContext.traceId;
      const unsampled = this.unsampled(inherited);
      this.correlate(store, unsampled);
      return this.within(unsampled, parentContext, () => fn(undefined));
    }

    const described = describeOperation(api, start);
    const root = this.tracer.startSpan(
      described.name,
      {
        kind: foreign ? api.SpanKind.INTERNAL : described.kind,
        attributes: described.attributes,
        links:
          start.kind === "job" && remote
            ? [{ context: remote.spanContext }]
            : undefined,
        root: parentContext === api.ROOT_CONTEXT,
      },
      parentContext,
    );
    this.correlate(store, root);
    // The application's own sampler said no.
    if (!root.isRecording()) {
      return this.within(root, parentContext, () => fn(undefined));
    }

    this.names.set(root, described.name);
    const operation = new OtelOperation(this, root, start);
    this.operations.set(root, operation);
    store?.set(OPERATION_KEY, operation);
    return this.within(root, parentContext, () => fn(operation));
  }

  currentOperation(): OperationHandle | undefined {
    const store = this.als.getStore() as Store | undefined;
    return store?.get(OPERATION_KEY) as OtelOperation | undefined;
  }

  runStep<T>(
    name: StepName,
    fn: (traced: boolean) => T,
    onError?: (error: unknown) => void,
  ): T {
    const parent = this.currentSpan();
    if (!parent?.isRecording()) {
      return fn(false);
    }
    const parentContext = this.contextUnder(parent);
    const span = this.startStep(parentContext, name);

    const onReturnValue = (res: unknown) => {
      span.end();
      return res;
    };
    const onFailure = (err: unknown): never => {
      onError?.(err);
      this.endWithError(span, parent, err);
      throw err;
    };

    return this.within(span, parentContext, () => {
      try {
        const result = fn(true);
        if (result instanceof Promise) {
          return result.then(onReturnValue).catch(onFailure) as T;
        }
        return onReturnValue(result) as T;
      } catch (err) {
        return onFailure(err);
      }
    });
  }

  openStep(name: StepName, tags?: Tags): StepHandle | undefined {
    const parent = this.currentSpan();
    if (!parent?.isRecording()) {
      return undefined;
    }
    const span = this.startStep(this.contextUnder(parent), name, tags);
    return this.stepHandle(span, parent);
  }

  enterStep(
    name: StepName,
  ): { step: StepHandle; restore: () => void } | undefined {
    const parent = this.currentSpan();
    if (!parent?.isRecording()) {
      return undefined;
    }
    const span = this.startStep(this.contextUnder(parent), name);
    const step = this.stepHandle(span, parent);
    const active = this.api.trace.getSpan(this.api.context.active());

    const cell = this.slot.getStore();
    if (cell) {
      // Shared by reference across the rest of the operation, so `restore`
      // is a real restore rather than a scoped one.
      const previous = { ...cell };
      cell.span = span;
      cell.base = active;
      return {
        step,
        restore: () => {
          cell.span = previous.span;
          cell.base = previous.base;
        },
      };
    }

    // No enclosing cell: a GraphQL operation with no transport in front of
    // it, whose store the agent installed with `enterWith`. Same here.
    const entered: Cell = { span, base: active };
    this.slot.enterWith(entered);
    return {
      step,
      restore: () => {
        entered.span = parent;
      },
    };
  }

  async runManualSpan<T>(
    name: string,
    fn: (span: SpanTagSink) => T | Promise<T>,
  ): Promise<T> {
    const parent = this.currentSpan();
    if (!parent?.isRecording()) {
      return fn(detachedSink(name));
    }
    const parentContext = this.contextUnder(parent);
    const span = this.tracer.startSpan(
      name,
      { kind: this.api.SpanKind.INTERNAL },
      parentContext,
    );
    this.names.set(span, name);
    return this.within(span, parentContext, async () => {
      try {
        const result = await fn(this.sinkOf(span));
        span.end();
        return result;
      } catch (err) {
        this.endWithError(span, parent, err);
        throw err;
      }
    });
  }

  activeSpan(): SpanTagSink | "untraced" | undefined {
    const span = this.currentSpan();
    return span?.isRecording() ? this.sinkOf(span) : "untraced";
  }

  captureError(error: Error, tags?: Tags): void {
    const span = this.currentSpan();
    // Reporting an error must not raise one, and an operation that is not
    // recorded has nowhere to report it.
    if (!span?.isRecording()) {
      return;
    }
    if (tags) {
      span.setAttributes(tags);
    }
    this.recordError(span, error);
  }

  currentSpanId(): string | undefined {
    const span = this.currentSpan();
    return span?.isRecording() ? span.spanContext().spanId : undefined;
  }

  injectContext(carrier: Record<string, unknown>, step?: StepHandle): void {
    const span = (step && this.steps.get(step)) ?? this.currentSpan();
    // Unrecorded, OTel's own context is the one to pass on: this recorder's
    // unsampled stand-in, or the span of the instrumentation it deferred to.
    const context = span?.isRecording()
      ? this.contextUnder(span)
      : this.api.context.active();
    try {
      this.propagator.inject(context, carrier, carrierSetter);
    } catch {
      // A propagator that throws costs the downstream service its parent,
      // not the application its call.
    }
  }

  getRedactor(): LogRedactor | null {
    return this.redactor;
  }

  /** @internal - the operation handle's way back. */
  finishOperation(operation: OtelOperation, outcome: () => OperationEnd): void {
    // Timed now; `outcome` is read a tick later, as the registry recorder
    // reads it, so hooks like `getUserId` see the request as it finished.
    const endTime = performance.timeOrigin + performance.now();
    setTimeout(() => {
      const { statusCode, userId, status } = outcome();
      const { root, start } = operation;
      if (userId !== undefined) {
        root.setAttribute("enduser.id", userId);
      }
      if (statusCode !== undefined && isHttp(start)) {
        root.setAttribute("http.response.status_code", statusCode);
      }
      const classified = classify(statusCode, operation.rootError);
      if (classified !== undefined) {
        root.setAttribute(ObserveAttributes.STATUS_CODE, classified);
      }

      const { rootError } = operation;
      if (rootError !== undefined) {
        root.setAttribute(
          ObserveAttributes.ERROR_HANDLED,
          rootError instanceof IntrinsicException,
        );
      }

      // Over HTTP a 4xx is the client's failure, not the server's: semantic
      // conventions leave the status unset for it. Elsewhere an error that
      // escaped the handler failed the operation, raised on purpose or not.
      const failed = isHttp(start)
        ? classified !== undefined && classified >= 500
        : rootError !== undefined ||
          status === "failed" ||
          (classified !== undefined && classified >= 500);
      if (failed) {
        root.setAttribute(
          "error.type",
          errorType(rootError) ?? status ?? String(classified),
        );
        root.setStatus({ code: this.api.SpanStatusCode.ERROR });
      }
      root.end(endTime);
    }, 0);
  }

  /** @internal */
  abandonOperation(operation: OtelOperation): void {
    // Spans already exported cannot be recalled, so the operation is ended
    // rather than dropped - its children would be orphaned otherwise.
    operation.root.setAttribute(ObserveAttributes.ABANDONED, true);
    operation.root.end();
  }

  /** @internal */
  setName(span: Otel.Span, name: string): void {
    span.updateName(name);
    this.names.set(span, name);
  }

  /**
   * The span work started here belongs under: this recorder's cell while
   * OTel has nothing more inner, the operation in the async store when a
   * hook-driven operation never got to run inside one.
   */
  private currentSpan(): Otel.Span | undefined {
    const cell = this.slot.getStore();
    const active = this.api.trace.getSpan(this.api.context.active());
    if (cell) {
      return !active || active === cell.base || active === cell.span
        ? cell.span
        : active;
    }
    const store = this.als.getStore() as Store | undefined;
    const operation = store?.get(OPERATION_KEY) as OtelOperation | undefined;
    return operation?.root ?? active;
  }

  /**
   * The caller's span context from an inbound carrier, if it holds a valid
   * one. Carriers come off the wire - headers, packet metadata, job options
   * in Redis - so the propagator's own validation is all that admits them.
   */
  private extract(
    raw: unknown,
  ): { context: Otel.Context; spanContext: Otel.SpanContext } | undefined {
    try {
      const carrier = toCarrier(raw);
      if (!carrier) {
        return undefined;
      }
      const context = this.propagator.extract(
        this.api.ROOT_CONTEXT,
        carrier,
        carrierGetter,
      ) as Otel.Context;
      const spanContext = this.api.trace.getSpanContext(context);
      return spanContext && this.api.trace.isSpanContextValid(spanContext)
        ? { context, spanContext }
        : undefined;
    } catch {
      return undefined;
    }
  }

  /**
   * An unrecorded operation under another instrumentation's span. OTel's
   * context is left alone - that span stays current, and stays what is
   * propagated - and only this recorder is told there is nothing to record.
   */
  private beneathForeign<T>(
    foreign: Otel.Span,
    store: Store | undefined,
    fn: () => T,
  ): T {
    this.correlate(store, foreign);
    const silent = this.api.trace.wrapSpanContext({
      ...foreign.spanContext(),
      traceFlags: this.api.TraceFlags.NONE,
    });
    return this.slot.run({ span: silent, base: foreign }, fn);
  }

  /**
   * Puts the operation's OTel trace id where log lines and enqueued jobs read
   * their correlation id, unless the application asked to keep its own. The
   * id the agent minted or adopted stays on the span as
   * `nestjs.observe.correlation_id`.
   */
  private correlate(store: Store | undefined, span: Otel.Span): void {
    const key = this.settings.traceIdKey;
    if (
      !store ||
      key === undefined ||
      this.settings.logCorrelation === "correlation-id"
    ) {
      return;
    }
    const spanContext = span.spanContext();
    if (this.api.trace.isSpanContextValid(spanContext)) {
      store.set(key, spanContext.traceId);
    }
  }

  private contextUnder(parent: Otel.Span): Otel.Context {
    return this.api.trace.setSpan(this.api.context.active(), parent);
  }

  private within<T>(
    span: Otel.Span,
    parentContext: Otel.Context,
    fn: () => T,
  ): T {
    return this.slot.run({ span, base: span }, () =>
      this.api.context.with(this.api.trace.setSpan(parentContext, span), fn),
    );
  }

  private startStep(parentContext: Otel.Context, name: StepName, tags?: Tags) {
    const label = `${name.className}.${name.methodKey}`;
    const span = this.tracer.startSpan(
      label,
      {
        kind: this.api.SpanKind.INTERNAL,
        attributes: { "code.function.name": label, ...tags },
      },
      parentContext,
    );
    this.names.set(span, label);
    return span;
  }

  private stepHandle(span: Otel.Span, parent: Otel.Span): StepHandle {
    let ended = false;
    const handle: StepHandle = {
      end: (error?: unknown) => {
        // A driver may report one call twice - a callback and an `error`
        // event, a settled promise and a late listener.
        if (ended) {
          return;
        }
        ended = true;
        if (error === undefined || error === null) {
          span.end();
        } else {
          this.endWithError(span, parent, error);
        }
      },
    };
    this.steps.set(handle, span);
    return handle;
  }

  private endWithError(span: Otel.Span, parent: Otel.Span, error: unknown) {
    this.recordError(span, error);
    span.end();
    // Only a step directly under the operation decides the operation failed:
    // an error thrown deeper down may have been caught by its caller.
    this.operations.get(parent)?.failWith(error);
  }

  private recordError(span: Otel.Span, error: unknown) {
    const type = errorType(error) ?? "Error";
    const raw =
      error instanceof Error
        ? { message: error.message, stack: error.stack }
        : { message: String(error), stack: undefined };
    const redact = (text: string) =>
      this.redactor ? this.redactor.redactMessage(text) : text;
    const message = redact(raw.message);

    span.addEvent("exception", {
      "exception.type": type,
      "exception.message": message,
      ...(raw.stack !== undefined && {
        "exception.stacktrace": redact(raw.stack),
      }),
    });
    span.setAttribute("error.type", type);
    span.setStatus({ code: this.api.SpanStatusCode.ERROR, message });
  }

  private sinkOf(span: Otel.Span): SpanTagSink {
    return {
      id: span.spanContext().spanId,
      name: this.names.get(span),
      setTags: (tags) => span.setAttributes(tags),
    };
  }

  /**
   * A span context that is valid - so it propagates, telling every service
   * downstream the trace is not sampled - but records nothing.
   */
  private unsampled(traceId?: string): Otel.Span {
    return this.api.trace.wrapSpanContext({
      traceId: traceId ?? randomBytes(16).toString("hex"),
      spanId: randomBytes(8).toString("hex"),
      traceFlags: this.api.TraceFlags.NONE,
    });
  }

  private shouldCapture(start: OperationStart): boolean {
    if (!start.sampling || !this.sampler) {
      return true;
    }
    const [protocol, attributes] = start.sampling;
    return (
      this.sampler.shouldCapture as (
        protocol: string,
        attributes: object,
      ) => boolean
    ).call(this.sampler, protocol, attributes);
  }
}

class OtelOperation implements OperationHandle {
  private done = false;
  private hasOperationId: boolean;
  rootError: unknown;

  constructor(
    private readonly recorder: OtelSpanRecorder,
    readonly root: Otel.Span,
    readonly start: OperationStart,
  ) {
    this.hasOperationId = start.operationId !== undefined;
  }

  setRoute(path: string): void {
    if (this.done) {
      return;
    }
    this.root.setAttribute("http.route", path);
    const method = this.start.attributes?.method;
    this.recorder.setName(this.root, method ? `${method} ${path}` : path);
  }

  annotate(update: {
    operationId: string;
    tags?: Tags;
    attributes?: { originalUrl?: string };
  }): void {
    if (this.done) {
      return;
    }
    if (!this.hasOperationId) {
      this.hasOperationId = true;
      this.root.setAttributes(graphqlAttributes(update.operationId));
    }
    if (update.tags) {
      this.root.setAttributes(update.tags);
    }
    if (update.attributes?.originalUrl !== undefined) {
      this.root.setAttribute("graphql.document", update.attributes.originalUrl);
    }
  }

  failWith(error: unknown): void {
    this.rootError = error;
  }

  end(outcome: () => OperationEnd = () => ({})): void {
    if (this.done) {
      return;
    }
    this.done = true;
    this.recorder.finishOperation(this, outcome);
  }

  abandon(): void {
    if (this.done) {
      return;
    }
    this.done = true;
    this.recorder.abandonOperation(this);
  }
}

/** Span name, kind and semantic-convention attributes for an operation. */
function describeOperation(
  api: OpenTelemetryApi,
  start: OperationStart,
): { name: string; kind: Otel.SpanKind; attributes: Otel.Attributes } {
  const attributes: Otel.Attributes = {
    ...start.tags,
    [ObserveAttributes.CORRELATION_ID]: start.correlationId,
  };
  if (start.protocol !== undefined) {
    attributes[ObserveAttributes.PROTOCOL] = start.protocol;
  }
  if (start.operationId !== undefined) {
    attributes[ObserveAttributes.OPERATION_ID] = start.operationId;
  }

  if (start.kind === "job") {
    const job = start.job ?? {};
    const queue = job.queueName ?? "job";
    Object.assign(
      attributes,
      definedOnly({
        "messaging.operation.type": "process",
        "messaging.destination.name": job.queueName,
        "messaging.message.id": job.id,
        [ObserveAttributes.JOB_NAME]: job.name,
        [ObserveAttributes.JOB_ATTEMPTS_MADE]: job.attemptsMade,
        [ObserveAttributes.JOB_MAX_ATTEMPTS]: job.maxAttempts,
        [ObserveAttributes.JOB_WAIT_DURATION]: job.waitDuration,
      }),
    );
    return {
      name: `process ${queue}`,
      kind: api.SpanKind.CONSUMER,
      attributes,
    };
  }

  const protocol = start.protocol ?? "";
  if (isHttp(start)) {
    const method = start.attributes?.method;
    Object.assign(
      attributes,
      definedOnly({
        "http.request.method": method,
        ...splitUrl(start.attributes?.originalUrl),
      }),
    );
    return {
      name: method ?? "HTTP",
      kind: api.SpanKind.SERVER,
      attributes,
    };
  }

  const operationId = start.operationId;
  if (protocol === "graphql") {
    Object.assign(
      attributes,
      operationId ? graphqlAttributes(operationId) : {},
      definedOnly({ "graphql.document": start.attributes?.originalUrl }),
    );
    return {
      name: operationId ?? "graphql",
      kind: api.SpanKind.SERVER,
      attributes,
    };
  }

  const messaging = MESSAGING_SYSTEMS[protocol];
  if (messaging) {
    Object.assign(
      attributes,
      definedOnly({
        "messaging.system": messaging,
        "messaging.operation.type": "process",
        "messaging.destination.name": operationId,
      }),
    );
    return {
      name: operationId ? `process ${operationId}` : `process ${messaging}`,
      kind: api.SpanKind.CONSUMER,
      attributes,
    };
  }

  if (protocol !== "ws") {
    Object.assign(
      attributes,
      definedOnly({
        "rpc.system": protocol.toLowerCase() || undefined,
        "rpc.method": operationId,
      }),
    );
  }
  return {
    name: operationId ?? protocol,
    kind: api.SpanKind.SERVER,
    attributes,
  };
}

function isHttp(start: OperationStart): boolean {
  return (
    start.kind === "request" &&
    (start.protocol === "http" || start.protocol === "https")
  );
}

function graphqlAttributes(operationId: string): Otel.Attributes {
  const [rootType, field] = operationId.split(".");
  const type = GRAPHQL_ROOT_TYPES[rootType];
  return {
    [ObserveAttributes.OPERATION_ID]: operationId,
    ...(type && { "graphql.operation.type": type }),
    ...(field && { "graphql.operation.name": field }),
  };
}

function splitUrl(url: string | undefined): Otel.Attributes {
  if (url === undefined) {
    return {};
  }
  const queryAt = url.indexOf("?");
  return queryAt === -1
    ? { "url.path": url }
    : {
        "url.path": url.slice(0, queryAt),
        "url.query": url.slice(queryAt + 1),
      };
}

/**
 * The status code an operation is filed under. A root step that threw
 * outranks a success code from the transport - RPC reports none, GraphQL
 * answers 200 - exactly as `OperationTraceRegistry.endTrace` decides it.
 */
function classify(
  reported: number | undefined,
  rootError: unknown,
): number | undefined {
  if (rootError === undefined || (reported !== undefined && reported >= 400)) {
    return reported;
  }
  if (!(rootError instanceof IntrinsicException)) {
    return UNHANDLED_ERROR_STATUS_CODE;
  }
  const status = (rootError as { getStatus?: () => unknown }).getStatus?.();
  return typeof status === "number" && status >= 400 && status < 500
    ? status
    : HANDLED_ERROR_STATUS_CODE;
}

function errorType(error: unknown): string | undefined {
  if (error === undefined) {
    return undefined;
  }
  if (error instanceof Error) {
    return error.constructor?.name || error.name || "Error";
  }
  return typeof error;
}

/**
 * The fields of an inbound carrier: a plain record (headers, packet metadata,
 * a job's stamped context) or gRPC `Metadata`, read through `getMap()`.
 */
function toCarrier(raw: unknown): Carrier | undefined {
  if (typeof raw !== "object" || raw === null) {
    return undefined;
  }
  const getMap = (raw as { getMap?: unknown }).getMap;
  if (typeof getMap === "function") {
    const map: unknown = getMap.call(raw);
    return typeof map === "object" && map !== null
      ? (map as Carrier)
      : undefined;
  }
  return raw as Carrier;
}

function fieldValue(value: unknown): string | undefined {
  if (typeof value === "string") {
    return value;
  }
  if (Buffer.isBuffer(value)) {
    return value.toString("utf8");
  }
  if (Array.isArray(value)) {
    return fieldValue(value[0]);
  }
  return undefined;
}

function detachedSink(name: string): SpanTagSink {
  return { id: "", name, setTags: () => undefined };
}

function definedOnly(
  values: Record<string, string | number | boolean | undefined>,
): Otel.Attributes {
  return Object.fromEntries(
    Object.entries(values).filter(([, value]) => value !== undefined),
  );
}
