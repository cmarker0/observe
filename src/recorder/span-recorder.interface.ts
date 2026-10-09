/**
 * The seam between the protocol agents and whatever records their spans.
 *
 * Phase 0 sketch - nothing implements or consumes this yet. It is shaped from
 * what the agents call on `OperationTraceRegistry` today (see
 * `docs/otel-migration/phase-0-findings.md`), with two goals:
 *
 * - the agents stop touching the registry, the shared buffer, the async store's
 *   `CALLER_METADATA_KEY` / `TRACE_REGISTRY_KEY` slots and the snapshot types;
 * - an OpenTelemetry recorder can sit behind it without the agents changing
 *   again, which is why steps *wrap* work (`runStep`) instead of being opened
 *   and closed by id: OTel context only flows through `context.with(fn)`.
 */
import type { JobContext } from "../interfaces/index.js";

export type TagValue = string | number | boolean;
export type Tags = Record<string, TagValue>;

export type OperationKind = "http" | "rpc" | "grpc" | "graphql" | "ws" | "job";

export type JobStatus = "completed" | "failed";

/** What an agent knows about an operation as it begins. */
export interface OperationStart {
  kind: OperationKind;
  /** Transport name as reported today: `http`, `TCP`, `KAFKA`, `ws`, ... */
  protocol?: string;
  /**
   * The id log lines and enqueued jobs correlate on: an adopted `x-request-id`,
   * an id inherited from the enqueuing operation, or a freshly minted one. The
   * registry recorder reports under it; the OTel recorder sets it as an
   * attribute and lets the span context carry the real trace id.
   */
  correlationId: string;
  operationId?: string;
  tags?: Tags;
  attributes?: { method?: string; originalUrl?: string };
  /** Set for job runs. */
  job?: JobContext & Record<string, unknown>;
  /**
   * Inbound headers / metadata / job options to extract a remote parent from.
   * Ignored by the registry recorder; `propagation.extract` for OTel (Phase 3).
   */
  carrier?: Record<string, unknown>;
  /**
   * Written into the operation's async store before anything else runs: the
   * user's `setAttributes` output, and agent bookkeeping like the job runner's
   * active-job marker.
   */
  contextEntries?: Record<string | symbol, unknown>;
  /**
   * `false` when the agent's protocol-specific `ignore` matched. Ignoring stays
   * in the agents - each protocol's predicate takes different arguments -
   * while sampling moves into the recorder (a `Sampler` for OTel).
   */
  record: boolean;
  /** Input to the function form of `tracesSampleRate` (a head sampler). */
  samplingAttributes?: Record<string, unknown>;
}

export interface OperationEnd {
  statusCode?: number;
  userId?: string;
  status?: JobStatus;
  /**
   * Called only if the recorder decides the request is worth capturing (error,
   * or slower than `http.capture.slowerThanMs`) - so the agent never needs the
   * finished snapshot to decide.
   */
  captureRequest?: () => Record<string, unknown> | undefined;
}

/** A started operation. Every method is a no-op after `end` or `abandon`. */
export interface OperationHandle {
  readonly correlationId: string;
  /** HTTP `setOnRouteTriggered`. */
  setRoute(method: string, path: string): void;
  /**
   * GraphQL labelling. `operationId` is first-wins, as in
   * `addGraphQLMetadataToTrace`; tags and attributes merge.
   */
  annotate(update: {
    operationId?: string;
    tags?: Tags;
    attributes?: { originalUrl?: string };
  }): void;
  /** Ends and exports. Replaces `endTrace` + `pluckSnapshot` + buffer insert. */
  end(outcome?: OperationEnd): void;
  /** Drops without exporting - a client that aborted. */
  abandon(): void;
}

/** Names a step the way spans are grouped today: `Class#method`. */
export interface StepName {
  className: string;
  methodKey: string;
}

/** A step opened without wrapping the work - see `openStep`. */
export interface StepHandle {
  setTags(tags: Tags): void;
  /** Idempotent: a second call is ignored (drivers report some calls twice). */
  end(error?: unknown): void;
}

/** What `TraceSpanDelegate` writes through, instead of a shared tags object. */
export interface SpanTagSink {
  readonly id: string;
  readonly name: string | undefined;
  setTags(tags: Tags): void;
}

export interface SpanRecorder {
  // --- operations (protocol agents) ----------------------------------------

  /**
   * Runs `fn` as the given operation. Returns `fn`'s result untouched.
   *
   * The recorder - not the agent - owns sampling, `ignore`, and the async
   * context `fn` runs in, so the agent passes in what the hooks need and gets
   * the handle to end it with. `handle` is `undefined` when the operation is
   * not recorded (ignored or sampled out); correlation still works inside `fn`.
   */
  runOperation<T>(
    start: OperationStart,
    fn: (handle: OperationHandle | undefined) => T,
  ): T;

  /**
   * The operation the current async context belongs to - for hooks that fire
   * outside the callback that started it (HTTP response hook, RPC end).
   */
  currentOperation(): OperationHandle | undefined;

  // --- steps (instance decorator, outgoing calls, GraphQL) ----------------

  /**
   * Runs `fn` as a child step of whatever is current, ending it when `fn`
   * returns, throws, or its promise settles. Calls `fn` untraced when there is
   * no recorded operation. `onError` runs before the error is recorded, which
   * is where the decorator relabels its `Proxy.` stack frame.
   */
  runStep<T>(
    name: StepName,
    fn: () => T,
    onError?: (error: unknown) => void,
  ): T;

  /**
   * Opens a step without wrapping the work, for callers that only get a
   * start and an end callback (outgoing driver patches). The step is NOT made
   * current: work started from here does not nest under it.
   */
  openStep(name: StepName, tags?: Tags): StepHandle | undefined;

  /**
   * As `openStep`, but also makes the step current for the rest of the
   * enclosing async context until the returned `restore` is called.
   *
   * Exists for one caller: the GraphQL agent, whose driver hooks return into
   * the execution instead of wrapping it. The registry recorder implements it
   * by mutating the shared store as the agent does today; the OTel recorder
   * has no `context.attach` to lean on and needs its own ALS slot - see the
   * findings doc. Anything that can wrap must use `runStep`.
   */
  enterStep(
    name: StepName,
  ): { step: StepHandle; restore: () => void } | undefined;

  // --- manual API (TracerService) -----------------------------------------

  runManualSpan<T>(name: string, fn: (span: SpanTagSink) => T): T;
  /** `undefined` when there is a recorded operation but no step open in it. */
  activeSpan(): SpanTagSink | "untraced" | undefined;
  captureError(error: Error, tags?: Tags): void;

  // --- correlation (log forwarder, logger patcher, job enqueue) -----------

  /** Correlation id and current span id for a log line. */
  currentIds(): { correlationId?: string; spanId?: string };
  /**
   * Writes what a downstream job needs to continue this trace into its
   * options. Registry: `JOB_TRACE_OPTION_KEY`. OTel: `propagation.inject`.
   */
  injectInto(carrier: Record<string, unknown>): void;
}
