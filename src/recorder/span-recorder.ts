/**
 * The seam between the code that sees work happen - protocol agents, the
 * instance decorator, the outgoing patches, `TracerService` - and whatever
 * records it as spans.
 *
 * Callers keep the module's async store (`AsyncLocalStorage` with the trace id
 * and `setAttributes` values in it): that store is user-facing and outlives
 * any one recorder. What moves behind this class is everything else - which
 * operation and step are current, sampling, how a finished operation is
 * assembled and where it is shipped.
 *
 * Steps *wrap* the work they measure (`runOperation`, `runStep`,
 * `runManualSpan`) wherever the caller can, because that is the only shape an
 * OpenTelemetry recorder can make current: OTel context flows through
 * `context.with(ctx, fn)` and has no `attach`. `openStep` and `enterStep` are
 * the exceptions, for callers that only ever get a start and an end callback.
 *
 * An abstract class rather than an interface so Nest can inject it by type.
 */
import type { JobSnapshot, RequestSnapshot } from "../interfaces/index.js";
import type { LogRedactor } from "../utils/log-redactor.js";

export type TagValue = string | number | boolean;
export type Tags = Record<string, TagValue>;

export type JobStatus = NonNullable<JobSnapshot["status"]>;

/** What `TraceSamplerService.shouldCapture` is asked for an operation. */
export type SamplingInput =
  | ["http", { url: string; method: string }]
  | ["rpc", { transport: string; ctx: Record<string, any> }]
  | ["grpc", { call: Record<string, any> }]
  | ["graphql", { operationId: string }]
  | ["ws", { gateway: string; pattern: string }];

/** What an agent knows about an operation as it begins. */
export interface OperationStart {
  /** Jobs are reported apart from requests. */
  kind: "request" | "job";
  /**
   * The id log lines and enqueued jobs correlate on - an adopted
   * `x-request-id`, an id inherited from the enqueuing operation, or one
   * freshly minted. Already in the async store under `traceIdKey`.
   */
  correlationId: string;
  /** Transport name as reported today: `http`, `TCP`, `KAFKA`, `ws`, ... */
  protocol?: string;
  operationId?: string;
  tags?: Tags;
  attributes?: { method?: string; originalUrl?: string };
  /** Job runs only: queue, name, id and driver metadata. */
  job?: Partial<JobSnapshot>;
  /** Omitted for operations `tracesSampleRate` does not apply to (jobs). */
  sampling?: SamplingInput;
  /**
   * `false` when the agent's own `ignore` matched. The operation is then not
   * recorded, and its async store is kept from joining any other operation
   * that happens to share its correlation id.
   */
  record?: boolean;
}

export interface OperationEnd {
  statusCode?: number;
  userId?: string;
  status?: JobStatus;
  /**
   * Asked once the operation is complete, with what it is reported with;
   * returns the request to attach, or `undefined` for none. Lets the HTTP
   * agent keep its capture rules without ever holding the finished snapshot.
   */
  captureRequest?: (finished: {
    error?: unknown;
    duration?: number;
  }) => RequestSnapshot["request"];
}

/** A recorded operation. Every method is a no-op once it has ended. */
export interface OperationHandle {
  /** HTTP `setOnRouteTriggered`. */
  setRoute(path: string): void;
  /**
   * GraphQL labelling. `operationId` is first-wins; tags and attributes merge
   * over what the transport recorded.
   */
  annotate(update: {
    operationId: string;
    tags?: Tags;
    attributes?: { originalUrl?: string };
  }): void;
  /**
   * Ends and ships the operation, off the current tick. `outcome` is read on
   * that later tick, so hooks like `getUserId` see the request as it finished.
   */
  end(outcome?: () => OperationEnd): void;
  /** Drops the operation without shipping it - a client that aborted. */
  abandon(): void;
}

/** Names a step the way spans are grouped: `Class#method`. */
export interface StepName {
  className: string;
  methodKey: string;
}

/** A step opened without wrapping the work - see `openStep`. */
export interface StepHandle {
  /** Idempotent: drivers report some calls twice. */
  end(error?: unknown): void;
}

/** What `TraceSpanDelegate` reads and writes through. */
export interface SpanTagSink {
  readonly id: string;
  readonly name: string | undefined;
  setTags(tags: Tags): void;
}

export abstract class SpanRecorder {
  // --- operations ---------------------------------------------------------

  /**
   * Runs `fn` as an operation, inside the async store the caller has already
   * entered. `fn` gets `undefined` when the operation is not recorded -
   * ignored (`record: false`) or sampled out; the store still correlates
   * either way.
   */
  abstract runOperation<T>(
    start: OperationStart,
    fn: (operation: OperationHandle | undefined) => T,
  ): T;

  /**
   * The recorded operation the current async context belongs to, for hooks
   * that fire outside the callback that started it.
   */
  abstract currentOperation(): OperationHandle | undefined;

  // --- steps --------------------------------------------------------------

  /**
   * Runs `fn` as a child step of whatever is current, ending the step when
   * `fn` returns, throws or its promise settles. `fn` is told whether it is
   * being traced; when it is not, nothing was opened. `onError` runs before
   * the error is recorded.
   */
  abstract runStep<T>(
    name: StepName,
    fn: (traced: boolean) => T,
    onError?: (error: unknown) => void,
  ): T;

  /**
   * Opens a step without wrapping the work, for callers that only get a
   * start and an end callback. The step is not made current: work started
   * from here does not nest under it.
   */
  abstract openStep(name: StepName, tags?: Tags): StepHandle | undefined;

  /**
   * As `openStep`, and also makes the step current for the rest of the
   * enclosing async context until `restore` is called. For the GraphQL agent
   * only, whose driver hooks return into execution instead of wrapping it.
   * Anything that can wrap must use `runStep`.
   */
  abstract enterStep(
    name: StepName,
  ): { step: StepHandle; restore: () => void } | undefined;

  // --- manual API (TracerService) -----------------------------------------

  /** Runs `fn` in a named child span of whatever is current. */
  abstract runManualSpan<T>(
    name: string,
    fn: (span: SpanTagSink) => T | Promise<T>,
  ): Promise<T>;

  /**
   * The current span, `"untraced"` when the operation is not recorded at all,
   * or `undefined` when it is recorded but no span is open.
   */
  abstract activeSpan(): SpanTagSink | "untraced" | undefined;

  abstract captureError(error: Error, tags?: Tags): void;

  // --- correlation and shared helpers -------------------------------------

  /** The current span id, for a log line written inside it. */
  abstract currentSpanId(): string | undefined;

  /**
   * The redactor error payloads go through, for a caller adding to the same
   * payload - so what it adds is held to the same rules. `null` when
   * redaction is switched off.
   */
  abstract getRedactor(): LogRedactor | null;
}
