import { AsyncLocalStorage } from "async_hooks";
import type { ObserveAgentSharedBuffer } from "../agent/observe-agent.shared-buffer.js";
import type { JobSnapshot, RequestSnapshot } from "../interfaces/index.js";
import {
  CALLER_METADATA_KEY,
  TRACE_REGISTRY_KEY,
} from "../observe.constants.js";
import type { OperationTraceRegistry } from "../services/operation-trace.registry.js";
import type { TraceSamplerService } from "../services/trace-sampler.service.js";
import type { LogRedactor } from "../utils/log-redactor.js";
import { uuidv7 } from "../utils/uuid-v7.util.js";
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

/**
 * Where an operation's handle is kept in the async store, for
 * `currentOperation`. A symbol, so it cannot collide with a user's
 * `setAttributes` keys.
 */
const OPERATION_KEY = Symbol("nestjs.observe.operation");

/**
 * The registry key an unrecorded operation's store is pointed at. Nothing is
 * ever registered under it, so steps inside the operation find no trace -
 * rather than falling back to the correlation id, which an adopted
 * `x-request-id` or an inherited job id may share with an operation that *is*
 * open in this process. Not a valid request id, so it cannot be adopted.
 */
const UNRECORDED_KEY = "#unrecorded";

type Store = Map<unknown, unknown>;

/**
 * `SpanRecorder` over `OperationTraceRegistry` and the shared buffer - the
 * agent's original backend, unchanged in behaviour.
 *
 * Built before the DI container, like the registry it wraps, so the
 * instrumentation hook can hold it. Shipping needs the buffer and sampling
 * needs the sampler, both providers; the module hands them over through
 * `attach` once they exist. Until then operations are recorded unsampled and
 * dropped when they end, which only a decorator used outside the module sees.
 */
export class RegistrySpanRecorder extends SpanRecorder {
  private buffer: ObserveAgentSharedBuffer | undefined;
  private sampler: TraceSamplerService | undefined;

  constructor(
    private readonly als: AsyncLocalStorage<any>,
    private readonly registry: OperationTraceRegistry,
    private readonly traceIdKey: string,
  ) {
    super();
  }

  attach(buffer: ObserveAgentSharedBuffer, sampler?: TraceSamplerService) {
    this.buffer = buffer;
    this.sampler = sampler;
  }

  runOperation<T>(
    start: OperationStart,
    fn: (operation: OperationHandle | undefined) => T,
  ): T {
    const store = this.als.getStore() as Store | undefined;
    if (start.record === false || !this.shouldCapture(start)) {
      store?.set(TRACE_REGISTRY_KEY, UNRECORDED_KEY);
      return fn(undefined);
    }

    // The registry key is usually the correlation id itself. Two cases give
    // the operation a key of its own: a job, whose inherited id may belong to
    // a request still open in this process and is reused by every retry; and
    // a request whose adopted `x-request-id` is already open here - a caller
    // that fans out or retries sends the same id twice.
    let registryKey = start.correlationId;
    if (start.kind === "job" || this.registry.hasTrace(registryKey)) {
      registryKey = uuidv7();
      store?.set(TRACE_REGISTRY_KEY, registryKey);
    }

    this.registry.startTrace(
      registryKey,
      start.kind === "job"
        ? ({ tags: start.tags, ...start.job } as JobSnapshot)
        : // Only the fields the agent set: `endTrace` reads an `operationId`
          // key, even an undefined one, as "a handler ran".
          withoutUndefined({
            protocol: start.protocol as string,
            operationId: start.operationId,
            tags: start.tags,
            attributes: start.attributes,
          }),
      start.correlationId,
    );

    const operation = new RegistryOperation(this, registryKey, start.kind);
    store?.set(OPERATION_KEY, operation);
    return fn(operation);
  }

  currentOperation(): OperationHandle | undefined {
    const store = this.als.getStore() as Store | undefined;
    return store?.get(OPERATION_KEY) as RegistryOperation | undefined;
  }

  runStep<T>(
    name: StepName,
    fn: (traced: boolean) => T,
    onError?: (error: unknown) => void,
  ): T {
    const store = this.als.getStore() as Store | undefined;
    const registryKey = this.registryKeyOf(store);
    if (!registryKey || !store) {
      return fn(false);
    }

    const { className, methodKey } = name;
    const stepId = this.registry.internalStartTraceStep(
      registryKey,
      className,
      methodKey,
      store.get(CALLER_METADATA_KEY) as string | undefined,
    );
    // No step means the registry holds no snapshot for this key: the
    // operation was ignored or sampled out, but its store still carries the
    // id - log correlation needs it there. Nothing was opened, so nothing
    // must be closed.
    if (stepId === undefined) {
      return fn(false);
    }

    const label = `${className}#${methodKey}`;
    const onReturnValue = (res: unknown) => {
      this.registry.internalEndTraceStep(
        registryKey,
        label,
        className,
        methodKey,
        stepId,
      );
      return res;
    };
    const onFailure = (err: unknown): never => {
      onError?.(err);
      this.registry.internalEndTraceStep(
        registryKey,
        label,
        className,
        methodKey,
        stepId,
        err as Error | string | object,
      );
      throw err;
    };

    return this.als.run(
      new Map([...store.entries(), [CALLER_METADATA_KEY, stepId]]),
      () => {
        try {
          const result = fn(true);
          if (result instanceof Promise) {
            return result.then(onReturnValue).catch(onFailure) as T;
          }
          return onReturnValue(result) as T;
        } catch (err) {
          return onFailure(err);
        }
      },
    );
  }

  openStep(name: StepName, tags?: Tags): StepHandle | undefined {
    const store = this.als.getStore() as Store | undefined;
    const opened = this.startStep(store, name);
    if (!opened) {
      return undefined;
    }
    if (tags) {
      const node = this.registry.getActiveSpan(
        opened.registryKey,
        opened.stepId,
      );
      if (node) {
        node.tags = { ...node.tags, ...tags };
      }
    }
    return opened.handle;
  }

  enterStep(
    name: StepName,
  ): { step: StepHandle; restore: () => void } | undefined {
    const store = this.als.getStore() as Store | undefined;
    const previousCallerId = store?.get(CALLER_METADATA_KEY);
    const opened = this.startStep(store, name);
    if (!opened || !store) {
      return opened && { step: opened.handle, restore: () => undefined };
    }

    // The store is shared by reference across the operation, so this makes
    // the step current for everything that continues from here - and
    // `restore` is a real restore rather than a scoped one.
    store.set(CALLER_METADATA_KEY, opened.stepId);
    return {
      step: opened.handle,
      restore: () => {
        if (previousCallerId === undefined) {
          store.delete(CALLER_METADATA_KEY);
        } else {
          store.set(CALLER_METADATA_KEY, previousCallerId);
        }
      },
    };
  }

  runManualSpan<T>(
    name: string,
    fn: (span: SpanTagSink) => T | Promise<T>,
  ): Promise<T> {
    const store = this.als.getStore() as Store | undefined;
    return this.registry.createManualSpan(
      this.registryKeyOf(store) ?? "",
      store?.get(CALLER_METADATA_KEY) as string | undefined,
      name,
      (delegate) => fn(delegate.asSink()),
    ) as Promise<T>;
  }

  activeSpan(): SpanTagSink | "untraced" | undefined {
    const store = this.als.getStore() as Store | undefined;
    const registryKey = this.registryKeyOf(store);
    if (!registryKey) {
      return "untraced";
    }
    const node = this.registry.getActiveSpan(
      registryKey,
      store?.get(CALLER_METADATA_KEY) as string | undefined,
    );
    if (!node) {
      return this.registry.hasTrace(registryKey) ? undefined : "untraced";
    }
    const tags = (node.tags ??= {});
    return {
      // Optional on the shared `TraceSpan` interface, always set on the nodes
      // the registry builds - and `getActiveSpan` only returns those.
      id: node.spanId ?? "",
      name: node.name,
      setTags: (update) => Object.assign(tags, update),
    };
  }

  captureError(error: Error, tags?: Tags): void {
    const store = this.als.getStore() as Store | undefined;
    const registryKey = this.registryKeyOf(store);
    // Reporting an error must not raise one, and an operation that is not
    // recorded has nowhere to report it.
    if (!registryKey || registryKey === UNRECORDED_KEY) {
      return;
    }
    this.registry.captureError(
      registryKey,
      store?.get(CALLER_METADATA_KEY) as string | undefined,
      error,
      tags ?? {},
    );
  }

  currentSpanId(): string | undefined {
    const store = this.als.getStore() as Store | undefined;
    return store?.get(CALLER_METADATA_KEY) as string | undefined;
  }

  injectContext(): void {
    // Snapshots correlate on the trace id alone, which callers already send
    // as `x-request-id`; there is no span context to carry.
  }

  getRedactor(): LogRedactor | null {
    return this.registry.getRedactor();
  }

  /** @internal - the operation handle's way back into the registry. */
  finishOperation(
    registryKey: string,
    kind: OperationStart["kind"],
    outcome: () => OperationEnd,
  ): void {
    setTimeout(async () => {
      const { statusCode, userId, status, captureRequest } = outcome();
      if (kind === "job") {
        this.registry.endTrace(registryKey, { status });
      } else {
        this.registry.endTrace(registryKey, { statusCode, userId });
      }

      // `pluckSnapshot` deletes what it returns, so a trace already plucked
      // answers undefined. Dropped rather than reported: there is nothing to
      // send.
      const snapshot = await this.registry.pluckSnapshot(registryKey);
      if (!snapshot || !this.buffer) {
        return;
      }
      if (kind === "job") {
        this.buffer.insertJobSnapshot(snapshot as JobSnapshot);
        return;
      }
      const request = snapshot as RequestSnapshot;
      const captured = captureRequest?.(request);
      if (captured) {
        request.request = captured;
      }
      this.buffer.insertRequestSnapshot(request);
    }, 0);
  }

  /** @internal */
  get operationRegistry(): OperationTraceRegistry {
    return this.registry;
  }

  private shouldCapture(start: OperationStart): boolean {
    if (!start.sampling || !this.sampler) {
      return true;
    }
    const [protocol, attributes] = start.sampling;
    // The sampler's overloads pair each protocol with its attributes; the
    // tuple type already holds that pairing, which a spread cannot convey.
    return (
      this.sampler.shouldCapture as (
        protocol: string,
        attributes: object,
      ) => boolean
    ).call(this.sampler, protocol, attributes);
  }

  /**
   * The key the current operation is registered under: its own registry key
   * where it was given one, the correlation id otherwise.
   */
  private registryKeyOf(store: Store | undefined): string | undefined {
    const key = store?.get(TRACE_REGISTRY_KEY) ?? store?.get(this.traceIdKey);
    return typeof key === "string" ? key : undefined;
  }

  private startStep(store: Store | undefined, name: StepName) {
    const registryKey = this.registryKeyOf(store);
    if (!registryKey) {
      return undefined;
    }
    const { className, methodKey } = name;
    const stepId = this.registry.internalStartTraceStep(
      registryKey,
      className,
      methodKey,
      store?.get(CALLER_METADATA_KEY) as string | undefined,
    );
    if (stepId === undefined) {
      return undefined;
    }

    let ended = false;
    const handle: StepHandle = {
      end: (error?: unknown) => {
        // A driver may report one call twice - a callback and an `error`
        // event, a settled promise and a late listener. The registry counts
        // closes against opens, so a second close would unbalance it.
        if (ended) {
          return;
        }
        ended = true;
        this.registry.internalEndTraceStep(
          registryKey,
          `${className}#${methodKey}`,
          className,
          methodKey,
          stepId,
          error === undefined || error === null ? undefined : error,
        );
      },
    };
    return { registryKey, stepId, handle };
  }
}

class RegistryOperation implements OperationHandle {
  private done = false;

  constructor(
    private readonly recorder: RegistrySpanRecorder,
    private readonly registryKey: string,
    private readonly kind: OperationStart["kind"],
  ) {}

  setRoute(path: string): void {
    if (!this.done) {
      this.recorder.operationRegistry.addRouteMetadataToTrace(
        this.registryKey,
        path,
      );
    }
  }

  annotate(update: {
    operationId: string;
    tags?: Tags;
    attributes?: { originalUrl?: string };
  }): void {
    if (!this.done) {
      this.recorder.operationRegistry.addGraphQLMetadataToTrace(
        this.registryKey,
        update,
      );
    }
  }

  end(outcome: () => OperationEnd = () => ({})): void {
    if (this.done) {
      return;
    }
    this.done = true;
    this.recorder.finishOperation(this.registryKey, this.kind, outcome);
  }

  abandon(): void {
    // Once ended, the key may already belong to a later operation that
    // adopted the same id; dropping it would lose that one instead.
    if (this.done) {
      return;
    }
    this.done = true;
    this.recorder.operationRegistry.abandonTrace(this.registryKey);
  }
}

function withoutUndefined<T extends object>(value: T): T {
  return Object.fromEntries(
    Object.entries(value).filter(([, entry]) => entry !== undefined),
  ) as T;
}
