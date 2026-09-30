import {
  countTraceNodes,
  DEGRADED_TTL_MS,
  isNotableSnapshot,
} from "./degraded-ingest.protocol.js";
import { Inject, Injectable, Logger, Optional } from "@nestjs/common";
import { Counter, Gauge, Summary } from "../custom-metrics/index.js";
import {
  ObjectiveDeclaration,
  ObjectivesRegistry,
} from "../objectives/objectives.registry.js";
import {
  CustomMetricsEncoder,
  EncodedCustomMetric,
} from "../encoders/custom-metrics.encoder.js";
import {
  EncodedJobSnapshot,
  JobSnapshotEncoder,
} from "../encoders/job-snapshot.encoder.js";
import {
  EncodedRequestSnapshot,
  RequestSnapshotEncoder,
} from "../encoders/request-snapshot.encoder.js";
import {
  EncodedNodeRuntimeMetrics,
  RuntimeMetricsEncoder,
} from "../encoders/runtime-metrics.encoder.js";
import { CustomMetric } from "../interfaces/custom-metric.interface.js";
import { JobSnapshot } from "../interfaces/job-snapshot.interface.js";
import { NodeRuntimeMetrics } from "../interfaces/node-runtime-metrics.interface.js";
import { ObserveModuleOptionsWithDefaults } from "../interfaces/observe-options.interface.js";
import { RequestSnapshot } from "../interfaces/request-snapshot.interface.js";
import { OBSERVE_OPTIONS } from "../observe.constants.js";

const SHARED_BUFFER_SIZE = 1024 * 1024 * 16; // 16 MB
const DEFAULT_MAX_SNAPSHOTS_PER_TRANSACTION = 1000;

/**
 * Caps on the log portion of a batch.
 *
 * Logs are the one input with no natural ceiling - snapshots are gated by
 * `maxTracesPerBatch` and their largest caller-chosen field, the GraphQL
 * document, is capped where it is parsed - but a chatty service (or one stuck
 * in a retry loop) produces log lines without limit. Left uncapped they grow
 * the payload past `SHARED_BUFFER_SIZE`, and `encodeAndWrite` then throws for
 * the whole batch, so noisy logging silently costs you the traces and metrics
 * too.
 *
 * Both limits are needed: the count bounds ordinary chatter, and the per-entry
 * length bounds the single pathological line - a serialised payload dump or a
 * deep stack trace can be megabytes on its own.
 */
const MAX_LOGS_PER_TRANSACTION = 250;
const MAX_LOG_ENTRY_LENGTH = 4 * 1024; // 4 KB
const TRUNCATION_SUFFIX = "... [truncated by observe]";

/**
 * Log lines held back for a later batch when the current one is full.
 *
 * The per-batch cap bounds the *request*; this bounds the *process*. Together
 * they let a batch stay small - at most 250 lines - while up to 2250 can be in
 * flight overall, so a burst larger than one batch spills here instead of
 * being thrown away.
 *
 * Bounded because this is the one input with no natural limit. A process stuck
 * in a retry loop must eventually lose lines rather than grow this array until
 * the heap gives out - dropping telemetry is recoverable, taking the host
 * process down with it is not.
 */
const MAX_PENDING_LOGS = 2000;

/**
 * A log line waiting to be shipped.
 *
 * `level`, `context` and `attributes` are optional because they only exist for
 * lines the parser could classify - raw stdout that matches no known format is
 * still forwarded, carrying just its text.
 */
interface BufferedLogEntry {
  /**
   * Timestamp of the log entry in milliseconds since epoch.
   */
  timestamp: number;

  /**
   * Trace the line was emitted under, when one could be resolved.
   */
  traceId?: string;

  /**
   * Span the line was written inside, when one could be resolved. Only ever set
   * together with `traceId`, and only when that trace is the one the async
   * store is currently in - see StdoutForwarderService#toLogEntry.
   */
  spanId?: string;

  /**
   * Severity, lowercased to match the API's LogLevel values.
   */
  level?: string;

  /**
   * Nest logger context - the emitting class name, usually.
   */
  context?: string;

  /**
   * Fields a structured log carried beyond the modelled ones.
   */
  attributes?: Record<string, unknown>;

  /**
   * Text content of the log entry.
   */
  text: string;
}

/** Declarations one batch carries at most - the collector's own limit. */
const MAX_OBJECTIVES_PER_BATCH = 100;

/**
 * Payload structure for the agent metrics.
 * This payload is used to send metrics from the main thread to the worker thread.
 * It includes service information, request snapshots, runtime metrics, and custom metrics.
 */
interface AgentMetricsPayload {
  /**
   * Unique identifier for the service.
   */
  serviceId: string;
  /**
   * The release the process is running, as configured or inferred (see
   * `ObserveOptions.serviceVersion`). Absent when there is neither.
   */
  serviceVersion?: string;
  /**
   * Objectives declared with `@Objective`, each paired with the route its
   * handler was seen to serve (see ObjectivesRegistry).
   */
  objectives?: ObjectiveDeclaration[];
  /**
   * Spans built and then withheld because the collector reported this
   * account's ingestion is reduced.
   *
   * Reported because it cannot be inferred: the trees are not sent, so the
   * collector has no way to count the volume it is meant to be measuring the
   * account against. Without it, upgrading the agent would quietly earn an
   * account a longer reduced window - free service as a reward for behaving
   * well, which is the wrong incentive to build.
   */
  truncatedSpans?: number;
  /**
   * Whether this agent is forwarding logs.
   *
   * Reported on every batch rather than inferred from `logs` being present: a
   * service that has forwarding on but happened to log nothing this window is
   * indistinguishable from one that never enabled it, and that ambiguity is
   * exactly what the dashboard cannot resolve on its own.
   */
  forwardLogs?: boolean;
  /**
   * Array of encoded request snapshots.
   * Each snapshot contains information about a specific request,
   * including its traces, tags, and other metadata.
   */
  snapshots: EncodedRequestSnapshot[];
  /**
   * Node runtime metrics collected during the request processing.
   * This can include memory usage, CPU load, event loop delay, etc.
   */
  runtime?: EncodedNodeRuntimeMetrics;
  /**
   * Custom metrics collected during the request processing.
   * This can include counters or gauges that provide additional insights into the service's performance.
   */
  custom?: Array<EncodedCustomMetric>;
  /**
   * Optional array of log entries.
   * Each log entry contains a timestamp and the text content of the log.
   * If `forwardLogs` is enabled in the options, logs will be included here.
   */
  logs?: Array<BufferedLogEntry>;
  /**
   * Array of job snapshots.
   */
  jobs?: Array<EncodedJobSnapshot>;
}

@Injectable()
export class ObserveAgentSharedBuffer {
  private readonly _sharedBuffer = new SharedArrayBuffer(SHARED_BUFFER_SIZE);
  private readonly lock = new Int32Array(this._sharedBuffer, 0, 1);
  private readonly sharedBytes = new Uint8Array(this._sharedBuffer);
  private readonly encoder = new TextEncoder();
  private readonly logger = new Logger(ObserveAgentSharedBuffer.name);
  private _mainThreadBuffer: AgentMetricsPayload | null = null;

  /**
   * Log lines that did not fit the current batch, oldest first. Survives
   * `resetMainThreadBuffer` on purpose - it is the queue the batches are drawn
   * from, not part of any one of them.
   */
  private readonly _pendingLogs: BufferedLogEntry[] = [];

  /**
   * Metrics reporting a per-window quantity that is sitting in the current
   * buffer - a counter's increase, a summary's distribution - held so their
   * window can be closed once the payload is actually written.
   */
  private readonly _awaitingFlushAck = new Set<Counter<any> | Summary<any>>();

  get sharedBuffer() {
    return this._sharedBuffer;
  }

  constructor(
    @Inject(OBSERVE_OPTIONS)
    private readonly options: ObserveModuleOptionsWithDefaults,
    // Optional so the buffer can be built on its own, as most of its suites
    // build it; the module always provides it.
    @Optional()
    private readonly objectivesRegistry?: ObjectivesRegistry,
  ) {}

  insertRequestSnapshot(snapshot: RequestSnapshot) {
    // A request can end before its route metadata is ever captured - an auth
    // guard rejecting it, or a GraphQL document that never reaches a resolver
    // (`{ __typename }`) - leaving no operation id. The collector requires one
    // on every snapshot and rejects the *entire batch* over a single miss, so
    // fall back to the transport's URL, and drop the snapshot when even that
    // is absent rather than poison the batch it would ride in.
    snapshot.operationId ??= snapshot.attributes?.originalUrl;
    if (!snapshot.operationId) {
      if (this.options.debug) {
        this.logger.debug(
          `Snapshot for traceId "${snapshot.traceId}" has no operation id nor URL to fall back on. Ignoring snapshot.`,
        );
      }
      return;
    }

    if (!this._mainThreadBuffer) {
      this._mainThreadBuffer = this.createEmptyPayload();
    }

    const maxPerBatch =
      this.options.maxTracesPerBatch ?? DEFAULT_MAX_SNAPSHOTS_PER_TRANSACTION;
    if (this._mainThreadBuffer.snapshots.length >= maxPerBatch) {
      if (this.options.debug) {
        this.logger.debug(
          `Max snapshots per transaction reached: ${maxPerBatch}. Ignoring snapshot.`,
        );
      }
      return;
    }

    const encodedSnapshot: EncodedRequestSnapshot =
      RequestSnapshotEncoder.encode(snapshot);
    if (this.isDegraded() && !isNotableSnapshot(encodedSnapshot)) {
      // The collector is discarding these on arrival; shipping them costs the
      // application its own CPU and bandwidth for nothing. The snapshot still
      // goes - the request, its duration and its error are what the charts
      // are built from, and those are still being kept - and the count goes
      // with it, because it is the only record these existed.
      this.countTruncated(encodedSnapshot.t);
      delete encodedSnapshot.t;
    }
    this._mainThreadBuffer.snapshots.push(encodedSnapshot);

    // Read off the raw snapshot: the handler's span carries `Class.method`,
    // which is what `@Objective` declarations are keyed by. Declarations are
    // not telemetry, so they ride even a degraded batch - but a batch holds
    // only as many as the collector accepts, and one that did not fit is left
    // for a later request to bring due.
    const objectives = (this._mainThreadBuffer.objectives ??= []);
    if (objectives.length < MAX_OBJECTIVES_PER_BATCH) {
      const declaration = this.objectivesRegistry?.declarationFor(snapshot);
      if (declaration) {
        objectives.push(declaration);
      }
    }
    if (objectives.length === 0) {
      delete this._mainThreadBuffer.objectives;
    }
  }

  insertJobSnapshot(jobSnapshot: JobSnapshot) {
    if (!this._mainThreadBuffer) {
      this._mainThreadBuffer = this.createEmptyPayload();
    }
    if (!this._mainThreadBuffer.jobs) {
      this._mainThreadBuffer.jobs = [];
    }

    // Same cap as request snapshots, for the same reason: the buffer only
    // empties when a flush succeeds, so without a ceiling a stalled flush on
    // the worker service - the process that sees every queue job - grows this
    // array for as long as the stall lasts.
    const maxPerBatch =
      this.options.maxTracesPerBatch ?? DEFAULT_MAX_SNAPSHOTS_PER_TRANSACTION;
    if (this._mainThreadBuffer.jobs.length >= maxPerBatch) {
      if (this.options.debug) {
        this.logger.debug(
          `Max job snapshots per transaction reached: ${maxPerBatch}. Ignoring snapshot.`,
        );
      }
      return;
    }

    const encodedJobSnapshot: EncodedJobSnapshot =
      JobSnapshotEncoder.encode(jobSnapshot);
    if (this.isDegraded() && !isNotableSnapshot(encodedJobSnapshot)) {
      // A job carries no status code, so the same rule grades it on its error
      // and its duration - which is what the collector does with it too.
      this.countTruncated(encodedJobSnapshot.t);
      delete encodedJobSnapshot.t;
    }
    this._mainThreadBuffer.jobs.push(encodedJobSnapshot);
  }

  /**
   * Until when the collector's last answer says spans are being discarded.
   *
   * Zero means "not degraded", which is also where a fresh process starts:
   * the state is learned from a reply, so the first batch after start-up
   * always carries its spans. One batch of wasted trees is the price of not
   * persisting a guess across restarts.
   */
  private degradedUntil = 0;

  /**
   * Records what the collector said about the last batch.
   *
   * Refreshed on every reply rather than latched, so the window slides while
   * the account stays over its allowance and lapses on its own once it does
   * not - the collector has no way to announce the end of it, and an upgrade
   * must not need a restart to take effect.
   */
  setDegraded(degraded: boolean) {
    this.degradedUntil = degraded ? Date.now() + DEGRADED_TTL_MS : 0;
  }

  isDegraded(): boolean {
    return this.degradedUntil > Date.now();
  }

  /**
   * Adds a withheld tree to the batch's reported truncation.
   *
   * Every node, not every root: the collector meters a trace by its node
   * count, so a number that counted roots would under-report by the shape of
   * the tree and the account would be measured against a fraction of what it
   * actually produced.
   */
  private countTruncated(traces: unknown) {
    if (!this._mainThreadBuffer) {
      return;
    }
    this._mainThreadBuffer.truncatedSpans =
      (this._mainThreadBuffer.truncatedSpans ?? 0) + countTraceNodes(traces);
  }

  isBufferLocked() {
    return Atomics.load(this.lock, 0) !== 0;
  }

  isBufferEmpty() {
    return !this._mainThreadBuffer;
  }

  /**
   * Takes the lock, or reports that someone else holds it.
   *
   * One `compareExchange` rather than a read followed by a write: the worker
   * thread contends for this same word, so between a `load` saying the lock was
   * free and a `store` taking it, the worker can take it too - and both sides
   * then believe they hold it, one writing the buffer while the other reads it.
   * `compareExchange` only stores if the word is still what we read, and
   * returns what was there, so "it was free and it is now ours" is a single
   * indivisible step.
   */
  acquireLock() {
    if (Atomics.compareExchange(this.lock, 0, 0, 1) !== 0) {
      return false;
    }

    if (this.options.debug) {
      this.logger.debug("Shared buffer lock acquired.");
    }
    return true;
  }

  releaseLock() {
    Atomics.store(this.lock, 0, 0);
    Atomics.notify(this.lock, 0); // Notify any waiting threads

    if (this.options.debug) {
      this.logger.debug("Shared buffer lock released.");
    }
  }

  addNodeRuntimeMetrics(runtimeMetrics: NodeRuntimeMetrics | undefined) {
    if (!runtimeMetrics) {
      return;
    }
    if (!this._mainThreadBuffer) {
      this._mainThreadBuffer = this.createEmptyPayload();
    }
    this._mainThreadBuffer.runtime =
      RuntimeMetricsEncoder.encode(runtimeMetrics);
  }

  upsertCustomMetric(metric: Counter<any> | Gauge<any> | Summary<any>) {
    if (!this._mainThreadBuffer) {
      this._mainThreadBuffer = this.createEmptyPayload();
    }

    if (!this._mainThreadBuffer.custom) {
      this._mainThreadBuffer.custom = [];
    }

    if (metric instanceof Counter || metric instanceof Summary) {
      this._awaitingFlushAck.add(metric);
    }

    const existingMetricIndex = this._mainThreadBuffer.custom.findIndex(
      (m) => m.n === metric.name && m.t === metric.type,
    );
    if (existingMetricIndex !== -1) {
      this._mainThreadBuffer.custom[existingMetricIndex] = {
        ...this._mainThreadBuffer.custom[existingMetricIndex],
        ...CustomMetricsEncoder.encode(metric as CustomMetric),
      };
    } else {
      this._mainThreadBuffer.custom.push(
        CustomMetricsEncoder.encode(metric as CustomMetric),
      );
    }
  }

  /**
   * Buffers log entries, filling the current batch first and holding the rest
   * back for later batches. Oversized lines are truncated on the way in.
   *
   * The overflow is deferred rather than discarded: a burst that exceeds one
   * batch is the most interesting thing the logs will ever carry. Only once
   * the backlog is also full does anything get lost.
   *
   * When both are full the *newest* lines are dropped. During a burst the
   * opening lines are usually the ones that explain it; losing the tail of a
   * retry storm costs less than losing its first cause.
   *
   * Unlike the other insert methods this one stays silent when it drops. It is
   * called from the patched `process.stdout.write`, so a debug line here would
   * write to stdout, re-enter `pushLogs`, drop again, and log again - the cap
   * would turn every dropped entry into unbounded recursion.
   */
  pushLogs(logs: Array<BufferedLogEntry>) {
    if (!this._mainThreadBuffer) {
      this._mainThreadBuffer = this.createEmptyPayload();
    }
    if (!this._mainThreadBuffer.logs) {
      this._mainThreadBuffer.logs = [];
    }

    const buffered = this._mainThreadBuffer.logs;
    for (const log of logs) {
      // The `typeof` guard is not redundant with the type: entries arrive from
      // a patched `process.stdout.write`, where a caller passing something
      // unexpected must not crash the host - flush-time sanitization drops the
      // malformed entry instead.
      const entry =
        typeof log.text === "string" && log.text.length > MAX_LOG_ENTRY_LENGTH
          ? { ...log, text: this.truncateLogText(log.text) }
          : log;

      if (buffered.length < MAX_LOGS_PER_TRANSACTION) {
        buffered.push(entry);
        continue;
      }
      if (this._pendingLogs.length < MAX_PENDING_LOGS) {
        this._pendingLogs.push(entry);
        continue;
      }
      return;
    }
  }

  /**
   * Tops the outgoing batch up from the backlog.
   *
   * Called by the worker immediately before it decides whether there is
   * anything worth flushing - which is what stops a backlog stranding itself. A
   * burst leaves lines pending, the process then goes quiet, and with nothing
   * new arriving `pushLogs` would never run again to move them; the buffer
   * would read as empty and those lines would sit there until shutdown.
   *
   * Drained lines are appended after whatever the batch already holds, so a
   * batch is not ordered by time. Nothing downstream depends on that: every
   * entry carries its own timestamp, the recorder inserts by it, and the log
   * views sort on it.
   */
  drainPendingLogs() {
    if (this._pendingLogs.length === 0) {
      return;
    }

    if (!this._mainThreadBuffer) {
      this._mainThreadBuffer = this.createEmptyPayload();
    }
    if (!this._mainThreadBuffer.logs) {
      this._mainThreadBuffer.logs = [];
    }

    const capacity =
      MAX_LOGS_PER_TRANSACTION - this._mainThreadBuffer.logs.length;
    if (capacity <= 0) {
      return;
    }
    this._mainThreadBuffer.logs.push(
      ...this._pendingLogs.splice(0, Math.min(capacity, MAX_PENDING_LOGS)),
    );
  }

  private truncateLogText(text: string): string {
    return (
      text.slice(0, MAX_LOG_ENTRY_LENGTH - TRUNCATION_SUFFIX.length) +
      TRUNCATION_SUFFIX
    );
  }

  encodeAndWrite() {
    if (this.options.debug) {
      if (this._mainThreadBuffer?.snapshots) {
        this.logger.debug(
          `Encoding and writing ${this._mainThreadBuffer.snapshots.length} snapshots to shared buffer.`,
        );
        this.logger.debug(
          `Operations in shared buffer: ${this._mainThreadBuffer.snapshots.map(
            (s) => s.op,
          )}`,
        );
      }
    }

    const json = JSON.stringify(this._mainThreadBuffer);
    const jsonBytes = this.encoder.encode(json);
    const jsonLength = jsonBytes.length;

    if (jsonLength > SHARED_BUFFER_SIZE - 8) {
      // 8 bytes reserved for the lock and length
      throw new Error("Payload too large for shared buffer");
    }

    // Offset 4 for the lock value
    const view = new DataView(this._sharedBuffer, 4, 4);

    // Store the length of the JSON data
    view.setUint32(0, jsonLength);

    // Copy the JSON bytes into the shared buffer
    this.sharedBytes.set(jsonBytes, 8);

    // Only now is the payload genuinely handed off, so this is the point at
    // which a counter's reported increase, or a summary's window of
    // observations, can be considered delivered. Doing it any earlier (or in the
    // caller's finally block, which also runs when the write throws) would
    // silently drop a window's worth of measurements.
    for (const metric of this._awaitingFlushAck) {
      metric.markFlushed();
    }
    this._awaitingFlushAck.clear();
  }

  resetMainThreadBuffer() {
    this._mainThreadBuffer = null;
  }

  private createEmptyPayload(): AgentMetricsPayload {
    const payload = {
      serviceId: this.options.serviceId,
      snapshots: [],
      forwardLogs: Boolean(this.options.forwardLogs),
    } as AgentMetricsPayload;

    if (this.options.serviceVersion) {
      payload.serviceVersion = this.options.serviceVersion;
    }

    return payload;
  }
}
