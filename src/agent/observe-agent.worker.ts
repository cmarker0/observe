import {
  Inject,
  Injectable,
  Logger,
  OnApplicationShutdown,
  OnModuleInit,
  Optional,
} from "@nestjs/common";
import { hostname } from "node:os";
import { Worker } from "worker_threads";
import { ObserveModuleOptionsWithDefaults } from "../interfaces/observe-options.interface.js";
import { OBSERVE_OPTIONS } from "../observe.constants.js";
import { CpuProfilerService } from "../profiling/cpu-profiler.service.js";
import { ProfileUploader } from "../profiling/profile-uploader.js";
import {
  setActiveSliceRecorder,
  SpanSliceRecorder,
} from "../profiling/span-slice-recorder.js";
import { OtelMetrics } from "../metrics/otel-metrics.js";
import { NodeRuntimeMetricsService } from "../services/node-runtime-metrics.service.js";
import { parseDegradedMessage } from "./degraded-ingest.protocol.js";
import {
  detachedWorkerData,
  detachedWorkerSource,
} from "./detached-observe-worker.assembly.js";
import { ObserveAgentSharedBuffer } from "./observe-agent.shared-buffer.js";

const DEFAULT_RUNTIME_METRICS_INTERVAL = 60000; // 60 seconds
const MIN_RUNTIME_METRICS_INTERVAL = 30000; // 30 seconds
const DEFAULT_FLUSH_INTERVAL = 5000; // 5 seconds

/**
 * Where signals are posted when `endpoint` is not configured.
 */
const DEFAULT_COLLECTOR_ENDPOINT =
  process.env.OBSERVE_ENDPOINT ?? "https://observe-api.nestjs.com";
const MIN_FLUSH_INTERVAL = 1000; // 1 second

/**
 * The credentials `nest new --observe` writes into `AppModule`.
 *
 * A project fresh from the generator has these until someone signs up, so
 * they mean "not connected yet", not "wrong": sending them only buys a 401
 * and an `Error:` line on the very first `npm run start`.
 */
const SCAFFOLD_APP_KEY = "YOUR_APP_KEY";
const SCAFFOLD_APP_SECRET = "YOUR_APP_SECRET";

const DEMO_URL = "https://www.observe-demo.nestjs.com/dashboard";
const SIGN_UP_URL = "https://observe.nestjs.com";

@Injectable()
export class ObserveAgentWorker implements OnModuleInit, OnApplicationShutdown {
  private readonly logger = new Logger(ObserveAgentWorker.name);
  // Null before `initializeWorker` and again after termination, which is what
  // the shutdown path below checks for.
  private worker: Worker | null = null;
  /**
   * Set while `onApplicationShutdown` is tearing the worker down.
   *
   * `Worker#terminate()` stops the thread abruptly, so Node reports exit code
   * 1 - indistinguishable, at the `exit` listener, from a worker that died on
   * its own. Without this flag a graceful shutdown logs an error and spawns a
   * replacement thread that nothing will ever stop, which keeps the event loop
   * alive and can hold the process open.
   */
  private isTerminating = false;
  private flushInterval: NodeJS.Timeout;
  private runtimeMetricsInterval: NodeJS.Timeout | null = null;
  private stopOtelRuntimeMetrics: (() => void) | null = null;
  private cpuProfiler: CpuProfilerService | null = null;

  constructor(
    private readonly observeAgentSharedBuffer: ObserveAgentSharedBuffer,
    @Inject(OBSERVE_OPTIONS)
    private readonly options: ObserveModuleOptionsWithDefaults,
    private readonly nodeRuntimeMetricsService: NodeRuntimeMetricsService,
    @Optional() private readonly otelMetrics?: OtelMetrics,
  ) {}

  onModuleInit() {
    if (this.options.opentelemetry) {
      // Read by the application's metric reader, not shipped from here.
      if (this.options.runtimeMetrics && this.otelMetrics) {
        this.stopOtelRuntimeMetrics = this.otelMetrics.startRuntime();
      }
      // Spans and metrics leave through the application's SDK. Forwarded
      // logs are all that still travel to the collector, so without them
      // there is no worker to start and no buffer to flush.
      if (!this.options.forwardLogs) {
        return;
      }
    }

    if (this.hasScaffoldCredentials) {
      // Nothing is sent, so there is no worker to start. The flush timer below
      // still runs and empties the buffer, which the instrumentation keeps
      // writing into either way.
      this.logger.log(
        `Observe is installed but not connected yet: appKey and appSecret are still the placeholders from nest new, so no telemetry is sent. ` +
          `See what this app will look like: ${DEMO_URL} - then create a free account at ${SIGN_UP_URL} for your own key and secret.`,
      );
    } else {
      this.initializeWorker();
    }

    let flushIntervalTime =
      this.options.flushInterval ?? DEFAULT_FLUSH_INTERVAL;
    if (flushIntervalTime < MIN_FLUSH_INTERVAL) {
      this.logger.warn(
        `Flush interval is too short (${flushIntervalTime}ms). Setting to minimum of ${MIN_FLUSH_INTERVAL}ms.`,
      );
      // Clamp the value the timer is armed with, not just the recorded option:
      // the warning above used to be a lie, and a `flushInterval: 50` flushed
      // twenty times a second while claiming it had been raised to 1s.
      flushIntervalTime = MIN_FLUSH_INTERVAL;
      this.options.flushInterval = MIN_FLUSH_INTERVAL;
    }
    this.flushInterval = setInterval(() => this.flush(), flushIntervalTime);

    if (!this.options.opentelemetry && !this.hasScaffoldCredentials) {
      this.startRuntimeMetrics();
    }

    // Continuous profiling is shelved: a flame graph asks more of a reader than
    // most services' owners want to give it, and the signal is not worth the
    // sampling cost until there is a UI that makes it legible. The
    // implementation is kept and tested - restore this call and re-register the
    // /profiles routes to bring it back.
    // this.startContinuousProfiling();
  }

  /**
   * Starts the runtime-metrics collector: memory, CPU, GC, event loop.
   */
  private startRuntimeMetrics(): void {
    if (!this.options.runtimeMetrics) {
      return;
    }

    let interval =
      this.options.runtimeMetricsInterval ?? DEFAULT_RUNTIME_METRICS_INTERVAL;

    if (interval < MIN_RUNTIME_METRICS_INTERVAL) {
      this.logger.warn(
        `Runtime metrics interval is too short (${interval}ms). Setting to minimum of ${MIN_RUNTIME_METRICS_INTERVAL}ms.`,
      );
      interval = MIN_RUNTIME_METRICS_INTERVAL;
    }

    this.runtimeMetricsInterval = setInterval(() => {
      this.observeAgentSharedBuffer.addNodeRuntimeMetrics(
        this.nodeRuntimeMetricsService.collectNodeRuntimeMetrics(),
      );
    }, interval);

    if (this.options.debug) {
      this.logger.debug(`Collecting runtime metrics every ${interval}ms.`);
    }
  }

  /**
   * Starts the sampling profiler, if the application asked for it.
   *
   * Distinct from `runtimeMetrics`, which gauges how loaded the process is.
   * This one samples stacks to say which code was running, and it is the only
   * one of the two with a cost on the hot path - so it stays off unless asked
   * for, rather than following any existing flag.
   *
   * Currently unreachable: `profiling` is commented out of `ObserveOptions` and
   * the call in `onModuleInit` is commented out with it. The body is kept
   * compiling rather than deleted, so restoring the feature is uncommenting two
   * lines instead of recovering this from history. The local type below is what
   * `ObserveOptions.profiling` declared, and is the thing to delete when the
   * real option comes back.
   */
  private startContinuousProfiling(): void {
    const profiling = (
      this.options as {
        profiling?: {
          enabled?: boolean;
          sampleRateHz?: number;
          windowMs?: number;
          dutyMs?: number;
          maxCpuPercent?: number;
        };
      }
    ).profiling;

    if (!profiling?.enabled) {
      return;
    }

    const slices = new SpanSliceRecorder();
    const uploader = new ProfileUploader({
      endpoint: this.endpoint,
      appKey: this.options.appKey,
      appSecret: this.options.appSecret,
      // One node is one process. Two replicas of a service must not merge into
      // a single tree, so the identity has to distinguish them.
      serviceNode: `${hostname()}:${process.pid}`,
      onError: (message) => this.logger.warn(message),
    });

    this.cpuProfiler = new CpuProfilerService(slices, {
      sampleRateHz: profiling.sampleRateHz,
      windowMs: profiling.windowMs,
      dutyMs: profiling.dutyMs,
      maxCpuPercent: profiling.maxCpuPercent,
      onWindow: (window, sampleRateHz) =>
        uploader.enqueue(window, sampleRateHz),
      onDisabled: (reason) => {
        // Said out loud rather than degraded quietly: the customer has asked
        // for profiling and is no longer getting it.
        this.logger.warn(`Continuous profiling stopped - ${reason}`);
        setActiveSliceRecorder(null);
      },
    });

    // Only now does the instrumentation start recording span slices. With
    // profiling off this stays null and every span boundary costs one null
    // check.
    setActiveSliceRecorder(slices);

    void this.cpuProfiler.start().catch((err) => {
      this.logger.warn(`Continuous profiling failed to start: ${String(err)}`);
      setActiveSliceRecorder(null);
    });

    this.logger.log("Continuous profiling enabled.");
  }

  async onApplicationShutdown() {
    if (this.flushInterval) {
      clearInterval(this.flushInterval);
    }

    if (this.runtimeMetricsInterval) {
      clearInterval(this.runtimeMetricsInterval);
    }
    this.stopOtelRuntimeMetrics?.();
    this.stopOtelRuntimeMetrics = null;

    if (this.cpuProfiler) {
      setActiveSliceRecorder(null);
      await this.cpuProfiler.stop();
      this.cpuProfiler = null;
    }

    if (!this.worker) {
      return;
    }

    this.isTerminating = true;
    await this.worker.terminate();

    if (this.options.debug) {
      this.logger.debug("Worker terminated successfully.");
    }
    this.worker = null;
  }

  private get hasScaffoldCredentials(): boolean {
    return (
      this.options.appKey === SCAFFOLD_APP_KEY &&
      this.options.appSecret === SCAFFOLD_APP_SECRET
    );
  }

  private get endpoint(): string {
    return this.options.endpoint ?? DEFAULT_COLLECTOR_ENDPOINT;
  }

  /**
   * Says out loud when credentials are about to cross the network unencrypted.
   *
   * `endpoint` is free-form and both halves of the agent attach `x-api-key` /
   * `x-api-secret` to every request against it. Over `http://` to anything but
   * the local machine, those headers are readable by every hop in between -
   * a misconfiguration worth one warning, not a refusal: private networks and
   * tunnels exist, and an agent that stops reporting over a scheme choice
   * punishes the operator harder than the mistake does.
   */
  private warnIfCredentialsSentInClear(): void {
    if (!this.options.appKey && !this.options.appSecret) {
      return;
    }
    try {
      const url = new URL(this.endpoint);
      const loopback =
        url.hostname === "localhost" ||
        url.hostname === "127.0.0.1" ||
        url.hostname === "[::1]" ||
        url.hostname === "::1";
      if (url.protocol === "http:" && !loopback) {
        this.logger.warn(
          `The collector endpoint "${url.origin}" uses plain HTTP: appKey and appSecret will be sent unencrypted. Use https:// unless the network path is fully trusted.`,
        );
      }
    } catch {
      // An unparsable endpoint fails loudly elsewhere; nothing to add here.
    }
  }

  initializeWorker() {
    this.warnIfCredentialsSentInClear();
    // Source and data both come from the assembly module, which the worker's
    // tests build from too: the worker is eval'd with no module scope, so the
    // code it needs travels as stringified functions and the configuration as
    // `workerData`, and the one place that puts them together is the one
    // place a forgotten value can be caught.
    this.worker = new Worker(detachedWorkerSource(), {
      eval: true,
      workerData: detachedWorkerData({
        sharedBuffer: this.observeAgentSharedBuffer.sharedBuffer,
        endpoint: this.endpoint,
        appKey: this.options.appKey,
        appSecret: this.options.appSecret,
      }),
    });

    this.worker.on("message", (msg) => this.handleMessage(msg));
    this.worker.on("error", (error) => this.handleError(error));
    this.worker.on("exit", (code) => {
      if (this.isTerminating) {
        this.logger.debug("Worker exited on shutdown.");
        return;
      }
      if (code !== 0) {
        this.logger.error(
          `Worker stopped with exit code ${code}. Restarting worker...`,
        );
        this.initializeWorker(); // Restart the worker
      } else {
        this.logger.debug("Worker exited gracefully.");
      }
    });

    this.logger.log("Observe agent worker initialized.");
  }

  handleMessage(msg: string) {
    const degraded = parseDegradedMessage(msg);
    if (degraded !== null) {
      this.applyDegraded(degraded);
      return;
    }

    if (msg.includes("Error:")) {
      this.logger.error(msg);
      return;
    }

    if (this.options.debug) {
      this.logger.debug(msg);
    }
  }

  /**
   * Whether the last reply said the collector is discarding spans.
   *
   * Kept beside the log rather than read from the buffer so the notice fires
   * once per transition: an account sits in this state for days, and a line
   * per flush would bury the one thing worth reading.
   */
  private reportedDegraded = false;

  /**
   * Stops sending trace trees the collector has told us it is throwing away,
   * and says so once.
   *
   * Said at `warn` rather than `debug`: the application is still being
   * observed, the waterfalls are not being kept, and a developer who finds
   * that out by opening a trace and seeing nothing has been let down by an
   * agent that knew.
   */
  private applyDegraded(degraded: boolean) {
    this.observeAgentSharedBuffer.setDegraded(degraded);

    if (degraded && !this.reportedDegraded) {
      this.logger.warn(
        "Observe: this account has spent its monthly event allowance. Requests, jobs " +
          "and errors are still being recorded; spans are not, so traces will show no " +
          "waterfall. The agent has stopped sending them. See /dashboard/billing.",
      );
    } else if (!degraded && this.reportedDegraded) {
      this.logger.log("Observe: spans are being recorded again.");
    }
    this.reportedDegraded = degraded;
  }

  handleError(error: Error) {
    this.logger.error(error);
  }

  flush() {
    // Before the emptiness check, not after: lines held back from an earlier
    // batch are the only thing that might need sending once the process has
    // gone quiet, and an empty buffer would otherwise return here and strand
    // them until shutdown.
    this.observeAgentSharedBuffer.drainPendingLogs();

    if (this.hasScaffoldCredentials) {
      this.observeAgentSharedBuffer.resetMainThreadBuffer();
      return;
    }

    if (this.observeAgentSharedBuffer.isBufferEmpty()) {
      return;
    }

    if (this.observeAgentSharedBuffer.isBufferLocked()) {
      if (this.options.debug) {
        this.logger.debug(
          "Shared buffer is locked, cannot flush data to worker. Skipping flush for now.",
        );
      }
      return;
    }

    if (!this.observeAgentSharedBuffer.acquireLock()) {
      if (this.options.debug) {
        this.logger.debug(
          "Failed to acquire lock on shared buffer. Skipping flush for now.",
        );
      }
      return;
    }

    try {
      this.observeAgentSharedBuffer.encodeAndWrite();
    } catch (error) {
      this.logger.error("Error during flush:", error);
    } finally {
      this.observeAgentSharedBuffer.releaseLock();
      this.observeAgentSharedBuffer.resetMainThreadBuffer();
    }
  }
}
