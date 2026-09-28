import { Logger } from "@nestjs/common";
import { AsyncLocalStorage } from "async_hooks";
import { ObserveAgentSharedBuffer } from "../agent/observe-agent.shared-buffer.js";
import { ObserveModuleOptionsWithDefaults } from "../interfaces/index.js";
import { TRACE_REGISTRY_KEY } from "../observe.constants.js";
import { OperationTraceRegistry } from "../services/operation-trace.registry.js";
import { JobRunDescriptor, JobTraceRunner } from "./job-trace-runner.js";

/**
 * `jobs.ignore`, below every driver: BullMQ, Bull and `@nestjs/schedule` all
 * start their runs here, so this is the one place the option is decided - and
 * the queue drivers' own suites need a Redis this one does not.
 */
describe("JobTraceRunner: jobs.ignore", () => {
  const TRACE_ID_KEY = "traceId";

  let als: AsyncLocalStorage<Map<string, unknown>>;
  let registry: OperationTraceRegistry;
  let insertJobSnapshot: ReturnType<typeof vi.fn>;
  let warn: ReturnType<typeof vi.fn>;

  const createRunner = (jobs: ObserveModuleOptionsWithDefaults["jobs"]) =>
    new JobTraceRunner(
      { insertJobSnapshot } as unknown as ObserveAgentSharedBuffer,
      { traceIdKey: TRACE_ID_KEY, jobs } as ObserveModuleOptionsWithDefaults,
      registry,
      als as never,
      { warn, debug: vi.fn() } as unknown as Logger,
    );

  const job: JobRunDescriptor = {
    queueName: "emails",
    name: "heartbeat",
    id: 42,
    metadata: {},
  };

  /** What the handler saw of its own run. */
  const observeRun = () => {
    const store = als.getStore()!;
    const registryKey = store.get(TRACE_REGISTRY_KEY) as string;
    return {
      traceId: store.get(TRACE_ID_KEY),
      traced: registry.hasTrace(registryKey),
    };
  };

  beforeEach(() => {
    als = new AsyncLocalStorage();
    registry = new OperationTraceRegistry(als as never, false);
    insertJobSnapshot = vi.fn();
    warn = vi.fn();
  });

  it("runs a matched job under a trace id of its own without opening a trace", async () => {
    const runner = createRunner({ ignore: (run) => run.name === "heartbeat" });

    const seen = await runner.run(job, async () => observeRun());
    await new Promise((resolve) => setTimeout(resolve, 5));

    expect(seen.traceId).toEqual(expect.any(String));
    expect(seen.traced).toBe(false);
    expect(insertJobSnapshot).not.toHaveBeenCalled();
  });

  it("traces a job the predicate does not match", () => {
    const runner = createRunner({ ignore: (run) => run.name === "other" });

    expect(runner.run(job, () => observeRun()).traced).toBe(true);
  });

  it("hands the predicate the id as a string, as setAttributes gets it", () => {
    const ignore = vi.fn(() => true);
    createRunner({ ignore }).run(job, () => undefined);

    expect(ignore).toHaveBeenCalledWith({
      queueName: "emails",
      name: "heartbeat",
      id: "42",
    });
  });

  it("leaves a callback-style handler to finish through its own callback", () => {
    const runner = createRunner({ ignore: () => true });
    const done = vi.fn();

    runner.run(
      job,
      (settle) => {
        settle("completed");
        done();
      },
      true,
    );

    expect(done).toHaveBeenCalledOnce();
  });

  it("still runs, and traces, a job whose predicate throws", () => {
    const runner = createRunner({
      ignore: () => {
        throw new Error("bad predicate");
      },
    });

    const seen = runner.run(job, () => observeRun());

    expect(seen.traced).toBe(true);
    expect(warn).toHaveBeenCalledWith(expect.stringContaining("bad predicate"));
  });
});

/**
 * Two layers of instrumentation around one run: a BullMQ Pro worker, patched
 * where it calls the processor, running a processor `@nestjs/bullmq` decorated
 * as well. The inner layer has to join the run the outer one opened.
 */
describe("JobTraceRunner: a run nested in its own job's run", () => {
  const TRACE_ID_KEY = "traceId";

  let als: AsyncLocalStorage<Map<string, unknown>>;
  let registry: OperationTraceRegistry;
  let runner: JobTraceRunner<Record<string, unknown>>;

  const job = (id: string | number): JobRunDescriptor => ({
    queueName: "emails",
    name: "welcome",
    id,
    metadata: {},
  });

  const observeRun = () => {
    const store = als.getStore()!;
    return {
      traceId: store.get(TRACE_ID_KEY),
      registryKey: store.get(TRACE_REGISTRY_KEY) as string | undefined,
    };
  };

  beforeEach(() => {
    als = new AsyncLocalStorage();
    registry = new OperationTraceRegistry(als as never, false);
    runner = new JobTraceRunner(
      { insertJobSnapshot: vi.fn() } as unknown as ObserveAgentSharedBuffer,
      { traceIdKey: TRACE_ID_KEY } as ObserveModuleOptionsWithDefaults,
      registry,
      als as never,
      { warn: vi.fn(), debug: vi.fn() } as unknown as Logger,
    );
  });

  it("runs inside the trace the outer layer opened, rather than hiding it", () => {
    const [outer, inner] = runner.run(job("7"), () => [
      observeRun(),
      runner.run(job("7"), () => observeRun()),
    ]);

    expect(inner).toEqual(outer);
    expect(registry.hasTrace(inner.registryKey!)).toBe(true);
  });

  it("recognises the job by queue and id, whatever type the id arrives as", () => {
    const [outer, inner] = runner.run(job(7), () => [
      observeRun(),
      runner.run(job("7"), () => observeRun()),
    ]);

    expect(inner).toEqual(outer);
  });

  it("still isolates a different job that happens to start in that context", () => {
    const inner = runner.run(job("7"), () =>
      runner.run(job("8"), () => observeRun()),
    );

    expect(inner).toEqual({ traceId: undefined, registryKey: undefined });
  });
});

describe("JobTraceRunner: patchEnqueue", () => {
  const TRACE_ID_KEY = "traceId";

  let als: AsyncLocalStorage<Map<string, unknown>>;
  let runner: JobTraceRunner<Record<string, unknown>>;

  beforeEach(() => {
    als = new AsyncLocalStorage();
    runner = new JobTraceRunner(
      {} as ObserveAgentSharedBuffer,
      { traceIdKey: TRACE_ID_KEY } as ObserveModuleOptionsWithDefaults,
      {} as OperationTraceRegistry,
      als as never,
      { warn: vi.fn(), debug: vi.fn() } as unknown as Logger,
    );
  });

  const inTrace = <T>(fn: () => T) =>
    als.run(new Map([[TRACE_ID_KEY, "trace-1"]]), fn);

  it("wraps a subclass's own add, not the original its parent parked", () => {
    const baseAdd = vi.fn();
    const proAdd = vi.fn();
    class Queue {
      add(...args: unknown[]) {
        return baseAdd(...args);
      }
    }
    // Overrides `add` without calling `super.add`, as `QueuePro` may.
    class QueuePro extends Queue {
      override add(...args: unknown[]) {
        return proAdd(...args);
      }
    }
    runner.patchEnqueue(Queue.prototype as never, () => 2);
    runner.patchEnqueue(QueuePro.prototype as never, () => 2);

    inTrace(() => new QueuePro().add("welcome", {}, { attempts: 3 }));

    expect(baseAdd).not.toHaveBeenCalled();
    expect(proAdd).toHaveBeenCalledWith(
      "welcome",
      {},
      { attempts: 3, observeTraceId: "trace-1" },
    );
  });

  it("stamps once when a subclass's add goes through super.add", () => {
    const baseAdd = vi.fn();
    class Queue {
      add(...args: unknown[]) {
        return baseAdd(...args);
      }
    }
    class QueuePro extends Queue {
      override add(...args: unknown[]) {
        return super.add(...args);
      }
    }
    runner.patchEnqueue(Queue.prototype as never, () => 2);
    runner.patchEnqueue(QueuePro.prototype as never, () => 2);

    inTrace(() => new QueuePro().add("welcome", {}));

    expect(baseAdd).toHaveBeenCalledOnce();
    expect(baseAdd).toHaveBeenCalledWith(
      "welcome",
      {},
      { observeTraceId: "trace-1" },
    );
  });

  it("replaces its own wrapper when patched again, rather than nesting it", () => {
    const add = vi.fn();
    class Queue {
      add(...args: unknown[]) {
        return add(...args);
      }
    }
    runner.patchEnqueue(Queue.prototype as never, () => 2);
    runner.patchEnqueue(Queue.prototype as never, () => 2);

    inTrace(() => new Queue().add("welcome", {}));

    expect(add).toHaveBeenCalledOnce();
  });
});
