import { ProcessorDecoratorService } from "@nestjs/bullmq";
import { AsyncLocalStorage } from "async_hooks";
import { OperationTraceRegistry } from "../services/operation-trace.registry.js";
import { QueueObserveAgentService } from "./queue-observe-agent.service.js";

const prototype = ProcessorDecoratorService.prototype as {
  decorate?: unknown;
};
const originalDecorate = prototype.decorate;

const createAgent = () =>
  new QueueObserveAgentService(
    {} as never,
    { traceIdKey: "traceId" } as never,
    {} as never,
    new AsyncLocalStorage<Map<string, any>>(),
  );

/** An agent whose runner can open and close a trace for real. */
const createTracingAgent = () => {
  const als = new AsyncLocalStorage<Map<string, any>>();
  return new QueueObserveAgentService(
    { insertJobSnapshot: vi.fn() } as never,
    { traceIdKey: "traceId" } as never,
    new OperationTraceRegistry(als as never, false),
    als,
  );
};

afterEach(() => {
  prototype.decorate = originalDecorate;
  vi.restoreAllMocks();
});

/**
 * `@nestjs/bullmq` is an optional peer: the agent must patch the processor
 * decorator when it is installed and stay inert - not crash the module - when
 * it is not. The loader is stubbed for the latter; the package is always
 * present in this repository's own dependencies.
 */
describe("QueueObserveAgentService", () => {
  it("patches the processor decorator when @nestjs/bullmq is installed", () => {
    createAgent();

    expect(prototype.decorate).toEqual(expect.any(Function));
    expect(prototype.decorate).not.toBe(originalDecorate);
  });

  it("stays inert when @nestjs/bullmq is not installed", () => {
    vi.spyOn(
      QueueObserveAgentService.prototype as unknown as {
        loadProcessorDecoratorService: () => unknown;
      },
      "loadProcessorDecoratorService",
    ).mockReturnValue(undefined);

    expect(() => createAgent()).not.toThrow();
    expect(prototype.decorate).toBe(originalDecorate);
  });

  it("hands the processor the lock token and abort signal the worker passed", async () => {
    createTracingAgent();
    const processor = vi.fn(async () => "done");
    const signal = new AbortController().signal;

    const decorated = (
      prototype.decorate as (
        processor: unknown,
      ) => (...args: unknown[]) => Promise<unknown>
    )(processor);
    const job = { queueName: "emails", name: "welcome", id: "1" };
    await decorated(job, "lock-token", signal);

    expect(processor).toHaveBeenCalledWith(job, "lock-token", signal);
  });
});
