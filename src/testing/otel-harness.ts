import * as api from "@opentelemetry/api";
import { AsyncLocalStorageContextManager } from "@opentelemetry/context-async-hooks";
import { W3CTraceContextPropagator } from "@opentelemetry/core";
import {
  BasicTracerProvider,
  InMemorySpanExporter,
  ReadableSpan,
  SimpleSpanProcessor,
} from "@opentelemetry/sdk-trace-base";

/**
 * Shared plumbing for the suites that boot `createObserveModule({
 * opentelemetry })` and assert on the spans it produced, the counterpart of
 * `observe-harness.ts` for snapshots.
 *
 * Each suite gets its own provider and exporter, handed to the module as
 * `tracerProvider`, so suites never see each other's spans. The context
 * manager and propagator are global by nature: `installOtelGlobals` sets them
 * for a suite and `uninstallOtelGlobals` takes them down again.
 */
export class OtelTestSpans {
  readonly exporter = new InMemorySpanExporter();
  readonly provider = new BasicTracerProvider({
    spanProcessors: [new SimpleSpanProcessor(this.exporter)],
  });

  get finished(): ReadableSpan[] {
    return this.exporter.getFinishedSpans();
  }

  reset(): void {
    this.exporter.reset();
  }

  /**
   * The finished spans of the trace whose span is named `name`, once that
   * span has ended. Waits, since a root ends after the response is written.
   */
  async traceOf(name: string, timeoutMs = 3000): Promise<ReadableSpan[]> {
    const found = await this.waitFor(
      (spans) => spans.find((span) => span.name === name),
      timeoutMs,
      `a "${name}" span`,
    );
    const { traceId } = found.spanContext();
    return this.finished.filter(
      (span) => span.spanContext().traceId === traceId,
    );
  }

  /** Polls the finished spans until `pick` finds something. */
  async waitFor<T>(
    pick: (spans: ReadableSpan[]) => T | undefined,
    timeoutMs = 3000,
    what = "the expected spans",
  ): Promise<T> {
    const deadline = Date.now() + timeoutMs;
    for (;;) {
      const picked = pick(this.finished);
      if (picked !== undefined) {
        return picked;
      }
      if (Date.now() > deadline) {
        throw new Error(
          `Timed out waiting for ${what}. Finished: ${this.finished
            .map((span) => span.name)
            .join(", ")}`,
        );
      }
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
  }
}

export function installOtelGlobals(): void {
  api.context.setGlobalContextManager(
    new AsyncLocalStorageContextManager().enable(),
  );
  api.propagation.setGlobalPropagator(new W3CTraceContextPropagator());
}

export function uninstallOtelGlobals(): void {
  api.context.disable();
  api.propagation.disable();
}

export const spanIdOf = (span: ReadableSpan) => span.spanContext().spanId;
export const parentIdOf = (span: ReadableSpan) =>
  span.parentSpanContext?.spanId;

/** The one span named `name`, failing the test when there is none. */
export function spanNamed(spans: ReadableSpan[], name: string): ReadableSpan {
  const found = spans.find((span) => span.name === name);
  if (!found) {
    throw new Error(
      `No "${name}" span among: ${spans.map((span) => span.name).join(", ")}`,
    );
  }
  return found;
}

/**
 * `spans` drawn as an indented tree, one `KIND name` line per span, children
 * in start order. A span whose parent is not among `spans` is a root, so the
 * tree of one trace's spans reads top-down from the operation.
 *
 * Asserting on the drawing pins the whole shape at once - which span sits
 * under which, and of what kind - where per-span parent checks miss a span
 * that turned up somewhere unexpected.
 */
export function spanTree(spans: ReadableSpan[]): string {
  const ids = new Set(spans.map(spanIdOf));
  const children = new Map<string | undefined, ReadableSpan[]>();
  for (const span of spans) {
    const parent = parentIdOf(span);
    const key = parent !== undefined && ids.has(parent) ? parent : undefined;
    children.set(key, [...(children.get(key) ?? []), span]);
  }
  const lines: string[] = [];
  const draw = (parent: string | undefined, depth: number) => {
    const ordered = [...(children.get(parent) ?? [])].sort(
      (a, b) => toNanos(a.startTime) - toNanos(b.startTime),
    );
    for (const span of ordered) {
      lines.push(`${"  ".repeat(depth)}${api.SpanKind[span.kind]} ${span.name}`);
      draw(spanIdOf(span), depth + 1);
    }
  };
  draw(undefined, 0);
  return lines.join("\n");
}

function toNanos([seconds, nanos]: api.HrTime): number {
  return seconds * 1e9 + nanos;
}
