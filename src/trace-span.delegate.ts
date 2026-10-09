import type { SpanTagSink } from "./recorder/span-recorder.js";

export class TraceSpanDelegate {
  private readonly sink: SpanTagSink;

  get id(): string {
    return this.sink.id;
  }

  get name(): string | undefined {
    return this.sink.name;
  }

  constructor(sink: SpanTagSink);
  constructor(
    id: string,
    name: string | undefined,
    tags: Record<string, string | number | boolean>,
  );
  constructor(
    idOrSink: string | SpanTagSink,
    name?: string,
    tags?: Record<string, string | number | boolean>,
  ) {
    if (typeof idOrSink !== "string") {
      this.sink = idOrSink;
      return;
    }
    const target = tags ?? {};
    this.sink = {
      id: idOrSink,
      name,
      setTags: (update) => Object.assign(target, update),
    };
  }

  /**
   * Sets a tag on the trace span.
   * @param key - The key for the tag.
   * @param value - The value for the tag. This can be a string, number, or boolean.
   * @returns The current instance of the TraceSpanDelegate for method chaining.
   */
  setTag(key: string, value: string | number | boolean): TraceSpanDelegate {
    this.sink.setTags({ [key]: value });
    return this;
  }

  /**
   * Adds multiple tags to the trace span.
   * @param tags - An object containing key-value pairs for the tags.
   * @returns The current instance of the TraceSpanDelegate for method chaining.
   */
  addTags(tags: Record<string, string | number | boolean>): TraceSpanDelegate {
    this.sink.setTags(tags);
    return this;
  }

  /** @internal - what a recorder hands on in place of the delegate. */
  asSink(): SpanTagSink {
    return this.sink;
  }
}
