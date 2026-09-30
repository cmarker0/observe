/** Where `@Objective` keeps what a handler declares. */
export const OBJECTIVES_METADATA = "observe:objectives";

/**
 * A promise one route makes, stated next to the code that has to keep it.
 *
 * Declare either promise, or both; each becomes its own SLO in Observe, which
 * then tracks its error budget, alerts on its burn rate, and checks releases
 * against it - exactly as if it had been made in the dashboard, except that
 * the code owns it: change it here and the next deploy updates the SLO.
 */
export interface ObjectiveOptions {
  /**
   * Percent of requests that must not fail with an unhandled error - `99.9`
   * for three nines. Between 90 and 99.999.
   */
  availability?: number;
  /**
   * Percent of requests (`target`) that must finish under `underMs`:
   * `{ underMs: 300, target: 99 }` promises 99% under 300ms.
   */
  latency?: { underMs: number; target: number };
  /** The rolling window the promise is judged over, in days. Default 28. */
  windowDays?: 7 | 14 | 28 | 30;
  /** What the dashboard calls it. Defaults to the route and the promise. */
  name?: string;
}

/**
 * Declares an objective for this route.
 *
 * ```ts
 * @Post()
 * @Objective({ availability: 99.9, latency: { underMs: 300, target: 99 } })
 * create(@Body() dto: CreateOrderDto) {}
 * ```
 *
 * Nothing is sent at boot. The agent pairs the declaration with the route the
 * handler actually serves on its first request - so a global prefix,
 * versioning and RouterModule paths are always right - and reports it with the
 * next batch. Applied more than once, every declaration counts.
 */
export function Objective(options: ObjectiveOptions): MethodDecorator {
  return (_target, _key, descriptor: PropertyDescriptor) => {
    const declared: ObjectiveOptions[] =
      Reflect.getMetadata(OBJECTIVES_METADATA, descriptor.value) ?? [];
    // Decorators apply bottom-up; prepending keeps them in the order written.
    Reflect.defineMetadata(
      OBJECTIVES_METADATA,
      [options, ...declared],
      descriptor.value,
    );
    return descriptor;
  };
}

/** The collector's own bounds, so a bad declaration is caught at boot. */
const MIN_TARGET = 90;
const MAX_TARGET = 99.999;
const WINDOW_DAYS = [7, 14, 28, 30];

const isTarget = (value: unknown) =>
  typeof value === "number" && value >= MIN_TARGET && value <= MAX_TARGET;

/**
 * Why a declaration cannot be sent, or nothing. Mirrors what the collector
 * accepts: a batch carrying a declaration it refuses is refused whole, so a
 * bad one has to be dropped - loudly - before it can cost any telemetry.
 */
export function objectiveProblems(options: ObjectiveOptions): string[] {
  const problems: string[] = [];
  if (options.availability === undefined && options.latency === undefined) {
    problems.push("declares neither availability nor latency");
  }
  if (options.availability !== undefined && !isTarget(options.availability)) {
    problems.push(
      `availability must be a percentage from ${MIN_TARGET} to ${MAX_TARGET}`,
    );
  }
  if (options.latency !== undefined) {
    const { underMs, target } = options.latency;
    if (!Number.isInteger(underMs) || underMs < 1 || underMs > 600_000) {
      problems.push("latency.underMs must be whole milliseconds, up to 600000");
    }
    if (!isTarget(target)) {
      problems.push(
        `latency.target must be a percentage from ${MIN_TARGET} to ${MAX_TARGET}`,
      );
    }
  }
  if (
    options.windowDays !== undefined &&
    !WINDOW_DAYS.includes(options.windowDays)
  ) {
    problems.push(`windowDays must be one of ${WINDOW_DAYS.join(", ")}`);
  }
  if (options.name !== undefined && options.name.length > 120) {
    problems.push("name must be at most 120 characters");
  }
  return problems;
}

const OBJECTIVE_FIELDS = new Set([
  "availability",
  "latency",
  "windowDays",
  "name",
]);
const LATENCY_FIELDS = new Set(["underMs", "target"]);

/**
 * Fields an objective carries that the collector does not declare. TypeScript
 * catches them in an object literal, not in options built elsewhere - and the
 * collector refuses a batch with one whole, so they are left out rather than
 * sent.
 */
export function unknownObjectiveFields(options: ObjectiveOptions): string[] {
  const unknown = Object.keys(options).filter(
    (field) => !OBJECTIVE_FIELDS.has(field),
  );
  if (options.latency && typeof options.latency === "object") {
    unknown.push(
      ...Object.keys(options.latency)
        .filter((field) => !LATENCY_FIELDS.has(field))
        .map((field) => `latency.${field}`),
    );
  }
  return unknown;
}

/** An objective as it is sent: the fields the collector declares, and no others. */
export function toDeclaredObjective(
  options: ObjectiveOptions,
): ObjectiveOptions {
  return {
    ...(options.availability === undefined
      ? {}
      : { availability: options.availability }),
    ...(options.latency === undefined
      ? {}
      : {
          latency: {
            underMs: options.latency.underMs,
            target: options.latency.target,
          },
        }),
    ...(options.windowDays === undefined
      ? {}
      : { windowDays: options.windowDays }),
    ...(options.name === undefined ? {} : { name: options.name }),
  };
}
