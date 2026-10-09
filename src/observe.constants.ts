export const OBSERVE_OPTIONS = "OBSERVE_OPTIONS";
export const CALLER_METADATA_KEY = "#caller";

/**
 * Where a store keeps the registry key when it differs from the trace id.
 *
 * A queued job reports under the trace id of the operation that enqueued it,
 * and that operation may still be open in this very process - an API that
 * also runs its own workers. The registry is keyed per execution, so the job
 * gets a key of its own here while `traceIdKey` keeps the id everything
 * user-facing reads: logs, `currentTraceId()`, the snapshot.
 */
export const TRACE_REGISTRY_KEY = "#registryKey";

/**
 * The job option the enqueuing side stamps the active trace id into. Both
 * BullMQ and Bull persist unknown options verbatim, so it survives the trip
 * through Redis without touching the job's data.
 */
export const JOB_TRACE_OPTION_KEY = "observeTraceId";

/**
 * The job option the enqueuing span's propagation fields (W3C `traceparent`
 * and the like) are stamped into, when recording through OpenTelemetry. A
 * run links to that span. Kept beside `JOB_TRACE_OPTION_KEY` rather than in
 * place of it: the id still correlates the run's log lines.
 */
export const JOB_TRACE_CONTEXT_OPTION_KEY = "observeTraceContext";
