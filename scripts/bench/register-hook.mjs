// Lets OpenTelemetry instrumentations patch ES modules (Nest 12 is one).
import { register } from "node:module";

register("@opentelemetry/instrumentation/hook.mjs", import.meta.url);
