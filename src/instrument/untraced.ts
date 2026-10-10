/**
 * Set on a function the agent hands to a provider, so the instance decorator
 * leaves it alone when the provider keeps it in a field and calls it as a
 * method: the agent's own bookkeeping is not a step of the application.
 */
const UNTRACED = Symbol.for("@nestjs/observe:untraced");

export function markUntraced<Fn extends (...args: never[]) => unknown>(
  fn: Fn,
): Fn {
  return Object.assign(fn, { [UNTRACED]: true });
}

export function isMarkedUntraced(value: unknown): boolean {
  return (
    typeof value === "function" &&
    (value as { [UNTRACED]?: boolean })[UNTRACED] === true
  );
}
