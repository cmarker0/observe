import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "fs";
import { tmpdir } from "os";
import { dirname, join } from "path";
import { inferServiceVersion } from "./infer-service-version.util.js";

const SHA_A = "a".repeat(40);
const SHA_B = "b".repeat(40);
const SHA_C = "c".repeat(40);

/**
 * Checkouts are built by hand in a temporary directory rather than with git:
 * the SDK reads the files git writes, so the files are what these pin down,
 * and nothing here depends on git being installed.
 */
describe("inferServiceVersion", () => {
  let root: string;

  beforeEach(() => {
    root = mkdtempSync(join(tmpdir(), "observe-version-"));
  });

  afterEach(() => {
    rmSync(root, { recursive: true, force: true });
  });

  const write = (path: string, content: string) => {
    const file = join(root, path);
    mkdirSync(dirname(file), { recursive: true });
    writeFileSync(file, content);
  };

  /** No environment and no checkout: only what a test hands it. */
  const infer = (env: NodeJS.ProcessEnv = {}, ...dirs: string[]) =>
    inferServiceVersion(env, dirs.length ? dirs : [root]);

  describe("from the environment", () => {
    it("reads a platform's commit", () => {
      expect(infer({ VERCEL_GIT_COMMIT_SHA: SHA_A })).toEqual({
        version: SHA_A,
        source: "VERCEL_GIT_COMMIT_SHA",
      });
    });

    it("prefers OBSERVE_SERVICE_VERSION over anything a platform sets", () => {
      expect(
        infer({
          OBSERVE_SERVICE_VERSION: "2026.09.1",
          RENDER_GIT_COMMIT: SHA_A,
        })?.version,
      ).toBe("2026.09.1");
    });

    it("prefers a platform's commit over the CI job's", () => {
      expect(
        infer({ GITHUB_SHA: SHA_A, RAILWAY_GIT_COMMIT_SHA: SHA_B })?.version,
      ).toBe(SHA_B);
    });

    it("prefers the CI job's commit over a conventional name", () => {
      expect(infer({ GIT_SHA: SHA_A, CI_COMMIT_SHA: SHA_B })?.version).toBe(
        SHA_B,
      );
    });

    it("trims the value and skips blank ones", () => {
      expect(
        infer({ OBSERVE_SERVICE_VERSION: "  ", GIT_SHA: ` ${SHA_A}\n` }),
      ).toEqual({ version: SHA_A, source: "GIT_SHA" });
    });

    it("keeps apart long releases that differ only past the collector's limit", () => {
      const image = "123456789012.dkr.ecr.eu-west-1.amazonaws.com/orders-api";
      const release = (tag: string) =>
        infer({ OBSERVE_SERVICE_VERSION: `${image}:${tag}` })?.version;

      expect(release("2026.09.30-1")).toHaveLength(50);
      expect(release("2026.09.30-1")).not.toBe(release("2026.09.30-2"));
    });

    it("wins over the checkout", () => {
      write(".git/HEAD", `${SHA_A}\n`);
      expect(infer({ GITHUB_SHA: SHA_B })?.version).toBe(SHA_B);
    });
  });

  describe("from a checkout", () => {
    it("follows HEAD to the branch it names", () => {
      write(".git/HEAD", "ref: refs/heads/main\n");
      write(".git/refs/heads/main", `${SHA_A}\n`);

      expect(infer()).toEqual({ version: SHA_A, source: join(root, ".git") });
    });

    it("reads a detached HEAD directly", () => {
      write(".git/HEAD", `${SHA_B}\n`);
      expect(infer()?.version).toBe(SHA_B);
    });

    it("finds a branch that only exists in packed-refs", () => {
      write(".git/HEAD", "ref: refs/heads/release/1.x\n");
      write(
        ".git/packed-refs",
        [
          "# pack-refs with: peeled fully-peeled sorted",
          `${SHA_A} refs/heads/main`,
          `${SHA_B} refs/heads/release/1.x`,
          `^${SHA_C}`,
        ].join("\n"),
      );

      expect(infer()?.version).toBe(SHA_B);
    });

    it("prefers a loose ref over a stale packed one", () => {
      write(".git/HEAD", "ref: refs/heads/main\n");
      write(".git/refs/heads/main", SHA_B);
      write(".git/packed-refs", `${SHA_A} refs/heads/main\n`);

      expect(infer()?.version).toBe(SHA_B);
    });

    it("walks up from a nested directory", () => {
      write(".git/HEAD", `${SHA_A}\n`);
      mkdirSync(join(root, "apps/api/dist"), { recursive: true });

      expect(infer({}, join(root, "apps/api/dist"))?.version).toBe(SHA_A);
    });

    it("follows a worktree's .git file to its own HEAD and the shared branches", () => {
      write("repo/.git/refs/heads/feature", `${SHA_C}\n`);
      write("repo/.git/worktrees/feature/HEAD", "ref: refs/heads/feature\n");
      write("repo/.git/worktrees/feature/commondir", "../..\n");
      write(
        "feature/.git",
        `gitdir: ${join(root, "repo/.git/worktrees/feature")}\n`,
      );

      expect(infer({}, join(root, "feature"))?.version).toBe(SHA_C);
    });

    it("resolves a relative gitdir against the .git file's directory", () => {
      write("modules/app/HEAD", `${SHA_A}\n`);
      write("app/.git", "gitdir: ../modules/app\n");

      expect(infer({}, join(root, "app"))?.version).toBe(SHA_A);
    });

    it("stops at a repository with no commits instead of asking the one around it", () => {
      write(".git/HEAD", `${SHA_A}\n`);
      write("inner/.git/HEAD", "ref: refs/heads/main\n");

      expect(infer({}, join(root, "inner"))).toBeUndefined();
    });

    it("ignores a HEAD that does not hold a commit", () => {
      write(".git/HEAD", "ref: refs/heads/main\n");
      write(".git/refs/heads/main", "not a commit\n");

      expect(infer()).toBeUndefined();
    });

    it("tries the next directory when the first is not in a repository", () => {
      write("code/.git/HEAD", `${SHA_B}\n`);
      mkdirSync(join(root, "elsewhere"));

      expect(
        inferServiceVersion({}, [
          join(root, "elsewhere"),
          undefined,
          join(root, "code/dist"),
        ])?.version,
      ).toBe(SHA_B);
    });
  });

  describe("from a platform's revision", () => {
    it("is used only when no commit turned up", () => {
      expect(infer({ K_REVISION: "api-00042-xyz" })).toEqual({
        version: "api-00042-xyz",
        source: "K_REVISION",
      });

      write(".git/HEAD", `${SHA_A}\n`);
      expect(infer({ K_REVISION: "api-00042-xyz" })?.version).toBe(SHA_A);
    });

    it("keeps the end of a name longer than the collector stores", () => {
      const revision = `${"long-service-name-".repeat(3)}00042-xyz`;
      const version = infer({ CONTAINER_APP_REVISION: revision })?.version;

      expect(version).toHaveLength(50);
      expect(version?.endsWith("00042-xyz")).toBe(true);
    });
  });

  it("never infers a version longer than the collector accepts", () => {
    // A SHA-256 repository's commit id is 64 characters; the collector refuses
    // a whole batch whose version is over 50, so the head is sent instead.
    const sha256 = "d".repeat(64);
    write(".git/HEAD", `${sha256}\n`);
    expect(infer()?.version).toBe("d".repeat(50));
    expect(infer({ GIT_SHA: "x".repeat(80) })?.version).toHaveLength(50);
  });

  it("finds nothing when nothing names a release", () => {
    expect(infer()).toBeUndefined();
  });
});
