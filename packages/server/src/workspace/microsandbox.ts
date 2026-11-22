import path from "node:path"
import { constants } from "node:os"
import type { EnvironmentDriver } from "@opencode/core/environment/driver"
import { WorkspaceDriver } from "@opencode/core/workspace/driver"
import { Cause, Effect, Queue, Redacted, Schema, Sink, Stream } from "effect"
import { type PlatformError, systemError } from "effect/PlatformError"
import type { Command, CommandInput, CommandOptions, KillOptions, Signal } from "effect/process/ChildProcess"
import { ExitCode, make, makeHandle, ProcessId } from "effect/process/ChildProcessSpawner"
import type { ExecHandle, Sandbox, SandboxBuilder, SandboxHandle } from "microsandbox"

export const provider = "microsandbox"

export type Sdk = Pick<typeof import("microsandbox"), "Sandbox" | "SandboxNotFoundError" | "Rule" | "Destination">

export interface Mount {
  readonly host: string
  readonly guest: string
  readonly readonly: boolean
}

export type Network =
  | { readonly type: "all" }
  | { readonly type: "none" }
  | { readonly type: "hosts"; readonly hosts: ReadonlyArray<string> }

export interface Options {
  readonly image: string
  /** A script run once in each new machine, before the workspace is ready. */
  readonly setup?: string
  readonly mounts: ReadonlyArray<Mount>
  readonly network: Network
  readonly shell: string
  readonly cpus?: number
  readonly memoryMiB?: number
}

export const Binding = Schema.Struct({ name: Schema.String, id: Schema.String })
export type Binding = typeof Binding.Type

export const defaultImage = "ubuntu:24.04"

/**
 * The workspace contract needs GNU coreutils and findutils (Ubuntu has them), ripgrep for the search tools, setsid
 * from util-linux to kill a command's process group, and git for the agent.
 */
export const defaultSetup = [
  "command -v rg >/dev/null && command -v git >/dev/null && exit 0",
  "export DEBIAN_FRONTEND=noninteractive",
  "apt-get update",
  "apt-get install -y --no-install-recommends ca-certificates git ripgrep",
].join("\n")

const STOP_TIMEOUT_MS = 30_000

/**
 * Reads the driver's options from `OPENCODE_MICROSANDBOX_*` variables. Returns the problem as text when a variable is
 * malformed, so the server still starts and only this provider fails.
 */
export const fromEnv = (env: Readonly<Record<string, string | undefined>>): Options | string => {
  const image = env.OPENCODE_MICROSANDBOX_IMAGE || defaultImage
  const mounts = (env.OPENCODE_MICROSANDBOX_MOUNTS ?? "")
    .split(",")
    .map((entry) => entry.trim())
    .filter((entry) => entry.length > 0)
    .map(parseMount)
  const invalidMount = mounts.find((mount) => typeof mount === "string")
  if (typeof invalidMount === "string") return invalidMount
  const cpus = parseCount("OPENCODE_MICROSANDBOX_CPUS", env.OPENCODE_MICROSANDBOX_CPUS)
  if (typeof cpus === "string") return cpus
  const memoryMiB = parseCount("OPENCODE_MICROSANDBOX_MEMORY", env.OPENCODE_MICROSANDBOX_MEMORY)
  if (typeof memoryMiB === "string") return memoryMiB
  return {
    image,
    setup: env.OPENCODE_MICROSANDBOX_SETUP ?? (image === defaultImage ? defaultSetup : undefined),
    mounts: mounts.filter((mount): mount is Mount => typeof mount !== "string"),
    network: parseNetwork(env.OPENCODE_MICROSANDBOX_NETWORK),
    shell: env.OPENCODE_MICROSANDBOX_SHELL || "/bin/bash",
    cpus,
    memoryMiB,
  }
}

/** `host`, `host:guest`, `host:ro` or `host:guest:ro`. The guest path defaults to the host path. */
const parseMount = (entry: string): Mount | string => {
  const parts = entry.split(":")
  const readonly = parts.at(-1) === "ro"
  const [host, guest = host, ...rest] = readonly ? parts.slice(0, -1) : parts
  if (rest.length > 0 || !host || !path.posix.isAbsolute(host) || !path.posix.isAbsolute(guest))
    return `OPENCODE_MICROSANDBOX_MOUNTS: "${entry}" is not host[:guest][:ro] with absolute paths`
  return { host, guest, readonly }
}

const parseCount = (name: string, value: string | undefined): number | undefined | string => {
  if (value === undefined || value === "") return undefined
  const count = Number(value)
  return Number.isInteger(count) && count > 0 ? count : `${name}: "${value}" is not a positive integer`
}

const parseNetwork = (value: string | undefined): Network => {
  if (value === undefined || value === "" || value === "all") return { type: "all" }
  if (value === "none") return { type: "none" }
  return {
    type: "hosts",
    hosts: value
      .split(",")
      .map((host) => host.trim())
      .filter((host) => host.length > 0),
  }
}

export const sandboxName = (workspaceID: string) => `opencode-${workspaceID}`

export interface DriverInput {
  readonly options: Options | string
  /** Loads the SDK. Tests pass a fake; the real one loads its native addon only when a workspace is first used. */
  readonly load?: () => Promise<Sdk>
}

export const driver = (input: DriverInput): WorkspaceDriver.Interface => {
  let loading: Promise<Sdk> | undefined
  const sdk = attempt(() => (loading ??= (input.load ?? (() => import("microsandbox")))()))
  const options =
    typeof input.options === "string"
      ? Effect.fail(new WorkspaceDriver.Error({ message: input.options }))
      : Effect.succeed(input.options)
  const decodeBinding = Schema.decodeUnknownOption(Binding)

  const find = (microsandbox: Sdk, name: string) =>
    attempt(() =>
      microsandbox.Sandbox.get(name).catch((error: unknown): SandboxHandle | undefined => {
        if (error instanceof microsandbox.SandboxNotFoundError) return undefined
        throw error
      }),
    )

  // connectOrCreate adopts a machine an earlier attempt made, so a retried create never makes a second one.
  const boot = Effect.fnUntraced(function* (microsandbox: Sdk, workspaceID: string) {
    const config = yield* options
    const sandbox = yield* attempt(() => builder(microsandbox, config, workspaceID).connectOrCreate())
    if (config.setup) yield* runSetup(sandbox, config.setup)
    return sandbox
  })

  return WorkspaceDriver.make({
    shell: typeof input.options === "string" ? undefined : input.options.shell,
    create: ({ workspaceID }) =>
      Effect.gen(function* () {
        const microsandbox = yield* sdk
        const sandbox = yield* boot(microsandbox, workspaceID)
        const binding = { name: sandbox.name, id: sandbox.id }
        yield* attempt(() => sandbox.detach())
        return { binding }
      }),
    connect: ({ workspaceID, binding, saveBinding }) =>
      Effect.gen(function* () {
        const config = yield* options
        const microsandbox = yield* sdk
        const name = sandboxName(workspaceID)
        const handle = yield* find(microsandbox, name)
        // A machine removed outside opencode is made again, like a first create.
        const sandbox = handle
          ? yield* attempt(() => handle.connectOrStart({ detached: true }))
          : yield* boot(microsandbox, workspaceID)
        const known = decodeBinding(binding)
        if (known._tag === "None" || known.value.id !== sandbox.id)
          yield* saveBinding({ name: sandbox.name, id: sandbox.id })
        yield* Effect.addFinalizer(() => attempt(() => sandbox.detach()).pipe(Effect.ignore))
        return makeDriver(sandbox, config.shell)
      }),
    suspendForIdle: ({ workspaceID }) =>
      Effect.gen(function* () {
        const microsandbox = yield* sdk
        const handle = yield* find(microsandbox, sandboxName(workspaceID))
        if (!handle || handle.status !== "running") return
        yield* attempt(() => handle.stopWithTimeout(STOP_TIMEOUT_MS)).pipe(
          Effect.catch(() => attempt(() => handle.kill())),
        )
      }),
    destroy: ({ workspaceID }) =>
      Effect.gen(function* () {
        const microsandbox = yield* sdk
        const handle = yield* find(microsandbox, sandboxName(workspaceID))
        if (!handle) return
        yield* attempt(() => handle.destroy({ force: true, timeoutMs: STOP_TIMEOUT_MS }))
      }),
  })
}

const builder = (microsandbox: Sdk, config: Options, workspaceID: string): SandboxBuilder => {
  const base = microsandbox.Sandbox.builder(sandboxName(workspaceID))
    .image(config.image)
    .detached(true)
    .label("opencode.workspace", workspaceID)
  const sized = config.memoryMiB ? base.memory(config.memoryMiB) : base
  const withCpus = config.cpus ? sized.cpus(config.cpus) : sized
  const mounted = config.mounts.reduce(
    (current, mount) =>
      current.volume(mount.guest, (volume) =>
        mount.readonly ? volume.bind(mount.host).readonly() : volume.bind(mount.host),
      ),
    withCpus,
  )
  if (config.network.type === "all") return mounted
  if (config.network.type === "none") return mounted.disableNetwork()
  const policy = {
    defaultEgress: "deny" as const,
    defaultIngress: "allow" as const,
    rules: [
      microsandbox.Rule.allowDns(),
      ...config.network.hosts.map((host) => microsandbox.Rule.allowEgress(microsandbox.Destination.domain(host))),
    ],
  }
  return mounted.network((network) => network.policy(policy))
}

const runSetup = (sandbox: Sandbox, script: string) =>
  Effect.gen(function* () {
    const output = yield* attempt(() => sandbox.execWith("sh", (exec) => exec.args(["-c", script]).stdinNull()))
    if (output.success) return
    return yield* new WorkspaceDriver.Error({
      message: `microsandbox setup exited ${output.code}: ${output.stderr().trim().slice(-2000)}`,
    })
  })

const attempt = <A>(run: () => Promise<A>) =>
  Effect.tryPromise({
    try: run,
    catch: (cause) =>
      new WorkspaceDriver.Error({ message: cause instanceof Error ? cause.message : String(cause), cause }),
  })

/**
 * Records the pid of the command's process group leader, so a kill can reach every process it started. `isolated`
 * replaces the environment, like a spawn with `env` and no `extendEnv`, but keeps the image's PATH and HOME: the
 * server's own values mean nothing in the machine.
 */
const WRAPPER = `
pidfile=$1
mode=$2
shift 2
printf "%s" "$$" > "$pidfile"
trap 'rm -f -- "$pidfile"' EXIT
if [ "$mode" = isolated ]; then
  env -i PATH="$PATH" HOME="$HOME" "$@"
else
  "$@"
fi
`

/** Signals every process in the group that WRAPPER leads. Exits 47 when the command never wrote its pid. */
const KILL = `
pidfile=$1
sig=$2
i=0
while [ ! -s "$pidfile" ] && [ "$i" -lt 250 ]; do sleep 0.02; i=$((i + 1)); done
[ -s "$pidfile" ] || exit 47
target=$(cat "$pidfile")
pass=0
while [ "$pass" -lt 2 ]; do
  for stat in /proc/[0-9]*/stat; do
    [ -e "$stat" ] || continue
    pid=\${stat#/proc/}
    pid=\${pid%/stat}
    set -- $(sed "s/.*) //" "$stat" 2>/dev/null)
    if [ "\${3:-}" = "$target" ]; then
      kill -s "$sig" "$pid" 2>/dev/null || true
    fi
  done
  pass=$((pass + 1))
done
`

/**
 * Runs each command through the sandbox's streaming exec. Files use the derived process-backed defaults: exec goes
 * over a local virtio socket, so a native override would save little.
 */
export const makeDriver = (sandbox: Sandbox, shell: string): EnvironmentDriver.Driver => {
  const spawn = Effect.fnUntraced(function* (command: Command) {
    if (command._tag === "PipedCommand")
      return yield* Effect.fail(spawnError("spawn", "piped commands are unsupported"))
    if (command.options.additionalFds)
      return yield* Effect.fail(spawnError("spawn", "additional file descriptors are unsupported"))

    const pidFile = `/tmp/opencode-process-${crypto.randomUUID()}.pid`
    const env = plainEnv(command.options.env)
    const isolated = command.options.extendEnv !== true && env !== undefined
    const stdinConfig = command.options.stdin
    const stdinOptions: { readonly stream?: CommandInput; readonly endOnDone?: boolean } =
      typeof stdinConfig === "object" && !Stream.isStream(stdinConfig) ? stdinConfig : { stream: stdinConfig }
    const input = stdinOptions.stream ?? "pipe"
    const piped = input === "pipe" || Stream.isStream(input)
    const args = [
      "--wait",
      "sh",
      "-c",
      WRAPPER,
      "sh",
      pidFile,
      isolated ? "isolated" : "inherit",
      ...(isolated ? Object.entries(env).map(([key, value]) => `${key}=${value}`) : []),
      guestCommand(command.command),
      ...command.args.map(plain),
    ]
    const cwd = command.options.cwd
    const exec = yield* Effect.tryPromise({
      try: () =>
        sandbox.execStreamWith("setsid", (options) => {
          const withArgs = options.args(args)
          const withCwd = cwd ? withArgs.cwd(cwd) : withArgs
          const withEnv = !isolated && env ? withCwd.envs(env) : withCwd
          return piped ? withEnv.stdinPipe() : withEnv.stdinNull()
        }),
      catch: (cause) => spawnError("spawn", undefined, cause),
    })

    const onError = (cause: unknown) => spawnError("process", undefined, cause)
    let exited = false
    const waited = exec.wait().then((status) => {
      exited = true
      return status.code
    })
    waited.catch(() => undefined)
    const exitCode = Effect.tryPromise({ try: () => waited, catch: onError }).pipe(Effect.map(ExitCode))

    const stdout = yield* Queue.unbounded<Uint8Array, PlatformError | Cause.Done>()
    const stderr = yield* Queue.unbounded<Uint8Array, PlatformError | Cause.Done>()
    yield* events(exec, onError).pipe(
      Stream.runForEach((event) => {
        if (event.kind === "stdout") return Queue.offer(stdout, event.data)
        if (event.kind === "stderr") return Queue.offer(stderr, event.data)
        return Effect.void
      }),
      Effect.matchEffect({
        onSuccess: () => Effect.all([Queue.end(stdout), Queue.end(stderr)]),
        onFailure: (error) => Effect.all([Queue.fail(stdout, error), Queue.fail(stderr, error)]),
      }),
      Effect.forkScoped,
    )

    const signalGroup = (signal: Signal) =>
      Effect.tryPromise({
        try: async () => {
          const killer = await sandbox.execWith("sh", (options) =>
            options.args(["-c", KILL, "sh", pidFile, signal.replace(/^SIG/, "")]).stdinNull(),
          )
          if (killer.code === 47) await exec.signal(constants.signals[signal] ?? constants.signals.SIGTERM)
          else if (!killer.success) throw new Error(`microsandbox kill exited ${killer.code}`)
        },
        catch: onError,
      })
    const kill = (options?: KillOptions) => {
      if (exited) return Effect.void
      const forceAfter = options?.forceKillAfter
      const settled = forceAfter
        ? exitCode.pipe(
            Effect.timeout(forceAfter),
            Effect.catch(() => signalGroup("SIGKILL").pipe(Effect.andThen(exitCode))),
          )
        : exitCode
      return signalGroup(options?.killSignal ?? "SIGTERM").pipe(Effect.andThen(settled), Effect.asVoid)
    }
    yield* Effect.addFinalizer(() => kill(command.options).pipe(Effect.ignore))

    const sink = piped ? yield* Effect.tryPromise({ try: () => exec.takeStdin(), catch: onError }) : null
    let closing: Promise<void> | undefined
    const closeStdin = Effect.tryPromise({
      try: () => (closing ??= sink?.close() ?? Promise.resolve()),
      catch: onError,
    })
    const writeStdin = Sink.forEach((chunk: Uint8Array) =>
      sink ? Effect.tryPromise({ try: () => sink.write(chunk), catch: onError }) : Effect.void,
    )
    const stdin =
      stdinOptions.endOnDone === false ? writeStdin : writeStdin.pipe(Sink.ensuring(Effect.ignore(closeStdin)))
    if (Stream.isStream(input)) yield* Effect.forkScoped(Stream.run(input, stdin))

    const stdoutStream = Stream.fromQueue(stdout)
    const stderrStream = Stream.fromQueue(stderr)
    return makeHandle({
      pid: ProcessId(crypto.getRandomValues(new Uint32Array(1))[0]),
      exitCode,
      isRunning: Effect.sync(() => !exited),
      kill,
      stdin,
      stdout: stdoutStream,
      stderr: stderrStream,
      all: Stream.merge(stdoutStream, stderrStream),
      getInputFd: () => Sink.fail(spawnError("getInputFd", "unsupported")),
      getOutputFd: () => Stream.fail(spawnError("getOutputFd", "unsupported")),
      unref: Effect.succeed(Effect.void),
    })
  })

  return { spawner: make(spawn), shell }
}

const events = (exec: ExecHandle, onError: (cause: unknown) => PlatformError) => Stream.fromAsyncIterable(exec, onError)

/** Ripgrep is named by the server's own binary path. The machine runs the rg of its image instead. */
const guestCommand = (command: string) => (path.isAbsolute(command) && path.basename(command) === "rg" ? "rg" : command)

const plain = (value: string | Redacted.Redacted<string>) => (typeof value === "string" ? value : Redacted.value(value))

const plainEnv = (env: CommandOptions["env"]): Record<string, string> | undefined => {
  if (!env) return undefined
  return Object.fromEntries(
    Object.entries(env).flatMap(
      ([key, value]): Array<[string, string]> => (value === undefined ? [] : [[key, plain(value)]]),
    ),
  )
}

const spawnError = (method: string, description?: string, cause?: unknown) =>
  systemError({ _tag: "Unknown", module: "MicrosandboxDriver", method, description, cause })

export * as MicrosandboxWorkspace from "./microsandbox"
