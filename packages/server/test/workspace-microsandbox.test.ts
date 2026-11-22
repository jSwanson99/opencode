import { describe, expect, test } from "bun:test"
import { Workspace } from "@opencode/core/workspace"
import { WorkspaceDriver } from "@opencode/core/workspace/driver"
import { Effect, Exit, Scope, Stream } from "effect"
import { ChildProcess } from "effect/process"
import type { Sandbox } from "microsandbox"
import { it } from "../../core/test/lib/effect"
import { MicrosandboxWorkspace } from "../src/workspace/microsandbox"

type ExecEvent = { kind: "stdout" | "stderr"; data: Uint8Array } | { kind: "exited"; code: number }

interface ExecCall {
  readonly cmd: string
  readonly args: string[]
  readonly cwd?: string
  readonly envs?: Record<string, string>
  readonly stdin: "pipe" | "null"
}

/** What the fake machine does for one command: its output, its exit code, and whether it echoes its stdin. */
interface Behavior {
  readonly stdout?: string
  readonly stderr?: string
  readonly code?: number
  readonly echoStdin?: boolean
  readonly hang?: boolean
}

class SandboxNotFoundError extends Error {}

const encode = (text: string) => new TextEncoder().encode(text)

class Options {
  readonly call: { args: string[]; cwd?: string; envs?: Record<string, string>; stdin: "pipe" | "null" } = {
    args: [],
    stdin: "pipe",
  }
  args(args: string[]) {
    this.call.args = args
    return this
  }
  cwd(cwd: string) {
    this.call.cwd = cwd
    return this
  }
  envs(envs: Record<string, string>) {
    this.call.envs = envs
    return this
  }
  stdinPipe() {
    this.call.stdin = "pipe"
    return this
  }
  stdinNull() {
    this.call.stdin = "null"
    return this
  }
}

class FakeExec {
  private readonly queue: ExecEvent[] = []
  private waiting: ((event: ExecEvent | null) => void) | undefined
  private done = false
  private readonly input: Uint8Array[] = []
  private readonly exit = Promise.withResolvers<{ code: number; success: boolean }>()
  readonly signals: number[] = []

  constructor(private readonly behavior: Behavior) {
    if (behavior.stdout) this.push({ kind: "stdout", data: encode(behavior.stdout) })
    if (behavior.stderr) this.push({ kind: "stderr", data: encode(behavior.stderr) })
    if (!behavior.echoStdin && !behavior.hang) this.finish(behavior.code ?? 0)
  }

  private push(event: ExecEvent) {
    if (this.waiting) {
      const resolve = this.waiting
      this.waiting = undefined
      resolve(event)
      return
    }
    this.queue.push(event)
  }

  finish(code: number) {
    if (this.done) return
    this.push({ kind: "exited", code })
    this.done = true
    this.exit.resolve({ code, success: code === 0 })
    if (this.waiting) this.waiting(null)
  }

  async *[Symbol.asyncIterator]() {
    while (true) {
      const next = this.queue.shift()
      if (next) {
        yield next
        continue
      }
      if (this.done) return
      const event = await new Promise<ExecEvent | null>((resolve) => (this.waiting = resolve))
      if (event === null) return
      yield event
    }
  }

  async takeStdin() {
    return {
      write: async (data: Uint8Array) => void this.input.push(data),
      close: async () => {
        if (!this.behavior.echoStdin) return
        this.push({ kind: "stdout", data: new Uint8Array(Buffer.concat(this.input)) })
        this.finish(this.behavior.code ?? 0)
      },
    }
  }

  wait() {
    return this.exit.promise
  }

  async signal(signal: number) {
    this.signals.push(signal)
    this.finish(128 + signal)
  }
}

const makeFake = () => {
  const machines = new Map<string, FakeSandbox>()
  const built: Array<Record<string, unknown>> = []
  const execs: Array<{ call: ExecCall; process: FakeExec }> = []
  const shells: ExecCall[] = []
  let behave: (call: ExecCall) => Behavior = () => ({})
  let killExit = 0
  let nextID = 0

  class FakeSandbox {
    detached = 0
    constructor(
      readonly name: string,
      readonly id: string,
      public status: "running" | "stopped",
    ) {}
    async detach() {
      this.detached++
    }
    async execWith(cmd: string, configure: (options: Options) => Options) {
      const call = { cmd, ...configure(new Options()).call }
      shells.push(call)
      const killing = call.args[1]?.includes("/proc/")
      const pidFile = call.args[3]
      if (killing) {
        const target = execs.find((exec) => exec.call.args.includes(pidFile))
        if (killExit === 0) target?.process.finish(143)
        const code = killExit
        return { code, success: code === 0, stderr: () => "" }
      }
      const code = behave(call).code ?? 0
      return { code, success: code === 0, stderr: () => (code === 0 ? "" : "setup failed") }
    }
    async execStreamWith(cmd: string, configure: (options: Options) => Options) {
      const call = { cmd, ...configure(new Options()).call }
      const process = new FakeExec(behave(call))
      execs.push({ call, process })
      return process
    }
  }

  class Handle {
    stops = 0
    destroyed: unknown
    constructor(readonly machine: FakeSandbox) {}
    get status() {
      return this.machine.status
    }
    async connectOrStart() {
      this.machine.status = "running"
      return this.machine
    }
    async stopWithTimeout() {
      this.stops++
      this.machine.status = "stopped"
    }
    async kill() {
      this.machine.status = "stopped"
    }
    async destroy(options: unknown) {
      this.destroyed = options
      machines.delete(this.machine.name)
    }
  }
  const handles = new Map<string, Handle>()

  const builder = (name: string) => {
    const config: Record<string, unknown> = { name, volumes: [] as unknown[] }
    const chain = {
      image: (image: string) => ((config.image = image), chain),
      detached: (value: boolean) => ((config.detached = value), chain),
      label: (key: string, value: string) => ((config.label = [key, value]), chain),
      memory: (value: number) => ((config.memory = value), chain),
      cpus: (value: number) => ((config.cpus = value), chain),
      volume: (guest: string, configure: (mount: unknown) => unknown) => {
        const state = { host: "", readonly: false }
        const mount = {
          bind(host: string) {
            state.host = host
            return mount
          },
          readonly() {
            state.readonly = true
            return mount
          },
        }
        configure(mount)
        ;(config.volumes as unknown[]).push({ guest, ...state })
        return chain
      },
      disableNetwork: () => ((config.network = "none"), chain),
      network: (configure: (network: unknown) => unknown) => {
        configure({ policy: (policy: unknown) => (config.network = policy) })
        return chain
      },
      connectOrCreate: async () => {
        built.push(config)
        const existing = machines.get(name)
        if (existing) {
          existing.status = "running"
          return existing
        }
        const machine = new FakeSandbox(name, `id-${nextID++}`, "running")
        machines.set(name, machine)
        return machine
      },
    }
    return chain
  }

  const sdk = {
    Sandbox: {
      builder,
      get: async (name: string) => {
        const machine = machines.get(name)
        if (!machine) throw new SandboxNotFoundError(name)
        const handle = handles.get(name)
        if (handle?.machine === machine) return handle
        const created = new Handle(machine)
        handles.set(name, created)
        return created
      },
    },
    SandboxNotFoundError,
    Rule: { allowDns: () => ({ dns: true }), allowEgress: (destination: unknown) => ({ allow: destination }) },
    Destination: { domain: (domain: string) => ({ domain }) },
  }

  return {
    sdk: sdk as unknown as MicrosandboxWorkspace.Sdk,
    machines,
    handles,
    built,
    execs,
    shells,
    behave: (fn: (call: ExecCall) => Behavior) => (behave = fn),
    killExits: (code: number) => (killExit = code),
    sandbox: (name = "opencode-wrk_test") => {
      const machine = new FakeSandbox(name, `id-${nextID++}`, "running")
      machines.set(name, machine)
      return machine as unknown as Sandbox
    },
  }
}

const options = (overrides: Partial<MicrosandboxWorkspace.Options> = {}): MicrosandboxWorkspace.Options => ({
  image: "ubuntu:24.04",
  mounts: [],
  network: { type: "all" },
  shell: "/bin/bash",
  ...overrides,
})

const workspaceID = Workspace.ID.make("wrk_test")

describe("fromEnv", () => {
  test("defaults to Ubuntu with the setup that installs git and ripgrep", () => {
    expect(MicrosandboxWorkspace.fromEnv({})).toEqual({
      image: "ubuntu:24.04",
      setup: MicrosandboxWorkspace.defaultSetup,
      mounts: [],
      network: { type: "all" },
      shell: "/bin/bash",
      cpus: undefined,
      memoryMiB: undefined,
    })
  })

  test("reads mounts, network, sizes and an image without the default setup", () => {
    expect(
      MicrosandboxWorkspace.fromEnv({
        OPENCODE_MICROSANDBOX_IMAGE: "ghcr.io/me/agent:1",
        OPENCODE_MICROSANDBOX_MOUNTS: "/home/me/src/workspaces, /home/me/src/repos:/repos:ro,/opt/cache:ro",
        OPENCODE_MICROSANDBOX_NETWORK: "api.github.com, registry.npmjs.org",
        OPENCODE_MICROSANDBOX_CPUS: "2",
        OPENCODE_MICROSANDBOX_MEMORY: "4096",
        OPENCODE_MICROSANDBOX_SHELL: "/bin/zsh",
      }),
    ).toEqual({
      image: "ghcr.io/me/agent:1",
      setup: undefined,
      mounts: [
        { host: "/home/me/src/workspaces", guest: "/home/me/src/workspaces", readonly: false },
        { host: "/home/me/src/repos", guest: "/repos", readonly: true },
        { host: "/opt/cache", guest: "/opt/cache", readonly: true },
      ],
      network: { type: "hosts", hosts: ["api.github.com", "registry.npmjs.org"] },
      shell: "/bin/zsh",
      cpus: 2,
      memoryMiB: 4096,
    })
    expect(MicrosandboxWorkspace.fromEnv({ OPENCODE_MICROSANDBOX_NETWORK: "none" })).toMatchObject({
      network: { type: "none" },
    })
  })

  test("reports a malformed variable as text", () => {
    expect(MicrosandboxWorkspace.fromEnv({ OPENCODE_MICROSANDBOX_MOUNTS: "relative/path" })).toContain(
      "OPENCODE_MICROSANDBOX_MOUNTS",
    )
    expect(MicrosandboxWorkspace.fromEnv({ OPENCODE_MICROSANDBOX_CPUS: "two" })).toContain("OPENCODE_MICROSANDBOX_CPUS")
  })
})

describe("driver", () => {
  it.live("creates one detached machine per workspace and adopts it on a retry", () =>
    Effect.gen(function* () {
      const fake = makeFake()
      const driver = MicrosandboxWorkspace.driver({
        options: options({
          setup: "install things",
          memoryMiB: 2048,
          mounts: [{ host: "/src", guest: "/src", readonly: false }],
          network: { type: "hosts", hosts: ["api.github.com"] },
        }),
        load: async () => fake.sdk,
      })
      const first = yield* driver.create({ workspaceID })
      const second = yield* driver.create({ workspaceID })

      expect(first.binding).toEqual({ name: "opencode-wrk_test", id: "id-0" })
      expect(second.binding).toEqual(first.binding)
      expect(fake.machines.size).toBe(1)
      expect(fake.built[0]).toEqual({
        name: "opencode-wrk_test",
        image: "ubuntu:24.04",
        detached: true,
        label: ["opencode.workspace", "wrk_test"],
        memory: 2048,
        volumes: [{ guest: "/src", host: "/src", readonly: false }],
        network: {
          defaultEgress: "deny",
          defaultIngress: "allow",
          rules: [{ dns: true }, { allow: { domain: "api.github.com" } }],
        },
      })
      expect(fake.shells.map((call) => [call.cmd, ...call.args])).toEqual([
        ["sh", "-c", "install things"],
        ["sh", "-c", "install things"],
      ])
      expect((fake.machines.get("opencode-wrk_test") as unknown as { detached: number }).detached).toBe(2)
      expect(driver.shell).toBe("/bin/bash")
    }),
  )

  it.live("fails create with the setup's error and with a malformed configuration", () =>
    Effect.gen(function* () {
      const fake = makeFake()
      fake.behave(() => ({ code: 100 }))
      const failing = MicrosandboxWorkspace.driver({
        options: options({ setup: "exit 100" }),
        load: async () => fake.sdk,
      })
      const setup = yield* Effect.flip(failing.create({ workspaceID }))
      expect(setup).toBeInstanceOf(WorkspaceDriver.Error)
      expect(setup.message).toContain("setup exited 100")

      const malformed = MicrosandboxWorkspace.driver({
        options: "OPENCODE_MICROSANDBOX_CPUS: bad",
        load: async () => fake.sdk,
      })
      expect((yield* Effect.flip(malformed.create({ workspaceID }))).message).toBe("OPENCODE_MICROSANDBOX_CPUS: bad")
      expect(malformed.shell).toBeUndefined()
    }),
  )

  it.live("connects to the machine, saves a changed identity and detaches when the scope closes", () =>
    Effect.gen(function* () {
      const fake = makeFake()
      const driver = MicrosandboxWorkspace.driver({ options: options(), load: async () => fake.sdk })
      const { binding } = yield* driver.create({ workspaceID })
      fake.machines.get("opencode-wrk_test")!.status = "stopped"
      const saved: unknown[] = []
      const scope = yield* Scope.make()
      const connected = yield* driver
        .connect({ workspaceID, binding, saveBinding: (next) => Effect.sync(() => void saved.push(next)) })
        .pipe(Scope.provide(scope))
      expect(fake.machines.get("opencode-wrk_test")!.status).toBe("running")
      expect(saved).toEqual([])
      expect(connected.shell).toBe("/bin/bash")
      yield* Scope.close(scope, Exit.void)
      expect((fake.machines.get("opencode-wrk_test") as unknown as { detached: number }).detached).toBe(2)

      fake.machines.clear()
      const again = yield* Scope.make()
      yield* driver
        .connect({ workspaceID, binding, saveBinding: (next) => Effect.sync(() => void saved.push(next)) })
        .pipe(Scope.provide(again))
      expect(saved).toEqual([{ name: "opencode-wrk_test", id: "id-1" }])
      yield* Scope.close(again, Exit.void)
    }),
  )

  it.live("stops a running machine when idle and destroys it, treating a missing one as done", () =>
    Effect.gen(function* () {
      const fake = makeFake()
      const driver = MicrosandboxWorkspace.driver({ options: options(), load: async () => fake.sdk })
      const { binding } = yield* driver.create({ workspaceID })
      const saveBinding = () => Effect.void
      yield* driver.suspendForIdle({ workspaceID, binding, saveBinding })
      yield* driver.suspendForIdle({ workspaceID, binding, saveBinding })
      expect(fake.handles.get("opencode-wrk_test")!.stops).toBe(1)

      yield* driver.destroy({ workspaceID, binding })
      expect(fake.handles.get("opencode-wrk_test")!.destroyed).toEqual({ force: true, timeoutMs: 30_000 })
      expect(fake.machines.size).toBe(0)
      yield* driver.destroy({ workspaceID, binding: null })
      yield* driver.suspendForIdle({ workspaceID, binding, saveBinding })
    }),
  )
})

describe("spawner", () => {
  it.live("runs a command in its process group with a replaced environment and splits its output", () =>
    Effect.gen(function* () {
      const fake = makeFake()
      fake.behave((call) => (call.cmd === "setsid" ? { stdout: "out", stderr: "err", code: 3 } : {}))
      const { spawner } = MicrosandboxWorkspace.makeDriver(fake.sandbox(), "/bin/bash")
      const result = yield* Effect.scoped(
        Effect.gen(function* () {
          const handle = yield* spawner.spawn(
            ChildProcess.make("git", ["status"], { cwd: "/src/app", env: { LC_ALL: "C" }, stdin: "ignore" }),
          )
          const [stdout, stderr] = yield* Effect.all(
            [Stream.mkString(Stream.decodeText(handle.stdout)), Stream.mkString(Stream.decodeText(handle.stderr))],
            { concurrency: "unbounded" },
          )
          return { stdout, stderr, code: Number(yield* handle.exitCode) }
        }),
      )

      expect(result).toEqual({ stdout: "out", stderr: "err", code: 3 })
      const { call } = fake.execs[0]
      expect(call.cmd).toBe("setsid")
      expect(call.args.slice(0, 3)).toEqual(["--wait", "sh", "-c"])
      expect(call.args.slice(4, 5)).toEqual(["sh"])
      expect(call.args[5]).toMatch(/^\/tmp\/opencode-process-.+\.pid$/)
      expect(call.args.slice(6)).toEqual(["isolated", "LC_ALL=C", "git", "status"])
      expect(call.cwd).toBe("/src/app")
      expect(call.envs).toBeUndefined()
      expect(call.stdin).toBe("null")
    }),
  )

  it.live("adds variables to the machine's environment, streams stdin and runs the image's ripgrep", () =>
    Effect.gen(function* () {
      const fake = makeFake()
      fake.behave((call) => (call.cmd === "setsid" ? { echoStdin: true } : {}))
      const { spawner } = MicrosandboxWorkspace.makeDriver(fake.sandbox(), "/bin/bash")
      const output = yield* spawner.string(
        ChildProcess.make("/home/me/.local/share/opencode/bin/rg", ["--files"], {
          env: { RG: "1" },
          extendEnv: true,
          stdin: Stream.make(encode("hello "), encode("world")),
        }),
      )

      expect(output).toBe("hello world")
      const { call } = fake.execs[0]
      expect(call.args.slice(6)).toEqual(["inherit", "rg", "--files"])
      expect(call.envs).toEqual({ RG: "1" })
      expect(call.stdin).toBe("pipe")
    }),
  )

  it.live("kills the whole process group, and signals the command when it never started its wrapper", () =>
    Effect.gen(function* () {
      const fake = makeFake()
      fake.behave((call) => (call.cmd === "setsid" ? { hang: true } : {}))
      const { spawner } = MicrosandboxWorkspace.makeDriver(fake.sandbox(), "/bin/bash")
      yield* Effect.scoped(
        Effect.gen(function* () {
          const handle = yield* spawner.spawn(ChildProcess.make("sleep", ["100"], { stdin: "ignore" }))
          yield* handle.kill({ killSignal: "SIGINT" })
          expect(yield* handle.isRunning).toBe(false)
        }),
      )
      const kill = fake.shells.at(-1)!
      expect(kill.args.at(-1)).toBe("INT")
      expect(kill.args.at(-2)).toBe(fake.execs[0].call.args[5])
      expect(fake.execs[0].process.signals).toEqual([])

      fake.killExits(47)
      yield* Effect.scoped(
        Effect.gen(function* () {
          const handle = yield* spawner.spawn(ChildProcess.make("sleep", ["100"], { stdin: "ignore" }))
          yield* handle.kill()
        }),
      )
      expect(fake.execs[1].process.signals).toEqual([15])
    }),
  )

  it.live("refuses piped commands", () =>
    Effect.gen(function* () {
      const fake = makeFake()
      const { spawner } = MicrosandboxWorkspace.makeDriver(fake.sandbox(), "/bin/bash")
      const error = yield* Effect.flip(
        Effect.scoped(spawner.spawn(ChildProcess.make("ls").pipe(ChildProcess.pipeTo(ChildProcess.make("wc"))))),
      )
      expect(error.message).toContain("piped commands are unsupported")
      expect(fake.execs).toEqual([])
    }),
  )
})
