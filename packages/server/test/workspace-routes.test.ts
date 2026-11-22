import { expect } from "bun:test"
import { makeMemoryDriver } from "@opencode/core/environment/index"
import { WorkspaceDriver } from "@opencode/core/workspace/driver"
import { Deferred, Effect, Sink, Stream } from "effect"
import { ExitCode, make as makeSpawner, makeHandle, ProcessId } from "effect/process/ChildProcessSpawner"
import { tmpdirScoped } from "../../core/test/fixture/tmpdir"
import { it } from "../../core/test/lib/effect"
import { ServerFetch } from "../src/fetch"

const serve = Effect.fnUntraced(function* (workspaces: boolean) {
  const calls: string[] = []
  const spawned = yield* Deferred.make<{ readonly command: string; readonly args: ReadonlyArray<string> }>()
  const memory = makeMemoryDriver()
  const spawner = makeSpawner((command) =>
    Effect.gen(function* () {
      if (command._tag === "StandardCommand")
        yield* Deferred.succeed(spawned, { command: command.command, args: command.args.map(String) })
      return makeHandle({
        pid: ProcessId(1),
        exitCode: Effect.succeed(ExitCode(0)),
        isRunning: Effect.succeed(false),
        kill: () => Effect.void,
        stdin: Sink.drain,
        stdout: Stream.empty,
        stderr: Stream.empty,
        all: Stream.empty,
        getInputFd: () => Sink.drain,
        getOutputFd: () => Stream.empty,
        unref: Effect.succeed(Effect.void),
      })
    }),
  )
  const driver = WorkspaceDriver.make({
    shell: "/bin/fake-sh",
    create: ({ workspaceID }) => {
      calls.push("create")
      return Effect.succeed({ binding: { workspaceID } })
    },
    connect: () => {
      calls.push("connect")
      return Effect.succeed({ spawner, overrides: memory.overrides })
    },
    suspendForIdle: () => Effect.void,
    destroy: () => {
      calls.push("destroy")
      return Effect.void
    },
  })
  const directory = (yield* tmpdirScoped("opencode-workspace-routes-")).path
  const handler = yield* ServerFetch.make(
    {
      app: { version: "test" },
      database: { path: ":memory:" },
      models: { fetch: false },
      fs: { filewatcher: false },
      config: { directory, project: false, content: JSON.stringify({ experimental: { workspaces } }) },
    },
    { overrides: [WorkspaceDriver.node.replace(WorkspaceDriver.registryNode({ fake: driver }))] },
  )
  const request = (method: string, path: string, body?: unknown) =>
    Effect.promise(async () => {
      const response = await handler(
        new Request(`http://opencode.local${path}`, {
          method,
          headers: { "content-type": "application/json" },
          body: body === undefined ? undefined : JSON.stringify(body),
        }),
      )
      return { status: response.status, body: response.status === 204 ? undefined : await response.json() }
    })
  return { calls, spawned, request }
})

it.live("creates a workspace, runs a session's commands in it under the provider's shell, and destroys it", () =>
  Effect.gen(function* () {
    const server = yield* serve(true)

    const unknown = yield* server.request("POST", "/api/experimental/workspace", { provider: "nope" })
    expect(unknown.status).toBe(400)

    const created = yield* server.request("POST", "/api/experimental/workspace", { provider: "fake" })
    expect(created.status).toBe(200)
    const workspace = created.body.data
    expect(workspace).toMatchObject({ provider: "fake" })
    expect(workspace.id).toStartWith("wrk")
    expect(server.calls).toEqual(["create"])

    const again = yield* server.request("POST", "/api/experimental/workspace", { provider: "fake", id: workspace.id })
    expect(again.body.data.id).toBe(workspace.id)
    expect(server.calls).toEqual(["create"])

    const missing = yield* server.request("POST", "/api/session", {
      location: { directory: "/" },
      workspaceID: "wrk_missing",
    })
    expect(missing.status).toBe(400)

    const session = yield* server.request("POST", "/api/session", {
      location: { directory: "/" },
      workspaceID: workspace.id,
    })
    expect(session.status).toBe(200)
    const child = yield* server.request("POST", "/api/session", {
      parentID: session.body.data.id,
      workspaceID: workspace.id,
    })
    expect(child.status).toBe(400)

    const shell = yield* server.request("POST", `/api/session/${session.body.data.id}/shell`, { command: "echo hi" })
    expect(shell.status).toBe(204)
    const spawned = yield* Deferred.await(server.spawned).pipe(Effect.timeout("10 seconds"))
    expect(spawned.command).toBe("/bin/fake-sh")
    expect(spawned.args.join(" ")).toContain("echo hi")
    expect(server.calls).toContain("connect")

    const destroyed = yield* server.request("DELETE", `/api/experimental/workspace/${workspace.id}`)
    expect(destroyed.body.data).toEqual({ destroyed: true })
    expect((yield* server.request("DELETE", `/api/experimental/workspace/${workspace.id}`)).body.data).toEqual({
      destroyed: false,
    })
  }).pipe(Effect.scoped),
)

it.live("refuses workspace routes and workspace sessions while the flag is off", () =>
  Effect.gen(function* () {
    const server = yield* serve(false)
    expect((yield* server.request("POST", "/api/experimental/workspace", { provider: "fake" })).status).toBe(403)
    expect(
      (yield* server.request("POST", "/api/session", { location: { directory: "/" }, workspaceID: "wrk_any" })).status,
    ).toBe(403)
    expect(server.calls).toEqual([])
  }).pipe(Effect.scoped),
)
