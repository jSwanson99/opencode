import { Flag } from "@opencode/core/flag"
import { Location } from "@opencode/core/location"
import { LocationServiceMap } from "@opencode/core/location-services"
import { AbsolutePath } from "@opencode/core/schema"
import { Workspace } from "@opencode/core/workspace"
import { ConflictError, ForbiddenError, InvalidRequestError, ServiceUnavailableError } from "@opencode/protocol/errors"
import type { WorkspaceCreatePayload } from "@opencode/protocol/groups/workspace"
import { Global } from "@opencode/util/global"
import { Effect } from "effect"

/** Whether the `workspaces` flag is on in the server's base configuration, where the machine routes read theirs. */
export const workspacesEnabled = Effect.gen(function* () {
  const global = yield* Global.Service
  const locations = yield* LocationServiceMap.Service
  return Flag.enabled("workspaces").pipe(
    Effect.provide(locations.get(Location.Ref.make({ directory: AbsolutePath.make(global.config) }))),
    Effect.orElseSucceed(() => false),
  )
})

/**
 * `workspace.*` in the `server.experimental` group, and the workspace check of `session.create`. Every route needs the
 * `workspaces` flag. A workspace is made ready before it is returned, so a session's first tool never waits on a boot.
 */
export const makeWorkspace = Effect.gen(function* () {
  const enabled = yield* workspacesEnabled
  const workspaces = yield* Workspace.Service
  const requireEnabled = Effect.gen(function* () {
    if (yield* enabled) return
    return yield* new ForbiddenError({ message: "Workspaces are disabled; enable experimental.workspaces" })
  })
  const unavailable = (error: { readonly message?: string; readonly _tag: string }) =>
    new ServiceUnavailableError({ message: error.message ?? error._tag, service: "workspace" })
  const ready = Effect.fn("WorkspaceHandler.ready")(function* (workspaceID: Workspace.ID) {
    yield* requireEnabled
    return yield* workspaces.provision(workspaceID).pipe(
      Effect.catchTags({
        "Workspace.NotFound": () =>
          new InvalidRequestError({ message: `Workspace not found: ${workspaceID}`, field: "workspaceID" }),
        "WorkspaceDriver.Error": unavailable,
        "WorkspaceDriver.ProviderNotFound": (error) =>
          unavailable({ _tag: error._tag, message: `Workspace provider not registered: ${error.provider}` }),
      }),
    )
  })

  return {
    ready,
    create: Effect.fn("WorkspaceHandler.create")(function* (payload: WorkspaceCreatePayload) {
      yield* requireEnabled
      const workspaceID = yield* workspaces.create({ id: payload.id, provider: payload.provider }).pipe(
        Effect.catchTags({
          "Workspace.CreateConflict": (error) =>
            new ConflictError({
              message: `Workspace ${error.workspaceID} uses provider ${error.existingProvider}`,
              resource: error.workspaceID,
            }),
          "WorkspaceDriver.ProviderNotFound": (error) =>
            new InvalidRequestError({ message: `Unknown workspace provider: ${error.provider}`, field: "provider" }),
        }),
      )
      const info = yield* ready(workspaceID)
      return { data: { id: info.id, provider: info.provider, createdAt: info.createdAt, lastUsedAt: info.lastUsedAt } }
    }),
    destroy: Effect.fn("WorkspaceHandler.destroy")(function* (workspaceID: Workspace.ID) {
      yield* requireEnabled
      return {
        data: yield* workspaces.destroy(workspaceID).pipe(
          Effect.catchTags({
            "WorkspaceDriver.Error": unavailable,
            "WorkspaceDriver.ProviderNotFound": (error) =>
              unavailable({ _tag: error._tag, message: `Workspace provider not registered: ${error.provider}` }),
          }),
        ),
      }
    }),
  }
})
