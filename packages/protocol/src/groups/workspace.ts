import { Workspace } from "@opencode/schema/workspace"
import { Schema } from "effect"
import { HttpApiEndpoint, OpenApi } from "effect/http-api"
import { ConflictError, ForbiddenError, InvalidRequestError, ServiceUnavailableError } from "../errors.js"

const EXPERIMENTAL = "Behind the workspaces flag. Experimental: may change without compatibility guarantees."

const annotations = (identifier: string, summary: string, description: string) =>
  OpenApi.annotations({
    identifier: `experimental.workspace.${identifier}`,
    summary,
    description: `${description} ${EXPERIMENTAL}`,
    transform: (operation) => ({ ...operation, tags: ["experimental"] }),
  })

export const WorkspaceInfo = Schema.Struct({
  id: Workspace.ID,
  provider: Schema.String,
  createdAt: Schema.Number,
  lastUsedAt: Schema.Number,
}).annotate({ identifier: "Workspace.Info" })
export type WorkspaceInfo = typeof WorkspaceInfo.Type

export const WorkspaceCreatePayload = Schema.Struct({
  provider: Schema.String.annotate({ description: "A workspace provider the server registers, such as microsandbox." }),
  id: Workspace.ID.pipe(Schema.optional),
}).annotate({ identifier: "Workspace.CreateInput" })
export type WorkspaceCreatePayload = typeof WorkspaceCreatePayload.Type

/** Lives in the `server.experimental` group (see `groups/experimental.ts`). */
export const WorkspaceEndpoints = [
  HttpApiEndpoint.post("workspace.create", "/api/experimental/workspace", {
    payload: WorkspaceCreatePayload,
    success: Schema.Struct({ data: WorkspaceInfo }),
    error: [InvalidRequestError, ForbiddenError, ConflictError, ServiceUnavailableError],
  }).annotateMerge(
    annotations(
      "create",
      "Create a workspace",
      "Create a workspace from a provider and make its machine ready. Creating the same id again returns the same workspace.",
    ),
  ),
  HttpApiEndpoint.delete("workspace.destroy", "/api/experimental/workspace/:workspaceID", {
    params: { workspaceID: Workspace.ID },
    success: Schema.Struct({ data: Workspace.DestroyResult }),
    error: [ForbiddenError, InvalidRequestError, ServiceUnavailableError],
  }).annotateMerge(
    annotations(
      "destroy",
      "Destroy a workspace",
      "Remove a workspace and its machine. Sessions in it can no longer run tools.",
    ),
  ),
] as const
