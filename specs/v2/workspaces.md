# Workspaces

A workspace is a durable place where a session's tools run that is not the server's own machine. The server keeps the model loop, the session history and the credentials. A provider's driver owns the machine.

## Contract

- `WorkspaceDriver.Interface` (`packages/core/src/workspace/driver.ts`) has `create`, `connect`, `suspendForIdle` and `destroy`. `create` must be idempotent per workspace id. `connect` returns an `EnvironmentDriver.Driver`: a process spawner, optional file overrides and an optional shell.
- `Workspace` (`packages/core/src/workspace.ts`) stores each workspace's provider and binding, provisions it once, connects on first use and suspends it after 20 idle minutes.
- A Location whose ref names a `workspaceID` builds its `Environment` from the workspace. That Environment does not host the server: shell commands take none of the server's variables and skip the process daemon, and formatters do not run there. File operations use the process-backed defaults unless the driver overrides them, so the machine needs GNU coreutils and findutils. MCP stdio servers and ripgrep run through the workspace's spawner.
- The provider's `shell` is the shell of every command in its machines. Without one, the server's configured shell path is used, which may not exist in the machine.

## HTTP

Behind the `workspaces` flag (`experimental.workspaces`).

- `POST /api/experimental/workspace` with `{ provider, id? }` creates the workspace and provisions it before it answers. The same id again returns the same workspace. An unknown provider is a 400; a driver failure is a 503.
- `POST /api/session` takes `workspaceID`. The workspace is provisioned first. A child session takes its parent's workspace, so `workspaceID` with `parentID` is a 400.
- `DELETE /api/experimental/workspace/:workspaceID` destroys the workspace and reports whether it existed.
- Routes that resolve a Location from the request (`location[directory]`) cannot name a workspace. Routes under `/api/session/:sessionID` use the session's own Location, workspace included.

## microsandbox

`opencode serve` registers the `microsandbox` provider (`packages/server/src/workspace/microsandbox.ts`). Each workspace is one detached microVM named `opencode-<workspaceID>`, made with `connectOrCreate`, so a retried create adopts the machine an earlier attempt made. `connect` starts a stopped machine and makes a removed one again. `suspendForIdle` stops it; `destroy` removes it.

The SDK is imported on first use. Its native addon loads when the server runs from source with Bun. The compiled `opencode` binary does not embed the addon.

| Variable                        | Default                                                | Meaning                                                                                                                           |
| ------------------------------- | ------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------- |
| `OPENCODE_MICROSANDBOX_IMAGE`   | `ubuntu:24.04`                                         | OCI image of each machine.                                                                                                        |
| `OPENCODE_MICROSANDBOX_SETUP`   | installs `git` and `ripgrep` when the image is default | Script run with `sh -c` in each machine before its workspace is ready. It runs again when a create is retried.                    |
| `OPENCODE_MICROSANDBOX_MOUNTS`  | none                                                   | Comma-separated bind mounts, `host[:guest][:ro]`. Mount the session's directory at the same path, and a worktree's bare repo too. |
| `OPENCODE_MICROSANDBOX_NETWORK` | `all`                                                  | `all`, `none`, or comma-separated domains that egress may reach.                                                                  |
| `OPENCODE_MICROSANDBOX_CPUS`    | 1                                                      | vCPUs.                                                                                                                            |
| `OPENCODE_MICROSANDBOX_MEMORY`  | 512                                                    | Memory in MiB.                                                                                                                    |
| `OPENCODE_MICROSANDBOX_SHELL`   | `/bin/bash`                                            | Shell of every command.                                                                                                           |

A malformed variable fails every create and connect of this provider with its message. The server still starts.

Each command runs as `setsid --wait sh -c <wrapper>`. The wrapper writes the pid of the process group leader to a file in `/tmp`, so a kill signals every process in the group from inside the machine. A spawn that replaces the environment keeps the image's `PATH` and `HOME`. Ripgrep is named by the server's own binary path, which the driver replaces with the image's `rg`. Piped commands and extra file descriptors are refused.
