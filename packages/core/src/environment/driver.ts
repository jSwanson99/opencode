import type { ChildProcessSpawner } from "effect/process/ChildProcessSpawner"
import type { FilesImpl } from "./files.js"

export interface Driver {
  readonly spawner: ChildProcessSpawner["Service"]
  readonly overrides?: Partial<FilesImpl>
  /** The shell every command runs under, when the machine has its own rather than the server's configured one. */
  readonly shell?: string
}

export * as EnvironmentDriver from "./driver.js"
