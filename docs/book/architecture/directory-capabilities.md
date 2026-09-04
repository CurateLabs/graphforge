# Directory capability and lifecycle admission boundary

Issue #1017 consolidates directory identity checks while retaining storage's
additional lifecycle policy. This is a native filesystem boundary, independent
of query semantics or project-format migration.

## Threat model before consolidation

`graphforge-filesystem::StableDirectory` and storage's private
`LifecycleDirectory` currently both retain a directory handle and its native
volume/file identity. Their platform opens are identical: Unix uses
`RDONLY | DIRECTORY | NOFOLLOW | CLOEXEC` with `openat` for child lookup;
Windows uses read access, `FILE_FLAG_BACKUP_SEMANTICS`,
`FILE_FLAG_OPEN_REPARSE_POINT`, and read/write sharing without
`FILE_SHARE_DELETE`. Unsupported platforms reject directory admission.

| Threat or operation | StableDirectory | LifecycleDirectory |
| --- | --- | --- |
| Named directory replaced | Revalidation compares named identity with captured identity | Same, also rereads retained-handle metadata/identity |
| Final component is a symlink/reparse point or regular file | Rejects on open and revalidation | Rejects on open and revalidation |
| Child lookup | Checks a single plain name; Unix lookup is relative to retained parent; Windows reopens named path and checks parent before/after | Unix lookup is relative to retained parent; Windows reopens named path; lifecycle callers provide a validated target name and parent is rechecked afterward |
| Ancestor replaced by a link back to the same child inode | A standalone child validates only its own path, so its unchanged identity remains valid | Retains parent ancestry and rejects the substituted ancestor |
| Child crosses a volume boundary | Generic capability permits it | Lifecycle policy rejects it |
| Directory rename while retained on Windows | OS denies deletion/rename sharing | Same |
| Native identity | Full Windows file ID/volume serial or Unix device/inode | Same shared identity primitives |
| Namespace durability | `sync` reopens a write handle on Windows and checks identity before flushing | NTFS admission uses the existing write-through publication protocol; its namespace barrier only revalidates the ordinary directory, never claims directory flush durability |

These are compatible mechanisms with additional admission policy, not two
incompatible authority models. Ancestor retention, same-volume restrictions,
filesystem classification, lifecycle locks, private probe creation, and platform
namespace barriers belong to storage. Exact no-link directory opens and
retained/named identity checks belong to the filesystem capability.

## Shared adversarial evidence

`filesystem_admission::tests::directory_capability_conformance` applies the
same cases to both original implementations before their consolidation:

- open a real directory and reject a regular file without changing its bytes;
- substitute a retained named directory on Unix and reject revalidation;
- require Windows to deny rename of a retained directory, leaving it valid;
- reject final-component symlinks on Unix and junction/reparse points on Windows
  on both direct open and parent-relative child open, preserving outside data;
- on Unix, replace a parent by a symlink back to its displaced directory and
  explicitly distinguish the standalone capability's unchanged-child identity
  from lifecycle's retained-ancestor policy.

The tests live under the existing admission test prefix so the Linux, macOS,
and Windows admission lanes execute them. Windows junction creation is required
and checked; the suite does not skip link cases when a privilege is unavailable.
The Unix-only ancestor mutation reflects Windows' different no-delete-sharing
semantics, which the named-directory replacement case checks directly.

## Consolidation requirements

Lifecycle admission becomes a policy wrapper over `StableDirectory`, retaining
ancestor capabilities and its existing same-volume and barrier rules. One
filesystem revalidation protocol must check both named and retained directory
kind/identity and reject final-component links/reparse points. Storage may map
those failures into its existing phase/cause diagnostics; it must not reimplement
the native checks. Strengthening retained-handle validation in the generic
capability is intentional. No relaxed directory acceptance, fallback reopen, or
new claim of Windows directory-flush durability is permitted.

The existing admission evidence and recovery tests remain the behavioral gate.
The shared cases prove capability behavior; they do not by themselves certify
power-loss recovery or filesystem durability.
