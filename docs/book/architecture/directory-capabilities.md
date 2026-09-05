# Directory capability and lifecycle admission boundary

Issue #1017 consolidates directory identity checks while retaining storage's
additional lifecycle policy. This is a native filesystem boundary, independent
of query semantics or project-format migration.

## Threat model before consolidation

`graphforge-filesystem::StableDirectory` and storage's private
`LifecycleDirectory` both retained a directory handle and its native
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

The test-only baseline was committed before replacing either mechanism. At
`edbf4336`, the [Windows native lane](https://github.com/CurateLabs/graphforge/actions/runs/33927328124/job/101199483840)
passed all 24 admission tests (including the three applicable shared cases) and
25 filesystem tests; the [macOS native lane](https://github.com/CurateLabs/graphforge/actions/runs/33927328124/job/101199483884)
also passed. Linux passed the four shared cases, all 27 admission tests and all
15 filesystem tests before consolidation. The host's `/tmp` was tmpfs, so Linux
durable-admission tests used an explicit `TMPDIR` on the host's admitted ext4
volume.

## Consolidated implementation

Lifecycle admission is a policy wrapper over `StableDirectory`, retaining
ancestor capabilities and its existing same-volume and barrier rules.
`ProbeDirectory` also delegates its directory validation to the same capability.
The duplicate `RetainedDirectory` type, directory-open flags, link/reparse
predicate, and storage directory revalidation bodies are removed.

`StableDirectory::revalidate_named_detailed` is the sole native directory
revalidation protocol: it checks named and retained metadata, ordinary directory
kind, link/reparse status, and named/retained identity against the captured
identity. Typed failure stages let storage preserve its phase/cause diagnostics;
the ordinary I/O adapter retains the native error kind. Initial admission still
performs its policy-specific prechecks before adopting a retained handle.
Adoption accepts an opaque `OpenedDirectoryHandle` constructed by the shared
native open primitive, not an arbitrary `File`: metadata and identity alone
cannot prove that a Windows handle denies delete sharing.

The stronger retained-handle check is intentional. Lifecycle child opening now
also uses the capability's validated single-component name and before/after
parent checks; ancestry cloning revalidates the capability before and after
cloning. These additional checks can reject a concurrent substitution earlier.
The retained-ancestor rule remains storage policy: a standalone child still
validates its own identity, while lifecycle validates the retained ancestry.
No filesystem class, same-volume restriction, cooperative lock, probe evidence
field, or namespace barrier changes. In particular, admission does not call the
generic Windows directory `sync` operation or claim directory-flush durability.

The existing admission evidence and recovery tests remain the behavioral gate.
The shared cases prove capability behavior; they do not by themselves certify
power-loss recovery or filesystem durability.
