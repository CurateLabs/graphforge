# ADR 0020: NTFS write-through namespace durability

**Status:** Accepted
**Date:** 2026-08-16
**Build target:** v0.5.x (M6 native filesystem admission)
**Decision approval:** Maintainer-approved on 2026-08-16
**Amends:** ADR 0013 (Windows publication primitive), ADR 0018 (Windows
acknowledgement and filesystem scope)
**Related:** issue #779, parent #776, lifecycle integration #780

## Context

ADR 0013 and ADR 0018 treated `FlushFileBuffers` on a Windows directory handle
as equivalent to POSIX directory `fsync(2)` and listed both NTFS and ReFS as
supported. Microsoft documents neither claim. It does document NTFS metadata
write-through when a file is opened with `FILE_FLAG_WRITE_THROUGH`, and
handle-scoped rename through `SetFileInformationByHandle` and
`FILE_RENAME_INFO`.

GraphForge needs one honest namespace-durability primitive for its bounded
filesystem probe. It must retain race-free no-replace behavior, atomic
replacement, and deterministic reconciliation without weakening
`graphforge-storage`'s unsafe-code prohibition.

## Options considered

1. **Keep directory `FlushFileBuffers` and NTFS/ReFS support.** This preserves
   the old prose but relies on an undocumented directory durability guarantee.
2. **Use `ReplaceFileW` or `MoveFileExW` flags.** `ReplaceFileW` has no supported
   write-through flag, and `MOVEFILE_WRITE_THROUGH` documents write-through for
   copy-and-delete moves rather than the same-volume rename contract required
   here.
3. **Use NTFS write-through staging handles and handle-scoped rename.** This is
   the documented NTFS path and keeps namespace mutation tied to the exact
   flushed source identity.
4. **Claim ReFS by analogy with NTFS.** ReFS exposes compatible identity and
   rename APIs, but no authoritative acknowledgement-time persistence contract
   has been established for this protocol.

## Decision

GraphForge supports Windows durability only on fixed, writable local **NTFS**
volumes whose storage stack honestly honors write-through completion. ReFS is
stable unsupported/unproven and returns `GF_UNSUPPORTED_FILESYSTEM` before the
project root is mutated. The supported OS floor is Windows 10 version 1709 or
Windows Server version 1709 because readonly-safe retained-handle deletion uses
`FileDispositionInfoEx` with `FILE_DISPOSITION_FLAG_IGNORE_READONLY_ATTRIBUTE`.
Older implementations fail with an actionable unsupported-platform error;
GraphForge never clears a readonly attribute shared by a published hard link.

For each probed file publication, GraphForge:

1. creates or reopens the private staging file with
   `FILE_FLAG_WRITE_THROUGH` and without following reparse points;
2. writes and flushes the file contents;
3. opens and identity-verifies a directory guard without `FILE_SHARE_DELETE`,
   then holds it through rename and reconciliation so the directory anchor
   cannot be renamed or substituted;
4. retains the exact source handle and calls `SetFileInformationByHandle` with
   `FILE_RENAME_INFO` using the full normalized target path derived from that
   directory guard and a null `RootDirectory`;
5. uses `FileRenameInfo` with `ReplaceIfExists = FALSE` for atomic first
   creation and `FileRenameInfoEx` with `FILE_RENAME_REPLACE_IF_EXISTS |
   FILE_RENAME_POSIX_SEMANTICS` for atomic replacement, retaining the old
   target handle while making new opens of the target name resolve to the
   replacement; and
6. reconciles the retained source identity, retained target identity when
   replacing, and both source and target names after success or any reported
   error.

`ReplaceFileW` and `FlushFileBuffers` on a directory handle are not durability
authority. POSIX keeps file `fsync(2)` plus directory `fsync(2)`. The shared
contract term is therefore **platform-native namespace durability barrier**:
POSIX directory `fsync(2)`, or the NTFS write-through handle rename above.

Unsafe Windows FFI remains isolated in `graphforge-filesystem`.
`graphforge-storage` consumes only its safe Rust interface. Routing all project
lifecycle call sites through these primitives belongs to #780; #779 proves the
native backend and fail-closed admission contract.

## Consequences

- Windows support is narrower but evidence-backed: NTFS only, with ReFS and all
  other classes rejected as unsupported/unproven.
- The no-replace operation remains race-free because the operating system, not
  a pathname precheck, decides destination existence.
- Replacement and no-replace failures cannot be reported as clean failures
  until source/target identities and names reconcile; otherwise the result is
  state-unknown and callers must reconcile authority.
- POSIX acknowledgement remains unchanged and still requires directory
  `fsync(2)` for changed entries.
- GraphForge cannot prove durability if a drive, controller, hypervisor, or
  filesystem falsely acknowledges write-through completion. Such storage is
  outside the supported contract even when the volume reports `NTFS`.

## Required verification

- deterministic classifier coverage accepts only fixed writable NTFS and
  rejects ReFS with `GF_UNSUPPORTED_FILESYSTEM`;
- Windows-native tests cover write-through handle creation, same-handle
  no-replace and replacement rename, competing destinations, and identity/name
  reconciliation;
- Linux/macOS tests retain file and directory `fsync` behavior; and
- CI policy requires the Windows NTFS and macOS native probe jobs under the
  exact-head merge gate.
