---
title: "ADR 0064: Unverified filesystem admission"
adr: "0064"
status: "Accepted"
date: "2026-11-07"
superseded_by: null
revisit_when: "Filesystem qualification gains crash/power-loss evidence, or a platform cannot retain stable mount and lock identities"
---

# ADR 0064: Unverified filesystem admission

The maintainer decision in [#2332](https://github.com/CurateLabs/graphforge/issues/2332#issuecomment-6707719077)
amends filesystem classification in ADRs 0013, 0018 and 0020. Qualification is
separate from proving a broken primitive or detecting namespace substitution.

Rust admits unproven filesystem classes by default only after the full
publication primitive probe succeeds. `GRAPHFORGE_FILESYSTEM_DURABILITY=warn`
(default) emits `GF_UNVERIFIED_FILESYSTEM` once per open, never per write;
`strict` retains `GF_UNSUPPORTED_FILESYSTEM` refusal. Other values return
`GF_VALIDATION`. In-memory projects skip qualification and never warn.
Read-only volumes, failed native queries, broken primitives, links, namespace
changes and lock takeover always refuse. No fallback engine is introduced.

Linux compares retained `statx` mount IDs for file-versus-directory mount
identity: split-device overlayfs may give different `st_dev` on one mount.
Kernels without mount-ID support retain the strict device comparison. Native
file identity, link counts and named-versus-open identity checks remain intact.

Each immutable generation optionally records `unverified_durability` from its
writer's admission, like `producing_core_version`. A later qualified writer
omits the field; historical manifests never change. Open-time warnings report
the current mount independently of the selected generation's writer stamp.
Portable packages authenticate optional `source_unverified_durability`, exposed
by verification, export and publish receipts. Absent fields are omitted so
qualified projects and packages keep their serialization. Older pre-v1 readers
may refuse newly stamped metadata under existing format policy (ADR 0054).

Colab and Kaggle session disks can run with this warning; tmpfs loses data at
reboot and VM disks disappear when runtimes recycle. Export to persistent
storage. Network/FUSE mounts may still fail primitive checks. Local tmpfs and
split-device overlay write/reopen tests do not substitute for a connected
Colab acceptance run; that runtime evidence remains required on the issue.
