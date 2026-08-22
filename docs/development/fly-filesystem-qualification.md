# Fly filesystem qualification

This issue-882 probe answers only whether GraphForge admits a Fly volume as its process work root. It never runs or authorizes S24, S25, SCALE26, or another full certification.

## Prepare and review

Use a clean checkout at the exact commit to test. Build `containers/fly-filesystem-qualification/Dockerfile`, push it to a private registry, and resolve the pushed image to its immutable `sha256` digest. Authenticate `flyctl` without putting a token in a command or artifact.

Review the no-spend plan first (dry-run is the default):

```bash
python3 scripts/fly-filesystem-qualification.py \
  --expected-sha "$GIT_SHA" \
  --image "registry.example/graphforge@sha256:<64-hex-digest>" \
  --region den --org curatelabs \
  --app-name gf-fs-qual-<suffix> \
  --volume-name gf_fs_vol_<suffix> \
  --machine-name gf-fs-machine-<suffix>
```

The controller refuses a dirty tree, a different HEAD, a mutable image, a missing fixed region, unsafe names, or an existing app name. It creates the Machine through Fly's Machines API so the exact OCI digest is submitted without flyctl rewriting it; the token is obtained in memory and is never written to a command, file, log, or evidence. The plan has one small volume mounted at `/work`, a small explicitly sized performance Machine (2 CPUs and 4 GiB by default), no service or public port, restart policy `no`, and automatic destruction. The 128 GiB certification ceiling is not a qualification default or a sizing target. A later ladder or SCALE26 Machine must be selected from measured phase-level peak-RSS headroom and plateau evidence; continued material RSS growth with edge count is an architectural failure, not a reason to buy more RAM. The 500 GB certification envelope is expected to be storage/IO constrained and is outside this probe.

## Run the disposable probe

Only after approving the dry-run, repeat the command with `--execute --confirm-disposable`. This creates billable resources. The container bounds the Rust smoke to 900 seconds and remains alive for at most another 300 seconds for evidence retrieval. The controller validates the observed Machine config and sanitized evidence before acknowledging container exit.

Cleanup runs on success and failure in Machine, volume, app order. Repeating cleanup is safe. A rejected admission produces a typed code/cause and a non-zero controller exit; it blocks any full run. The committed artifact `fly-qualification-evidence.json` contains no Fly resource ID/name, secret, credential, or absolute path.

Validate a retrieved artifact independently:

```bash
python3 scripts/ci/validate-fly-filesystem-qualification.py \
  fly-qualification-evidence.json \
  --expected-sha "$GIT_SHA" \
  --expected-image-digest "sha256:<64-hex-digest>" \
  --expected-region den
```

After any interrupted operator session, confirm the disposable app is absent with `flyctl apps list`; if present, destroy its Machine first, then its volume, then the app. Never paste the resulting inventory into evidence because it contains provider identifiers.
