locals {
  decoded = try(nonsensitive(jsondecode(var.resolved_json)), {})
  locator_input_safe = nonsensitive(
    length(var.artifact_locator) >= 1 &&
    length(var.artifact_locator) <= 2048 &&
    trimspace(var.artifact_locator) == var.artifact_locator &&
    !can(regex("[[:cntrl:][:space:]]", var.artifact_locator)) &&
    !strcontains(var.artifact_locator, "\\") &&
    !startswith(var.artifact_locator, "/") &&
    !startswith(var.artifact_locator, "./") &&
    !startswith(var.artifact_locator, "../") &&
    !startswith(var.artifact_locator, "~") &&
    !startswith(lower(var.artifact_locator), "file:") &&
    !can(regex("^[A-Za-z]:", var.artifact_locator)) &&
    !can(regex("^[A-Za-z][A-Za-z0-9+.-]*://[^/]*@", var.artifact_locator)) &&
    !strcontains(var.artifact_locator, "?") &&
    !strcontains(var.artifact_locator, "#")
  )
  # Invalid values stay redacted while the output precondition reports the
  # bounded error. Only a credential-free locator becomes a non-secret spec.
  artifact_locator = local.locator_input_safe ? nonsensitive(var.artifact_locator) : "<invalid>"

  selected_targets = [
    for candidate in try(local.decoded.targets, []) : candidate
    if try(candidate.id, "") == var.target
  ]
  selected = try(one(local.selected_targets), {})

  artifact           = try(local.selected.artifact, {})
  artifact_kind      = try(local.artifact.kind, "")
  artifact_sha       = try(local.artifact.sha256, "")
  artifact_sha_valid = can(regex("^[0-9a-f]{64}$", local.artifact_sha))
  # Never interpolate an unvalidated value into a regular expression. The
  # sentinel is deliberately valid-shaped but cannot match an invalid pin.
  artifact_sha_pattern = local.artifact_sha_valid ? local.artifact_sha : "0000000000000000000000000000000000000000000000000000000000000000"
  topology             = try(local.selected.topology, {})
  target_kind          = try(local.selected.kind, "")
  source_ids           = try(local.selected.source_ids, [])
  secret_ids           = try(local.selected.secret_ids, [])
  capabilities         = try(local.selected.capabilities, [])

  known_source_ids = [for source in try(local.decoded.sources, []) : try(source.id, "")]
  known_secret_ids = [for secret in try(local.decoded.secrets, []) : try(secret.id, "")]
  selected_sources = [
    for source in try(local.decoded.sources, []) : source
    if contains(local.source_ids, try(source.id, ""))
  ]

  stable_source_ids = (
    length(local.source_ids) <= 256 &&
    length(distinct(local.source_ids)) == length(local.source_ids) &&
    alltrue([for id in local.source_ids : can(regex("^[a-z][a-z0-9]*(?:[-_][a-z0-9]+)*$", id)) && length(id) <= 64])
  )
  stable_secret_ids = (
    length(local.secret_ids) <= 128 &&
    length(distinct(local.secret_ids)) == length(local.secret_ids) &&
    alltrue([for id in local.secret_ids : can(regex("^[a-z][a-z0-9]*(?:[-_][a-z0-9]+)*$", id)) && length(id) <= 64])
  )
  references_exist = (
    alltrue([for id in local.source_ids : contains(local.known_source_ids, id)]) &&
    alltrue([for id in local.secret_ids : contains(local.known_secret_ids, id)])
  )

  reference_contract_safe = (
    alltrue([
      for secret in try(local.decoded.secrets, []) :
      length(keys(secret)) == 2 &&
      length(setsubtract(toset(keys(secret)), toset(["id", "source"]))) == 0
    ]) &&
    alltrue([
      for source in local.selected_sources :
      try(length(source.uri), 0) <= 2048 &&
      try(length(source.uri), 0) >= 1 &&
      !can(regex("^[A-Za-z][A-Za-z0-9+.-]*://[^/]*@", try(source.uri, "")))
    ])
  )

  target_contract_safe = (
    contains(["embedded", "service", "worker", "job", "host"], local.target_kind) &&
    contains(["embedded", "local", "external"], try(local.selected.ownership, "")) &&
    contains(["process", "container", "host"], try(local.topology.execution, "")) &&
    contains(["long_running", "on_demand"], try(local.topology.scheduling, "")) &&
    try(local.topology.replicas, 0) >= 1 &&
    try(local.topology.replicas, 0) <= 1024
  )

  artifact_safe = (
    contains(["python_wheel", "node_package", "native_binary", "oci_image"], local.artifact_kind) &&
    try(length(local.artifact.version), 0) >= 1 &&
    try(length(local.artifact.version), 0) <= 128 &&
    local.artifact_sha_valid
  )
  locator_safe = local.artifact_kind == "oci_image" ? (
    can(regex("^[a-z0-9][a-z0-9.-]*(:[1-9][0-9]{0,4})?/[a-z0-9][a-z0-9._-]*(/[a-z0-9][a-z0-9._-]*)*@sha256:${local.artifact_sha_pattern}$", local.artifact_locator))
    ) : (
    startswith(local.artifact_locator, "https://") &&
    can(regex("^https://[^/@]+(/|$)", local.artifact_locator)) &&
    !can(regex("^https://[^/]*@", local.artifact_locator))
  )

  requirements = {
    backup        = try(local.selected.backup, {})
    health        = try(local.selected.health, {})
    network       = try(local.selected.network, {})
    observability = try(local.selected.observability, {})
    resources     = try(local.selected.resources, {})
    storage       = try(local.selected.storage, {})
    write         = try(local.selected.write, {})
  }

  deployment_spec = {
    artifact = {
      kind    = local.artifact_kind
      locator = local.artifact_locator
      sha256  = local.artifact_sha
      version = try(local.artifact.version, "")
    }
    capability_compatibility = {
      requirements = local.capabilities
      status       = "requirements_declared"
    }
    connectivity = {
      status = "not_checked"
    }
    contract = "graphforge-deployment-spec/1"
    bindings = {
      secret_ids = local.secret_ids
      source_ids = local.source_ids
    }
    infrastructure = {
      mutation = "none"
      status   = "caller_owned"
    }
    ownership = {
      data           = "external"
      infrastructure = "caller_owned"
      runtime        = "caller_owned"
      specification  = "graphforge"
    }
    readiness              = { status = "not_checked" }
    resolved_config_sha256 = sha256(jsonencode(local.decoded))
    requirements           = local.requirements
    target_id              = var.target
    topology = {
      execution  = try(local.topology.execution, "")
      kind       = local.target_kind
      ownership  = try(local.selected.ownership, "")
      replicas   = try(local.topology.replicas, 0)
      scheduling = try(local.topology.scheduling, "")
    }
  }
}
