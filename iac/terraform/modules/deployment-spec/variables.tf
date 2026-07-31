variable "resolved_json" {
  description = "Canonical, secret-free graphforge-resolved-config/1 JSON. The module selects one target and never emits the original document."
  type        = string
  sensitive   = true
}

variable "target" {
  description = "Stable identifier of the GraphForge deployment target."
  type        = string

  validation {
    condition     = can(regex("^[a-z][a-z0-9]*(?:[-_][a-z0-9]+)*$", var.target)) && length(var.target) <= 64
    error_message = "target must be a GraphForge stable identifier."
  }
}

variable "artifact_locator" {
  description = "Immutable public artifact locator. OCI images must use repository@sha256:<digest>; other artifacts must use HTTPS."
  type        = string
  sensitive   = true
}
