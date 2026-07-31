variable "resolved_json" {
  description = "Canonical graphforge-resolved-config/1 JSON. Secret references are allowed; secret values are forbidden."
  type        = string
  sensitive   = true
}

variable "target" {
  description = "Stable identifier of the named GraphForge target to validate."
  type        = string

  validation {
    condition     = can(regex("^[a-z][a-z0-9]*(?:[-_][a-z0-9]+)*$", var.target)) && length(var.target) <= 64
    error_message = "target must be a GraphForge stable identifier."
  }
}
