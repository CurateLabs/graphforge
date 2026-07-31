terraform {
  required_version = ">= 1.8.0"

  required_providers {
    graphforge = {
      source  = "curatelabs/graphforge"
      version = "~> 0.5"
    }
  }
}
