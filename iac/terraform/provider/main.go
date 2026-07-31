// The GraphForge Terraform provider is an offline, validation-only provider.
package main

import (
	"context"
	"flag"
	"log"

	"github.com/CurateLabs/graphforge/iac/terraform/provider/internal/provider"
	"github.com/hashicorp/terraform-plugin-framework/providerserver"
)

var version = "dev"

func main() {
	var debug bool
	flag.BoolVar(&debug, "debug", false, "run the provider with debugger support")
	flag.Parse()

	err := providerserver.Serve(
		context.Background(),
		provider.New(version),
		providerserver.ServeOpts{
			Address: "registry.terraform.io/curatelabs/graphforge",
			Debug:   debug,
		},
	)
	if err != nil {
		log.Fatal(err)
	}
}
