package provider

import (
	"context"

	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/provider"
	providerschema "github.com/hashicorp/terraform-plugin-framework/provider/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource"
)

var _ provider.Provider = (*graphForgeProvider)(nil)

type graphForgeProvider struct {
	version string
}

// New returns an offline provider factory. The provider has no configuration,
// credentials, clients, resources, or network behavior.
func New(version string) func() provider.Provider {
	return func() provider.Provider {
		return &graphForgeProvider{version: version}
	}
}

func (p *graphForgeProvider) Metadata(
	_ context.Context,
	_ provider.MetadataRequest,
	response *provider.MetadataResponse,
) {
	response.TypeName = "graphforge"
	response.Version = p.version
}

func (p *graphForgeProvider) Schema(
	_ context.Context,
	_ provider.SchemaRequest,
	response *provider.SchemaResponse,
) {
	response.Schema = providerschema.Schema{
		Description: "Offline, read-only validation of GraphForge resolved target configuration.",
	}
}

func (p *graphForgeProvider) Configure(
	_ context.Context,
	_ provider.ConfigureRequest,
	_ *provider.ConfigureResponse,
) {
}

func (p *graphForgeProvider) Resources(context.Context) []func() resource.Resource {
	return nil
}

func (p *graphForgeProvider) DataSources(context.Context) []func() datasource.DataSource {
	return []func() datasource.DataSource{NewInfraValidationDataSource}
}
