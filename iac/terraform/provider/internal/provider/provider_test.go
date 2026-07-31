package provider

import (
	"context"
	"testing"

	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/provider"
)

func TestProviderHasOneDataSourceAndNoResources(t *testing.T) {
	instance := New("test")()
	var metadata provider.MetadataResponse
	instance.Metadata(context.Background(), provider.MetadataRequest{}, &metadata)
	if metadata.TypeName != "graphforge" || metadata.Version != "test" {
		t.Fatalf("unexpected metadata: %#v", metadata)
	}
	if resources := instance.Resources(context.Background()); len(resources) != 0 {
		t.Fatalf("provider exposes %d mutation resources", len(resources))
	}
	dataSources := instance.DataSources(context.Background())
	if len(dataSources) != 1 {
		t.Fatalf("provider exposes %d data sources", len(dataSources))
	}
	var response datasource.MetadataResponse
	dataSources[0]().Metadata(
		context.Background(),
		datasource.MetadataRequest{ProviderTypeName: "graphforge"},
		&response,
	)
	if response.TypeName != "graphforge_infra_validation" {
		t.Fatalf("unexpected data source name: %q", response.TypeName)
	}
}
