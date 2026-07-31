package provider

import (
	"context"

	"github.com/CurateLabs/graphforge/iac/terraform/provider/internal/validation"
	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/datasource/schema"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

var _ datasource.DataSource = (*infraValidationDataSource)(nil)

type infraValidationDataSource struct{}

type infraValidationModel struct {
	ResolvedJSON            types.String `tfsdk:"resolved_json"`
	Target                  types.String `tfsdk:"target"`
	Contract                types.String `tfsdk:"contract"`
	ResolvedConfigSHA256    types.String `tfsdk:"resolved_config_sha256"`
	SelectedTargetJSON      types.String `tfsdk:"selected_target_json"`
	StaticValidity          types.String `tfsdk:"static_validity"`
	PlannedInfrastructure   types.String `tfsdk:"planned_infrastructure"`
	Connectivity            types.String `tfsdk:"connectivity"`
	Readiness               types.String `tfsdk:"readiness"`
	CapabilityCompatibility types.String `tfsdk:"capability_compatibility"`
	ValidationJSON          types.String `tfsdk:"validation_json"`
}

func NewInfraValidationDataSource() datasource.DataSource {
	return &infraValidationDataSource{}
}

func (d *infraValidationDataSource) Metadata(
	_ context.Context,
	request datasource.MetadataRequest,
	response *datasource.MetadataResponse,
) {
	response.TypeName = request.ProviderTypeName + "_infra_validation"
}

func (d *infraValidationDataSource) Schema(
	_ context.Context,
	_ datasource.SchemaRequest,
	response *datasource.SchemaResponse,
) {
	response.Schema = schema.Schema{
		Description: "Validates one named GraphForge target offline without provisioning or probing it.",
		Attributes: map[string]schema.Attribute{
			"resolved_json": schema.StringAttribute{
				Required:    true,
				Sensitive:   true,
				Description: "Canonical graphforge-resolved-config/1 JSON. It is never copied to an output.",
			},
			"target": schema.StringAttribute{
				Required:    true,
				Description: "Stable identifier of the target to validate.",
			},
			"contract": schema.StringAttribute{
				Computed:    true,
				Description: "Validation receipt contract.",
			},
			"resolved_config_sha256": schema.StringAttribute{
				Computed:    true,
				Description: "SHA-256 of canonical resolved configuration JSON.",
			},
			"selected_target_json": schema.StringAttribute{
				Computed:    true,
				Description: "Canonical selected target JSON containing references, never secret values or data.",
			},
			"static_validity": schema.StringAttribute{
				Computed:    true,
				Description: "Static configuration state; valid on successful read.",
			},
			"planned_infrastructure": schema.StringAttribute{
				Computed:    true,
				Description: "Provider-neutral plan state; validated without mutation.",
			},
			"connectivity": schema.StringAttribute{
				Computed:    true,
				Description: "Live connectivity state; always not_checked by this provider.",
			},
			"readiness": schema.StringAttribute{
				Computed:    true,
				Description: "Live readiness state; always not_checked by this provider.",
			},
			"capability_compatibility": schema.StringAttribute{
				Computed:    true,
				Description: "Capability state; requirements_declared, not a live compatibility claim.",
			},
			"validation_json": schema.StringAttribute{
				Computed:    true,
				Description: "Canonical graphforge-infra-validation/1 receipt JSON.",
			},
		},
	}
}

func (d *infraValidationDataSource) Read(
	ctx context.Context,
	request datasource.ReadRequest,
	response *datasource.ReadResponse,
) {
	var config infraValidationModel
	response.Diagnostics.Append(request.Config.Get(ctx, &config)...)
	if response.Diagnostics.HasError() {
		return
	}
	if config.ResolvedJSON.IsNull() || config.ResolvedJSON.IsUnknown() ||
		config.Target.IsNull() || config.Target.IsUnknown() {
		response.Diagnostics.AddError(
			"GraphForge target validation requires known inputs",
			"resolved_json and target must both be known during plan.",
		)
		return
	}

	result, err := validation.Validate(config.ResolvedJSON.ValueString(), config.Target.ValueString())
	if err != nil {
		// Errors are deliberately bounded and never include resolved input,
		// secret values, source URIs, or selected target payloads.
		response.Diagnostics.AddError("Invalid GraphForge infrastructure target", err.Error())
		return
	}

	state := infraValidationModel{
		ResolvedJSON:            config.ResolvedJSON,
		Target:                  config.Target,
		Contract:                types.StringValue(result.Contract),
		ResolvedConfigSHA256:    types.StringValue(result.ResolvedConfigSHA256),
		SelectedTargetJSON:      types.StringValue(result.SelectedTargetJSON),
		StaticValidity:          types.StringValue(result.StaticValidity.Status),
		PlannedInfrastructure:   types.StringValue(result.PlannedInfrastructure.Status),
		Connectivity:            types.StringValue(result.Connectivity.Status),
		Readiness:               types.StringValue(result.Readiness.Status),
		CapabilityCompatibility: types.StringValue(result.CapabilityCompatibility.Status),
		ValidationJSON:          types.StringValue(result.JSON),
	}
	response.Diagnostics.Append(response.State.Set(ctx, &state)...)
}
