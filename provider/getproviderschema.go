package provider

import (
	"context"
	"fmt"

	"github.com/hashicorp/terraform-plugin-go/tfprotov5"
)

// GetProviderSchema function
func (s *RawProviderServer) GetProviderSchema(ctx context.Context, req *tfprotov5.GetProviderSchemaRequest) (*tfprotov5.GetProviderSchemaResponse, error) {

	cfgSchema := GetProviderConfigSchema()

	resSchema, err := GetProviderResourceSchema()
	if err != nil {
		return nil, fmt.Errorf("failed to construct resource schema: %s", err)
	}
	resp := &tfprotov5.GetProviderSchemaResponse{
		Provider:        cfgSchema,
		ResourceSchemas: resSchema,
	}
	return resp, nil
}
