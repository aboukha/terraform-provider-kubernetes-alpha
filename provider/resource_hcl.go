package provider

import (
	"context"
	"fmt"

	"github.com/zclconf/go-cty/cty"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
)

// PlanUpdateResourceHCLServerSide calculates the state for a new resource based on HCL manifest
func PlanUpdateResourceHCLServerSide(ctx context.Context, m *cty.Value) (*cty.Value, error) {
	po, err := CtyObjectToUnstructured(m)
	if err != nil {
		return nil, err
	}
	uo := unstructured.Unstructured{Object: po}

	gvr, err := GVRFromCtyUnstructured(&uo)
	if err != nil {
		return nil, err
	}

	ns, err := IsResourceNamespaced(gvr)
	if err != nil {
		return nil, err
	}

	rnamespace := uo.GetNamespace()
	rname := uo.GetName()

	c, err := GetDynamicClient()
	if err != nil {
		return nil, err
	}

	var r dynamic.ResourceInterface
	if ns {
		r = c.Resource(gvr).Namespace(rnamespace)
	} else {
		r = c.Resource(gvr)
	}
	jr, err := uo.MarshalJSON()
	if err != nil {
		return nil, err
	}
	ro, err := r.Patch(ctx,
		rname,
		types.ApplyPatchType,
		jr,
		v1.PatchOptions{
			DryRun:       []string{v1.DryRunAll},
			FieldManager: "Terraform",
		})
	if err != nil {
		return nil, fmt.Errorf("update dry-run for %s failed: %s",
			types.NamespacedName{Namespace: rnamespace, Name: rname}, err)
	}

	nobj, err := UnstructuredToCty(FilterEphemeralFields(ro.Object))
	if err != nil {
		return nil, err
	}
	return &nobj, nil
}

// FilterEphemeralFields removes certain fields from an API response object
// which would otherwise cause a false diff
func FilterEphemeralFields(in map[string]interface{}) map[string]interface{} {
	// Remove "status" attribute
	delete(in, "status")

	meta := in["metadata"].(map[string]interface{})

	// Remove "uid", "creationTimestamp", "resourceVersion" as
	// they change with most resource operations
	delete(meta, "uid")
	delete(meta, "creationTimestamp")
	delete(meta, "resourceVersion")

	// TODO: we should be filtering API responses based on the contents of 'managedFields'
	// and only retain the attributes for which the manager is Terraform
	delete(meta, "managedFields")

	return in
}
