package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/davecgh/go-spew/spew"
	"github.com/hashicorp/go-cty/cty"
	"github.com/hashicorp/go-cty/cty/msgpack"
	"github.com/hashicorp/terraform-plugin-go/tfprotov5"
	"github.com/hashicorp/terraform-plugin-go/tfprotov5/tftypes"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/logging"
	"github.com/mitchellh/go-homedir"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/install"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	apimachineryschema "k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

func init() {
	install.Install(scheme.Scheme)
}

// RawProviderServer implements the ProviderServer interface as exported from ProtoBuf.
type RawProviderServer struct{}

// PrepareProviderConfig function
func (s *RawProviderServer) PrepareProviderConfig(ctx context.Context, req *tfprotov5.PrepareProviderConfigRequest) (*tfprotov5.PrepareProviderConfigResponse, error) {
	resp := &tfprotov5.PrepareProviderConfigResponse{}

	resp.Diagnostics = []*tfprotov5.Diagnostic{}

	return resp, nil
}

// ValidateResourceTypeConfig function
func (s *RawProviderServer) ValidateResourceTypeConfig(ctx context.Context, req *tfprotov5.ValidateResourceTypeConfigRequest) (*tfprotov5.ValidateResourceTypeConfigResponse, error) {
	//	Dlog.Printf("[ValidateResourceTypeConfig][Request]\n%s\n", spew.Sdump(*req))

	config := &tfprotov5.ValidateResourceTypeConfigResponse{}
	return config, nil
}

// ValidateDataSourceConfig function
func (s *RawProviderServer) ValidateDataSourceConfig(ctx context.Context, req *tfprotov5.ValidateDataSourceConfigRequest) (*tfprotov5.ValidateDataSourceConfigResponse, error) {
	//	Dlog.Printf("[ValidateDataSourceConfig][Request]\n%s\n", spew.Sdump(*req))

	return nil, status.Errorf(codes.Unimplemented, "method ValidateDataSourceConfig not implemented")
}

// UpgradeResourceState isn't really useful in this provider, but we have to loop the state back through to keep Terraform happy.
func (s *RawProviderServer) UpgradeResourceState(ctx context.Context, req *tfprotov5.UpgradeResourceStateRequest) (*tfprotov5.UpgradeResourceStateResponse, error) {
	resp := &tfprotov5.UpgradeResourceStateResponse{}
	resp.Diagnostics = []*tfprotov5.Diagnostic{}

	sch := GetProviderResourceSchema()

	rt := GetObjectTypeFromSchema(sch[req.TypeName])

	rv := tftypes.Object(map[string]interface{})
	err := json.Unmarshal(req.RawState.JSON, rv)
	if err != nil {
		resp.Diagnostics = append(resp.Diagnostics, &tfprotov5.Diagnostic{
			Severity: tfprotov5.DiagnosticSeverityError,
			Summary:  "Failed to decode old state during upgrade",
			Detail:   err.Error(),
		})
		return resp, nil
	}
	us, err := tfprotov5.NewDynamicValue(rt, rv)
	if err != nil {
		resp.Diagnostics = append(resp.Diagnostics, &tfprotov5.Diagnostic{
			Severity: tfprotov5.DiagnosticSeverityError,
			Summary:  "Failed to encode new state during upgrade",
			Detail:   err.Error(),
		})
	}
	resp.UpgradedState = &us

	return resp, nil
}

// ConfigureProvider function
func (s *RawProviderServer) ConfigureProvider(ctx context.Context, req *tfprotov5.ConfigureProviderRequest) (*tfprotov5.ConfigureProviderResponse, error) {
	response := &tfprotov5.ConfigureProviderResponse{}
	var err error

	ct := getConfigObjectType()
	providerConfig := msgpack.Unmarshal(req.Config.Msgpack)

	diags := []*tfprotov5.Diagnostic{}

	configPath := providerConfig.GetAttr("config_path")
	if !configPath.IsNull() {
		configPathAbs, err := homedir.Expand(configPath.AsString())
		if err == nil {
			_, err = os.Stat(configPathAbs)
		}
		if err != nil {
			diags = append(diags, &tfprotov5.Diagnostic{
				Severity:  tfprotov5.DiagnosticSeverityInvalid,
				Summary:   "Invalid attribute in provider configuration",
				Detail:    "'config_path' refers to an invalid file path: " + configPathAbs,
				Attribute: &tfprotov5.AttributePath{}.WithAttributeName("config_path"),
			})
		}
	}

	host := providerConfig.GetAttr("host")
	if !host.IsNull() && host.IsKnown() {
		_, err = url.ParseRequestURI(host.AsString())
		if err != nil {
			diags = append(diags, &tfprotov5.Diagnostic{
				Severity: tfprotov5.DiagnosticSeverityInvalid,
				Summary:  "Invalid attribute in provider configuration",
				Detail:   "'host' is not a valid URL",
				Attribute: &tfprotov5.AttributePath{
					Steps: []*tfprotov5.AttributePathStep{
						{
							Selector: &tfprotov5.AttributePath_Step_AttributeName{
								AttributeName: "host",
							},
						},
					},
				},
			})
		}
	}

	pemCC := providerConfig.GetAttr("client_certificate")
	if !pemCC.IsNull() && host.IsKnown() {
		cc, _ := pem.Decode([]byte(pemCC.AsString()))
		if cc == nil || cc.Type != "CERTIFICATE" {
			diags = append(diags, &tfprotov5.Diagnostic{
				Severity:  tfprotov5.DiagnosticSeverityInvalid,
				Summary:   "Invalid attribute in provider configuration",
				Detail:    "'client_certificate' is not a valid PEM encoded certificate",
				Attribute: &tfprotov5.AttributePath{}.WithAttributeName("client_certificate"),
			})
		}
	}

	pemCA := providerConfig.GetAttr("cluster_ca_certificate")
	if !pemCA.IsNull() && host.IsKnown() {
		ca, _ := pem.Decode([]byte(pemCA.AsString()))
		if ca == nil || ca.Type != "CERTIFICATE" {
			diags = append(diags, &tfprotov5.Diagnostic{
				Severity:  tfprotov5.DiagnosticSeverityInvalid,
				Summary:   "Invalid attribute in provider configuration",
				Detail:    "'cluster_ca_certificate' is not a valid PEM encoded certificate",
				Attribute: &tfprotov5.AttributePath{}.WithAttributeName("cluster_ca_certificate"),
			})
		}
	}

	pemCK := providerConfig.GetAttr("client_key")
	if !pemCK.IsNull() && host.IsKnown() {
		ck, _ := pem.Decode([]byte(pemCK.AsString()))
		if ck == nil || !strings.Contains(ck.Type, "PRIVATE KEY") {
			diags = append(diags, &tfprotov5.Diagnostic{
				Severity:  tfprotov5.DiagnosticSeverityInvalid,
				Summary:   "Invalid attribute in provider configuration",
				Detail:    "'client_key' is not a valid PEM encoded private key",
				Attribute: &tfprotov5.AttributePath{}.WithAttributeName("client_key"),
			})
		}
	}

	if len(diags) > 0 {
		response.Diagnostics = diags
		return response, errors.New("failed to validate provider configuration")
	}

	ps := GetGlobalState()

	ssp := providerConfig.GetAttr("server_side_planning")
	if !ssp.IsKnown() || ssp.IsNull() {
		ssp = tftypes.NewValue(tftype.Bool) // default to true
	}
	ps[SSPlanning] = ssp.True()

	overrides := &clientcmd.ConfigOverrides{}
	loader := &clientcmd.ClientConfigLoadingRules{}

	if configPathEnv, ok := os.LookupEnv("KUBE_CONFIG_PATH"); ok && configPathEnv != "" {
		configPath = tftypes.NewValue(tftypes.String, configPathEnv)
	} else {
		configPath = providerConfig.GetAttr("config_path")
	}
	if !configPath.IsNull() && configPath.IsKnown() {
		configPathAbs, err := homedir.Expand(configPath.AsString())
		if err != nil {
			return response, fmt.Errorf("cannot load specified config file: %s", err)
		}
		loader.ExplicitPath = configPathAbs
	}

	var cfgContext tftypes.Value
	if cfgContextEnv, ok := os.LookupEnv("KUBE_CTX"); ok && cfgContextEnv != "" {
		cfgContext = tftypes.NewValue(tftypes.String, cfgContextEnv)
	} else {
		cfgContext = providerConfig.GetAttr("config_context")
	}
	if !cfgContext.IsNull() {
		overrides.CurrentContext = cfgContext.AsString()
	}

	overrides.Context = clientcmdapi.Context{}

	var cfgCtxCluster tftypes.Value
	if cfgCtxClusterEnv, ok := os.LookupEnv("KUBE_CTX_CLUSTER"); ok && cfgCtxClusterEnv != "" {
		cfgCtxCluster = tftypes.NewValue(tftypes.String, cfgCtxClusterEnv)
	} else {
		cfgCtxCluster = providerConfig.GetAttr("config_context_cluster")
	}
	if !cfgCtxCluster.IsNull() && cfgCtxCluster.IsKnown() {
		overrides.Context.Cluster = cfgCtxCluster.AsString()
	}

	var cfgContextAuthInfo tftypes.Value
	if cfgContextAuthInfoEnv, ok := os.LookupEnv("KUBE_CTX_USER"); ok && cfgContextAuthInfoEnv != "" {
		cfgContextAuthInfo = tftypes.NewValue(tftypes.String, cfgContextAuthInfoEnv)
	} else {
		cfgContextAuthInfo = providerConfig.GetAttr("config_context_user")
	}
	if !cfgContextAuthInfo.IsNull() && cfgContextAuthInfo.IsKnown() {
		overrides.Context.AuthInfo = cfgContextAuthInfo.AsString()
	}

	var insecure tftypes.Value
	if insecureEnv, ok := os.LookupEnv("KUBE_INSECURE"); ok && insecureEnv != "" {
		iv, err := strconv.ParseBool(insecureEnv)
		if err != nil {
			return response, fmt.Errorf("failed to parse config value of 'insecure': %s", err)
		}
		insecure = tftypes.NewValue(tftypes.Bool, iv)
	} else {
		insecure = providerConfig.GetAttr("insecure")
	}
	if !insecure.IsNull() {
		overrides.ClusterInfo.InsecureSkipTLSVerify = insecure.True()
	}

	var clusterCA tftypes.Value
	if clusterCAEnv, ok := os.LookupEnv("KUBE_CLUSTER_CA_CERT_DATA"); ok && clusterCAEnv != "" {
		clusterCA = tftypes.NewValue(tftypes.String, clusterCAEnv)
	} else {
		clusterCA = providerConfig.GetAttr("cluster_ca_certificate")
	}
	if !clusterCA.IsNull() && clusterCA.IsKnown() {
		overrides.ClusterInfo.CertificateAuthorityData = bytes.NewBufferString(clusterCA.AsString()).Bytes()
	}

	var clientCrt tftypes.Value
	if clientCrtEnv, ok := os.LookupEnv("KUBE_CLIENT_CERT_DATA"); ok && clientCrtEnv != "" {
		clientCrt = tftypes.NewValue(tftypes.String, clientCrtEnv)
	} else {
		clientCrt = providerConfig.GetAttr("client_certificate")
	}
	if !clientCrt.IsNull() && clientCrt.IsKnown() {
		overrides.AuthInfo.ClientCertificateData = bytes.NewBufferString(clientCrt.AsString()).Bytes()
	}

	var clientCrtKey tftypes.Value
	if clientCrtKeyEnv, ok := os.LookupEnv("KUBE_CLIENT_KEY_DATA"); ok && clientCrtKeyEnv != "" {
		clientCrtKey = tftypes.NewValue(tftypes.String, clientCrtKeyEnv)
	} else {
		clientCrtKey = providerConfig.GetAttr("client_key")
	}
	if !clientCrtKey.IsNull() && clientCrtKey.IsKnown() {
		overrides.AuthInfo.ClientKeyData = bytes.NewBufferString(clientCrtKey.AsString()).Bytes()
	}

	if hostEnv, ok := os.LookupEnv("KUBE_HOST"); ok && hostEnv != "" {
		host = tftypes.NewValue(tftypes.String, hostEnv)
	} else {
		host = providerConfig.GetAttr("host")
	}
	if !host.IsNull() && host.IsKnown() {
		// Server has to be the complete address of the kubernetes cluster (scheme://hostname:port), not just the hostname,
		// because `overrides` are processed too late to be taken into account by `defaultServerUrlFor()`.
		// This basically replicates what defaultServerUrlFor() does with config but for overrides,
		// see https://github.com/kubernetes/client-go/blob/v12.0.0/rest/url_utils.go#L85-L87
		hasCA := len(overrides.ClusterInfo.CertificateAuthorityData) != 0
		hasCert := len(overrides.AuthInfo.ClientCertificateData) != 0
		defaultTLS := hasCA || hasCert || overrides.ClusterInfo.InsecureSkipTLSVerify
		hostURL, _, err := rest.DefaultServerURL(host.AsString(), "", apimachineryschema.GroupVersion{}, defaultTLS)
		if err != nil {
			return nil, fmt.Errorf("Failed to parse host: %s", err)
		}
		overrides.ClusterInfo.Server = hostURL.String()
	}

	var username tftypes.Value
	if usernameEnv, ok := os.LookupEnv("KUBE_USERNAME"); ok && usernameEnv != "" {
		username = tftypes.NewValue(tftypes.String, usernameEnv)
	} else {
		username = providerConfig.GetAttr("username")
	}
	if !username.IsNull() {
		overrides.AuthInfo.Username = username.AsString()
	}

	var password tftypes.Value
	if passwordEnv, ok := os.LookupEnv("KUBE_PASSWORD"); ok && passwordEnv != "" {
		password = tftypes.NewValue(tftypes.String, passwordEnv)
	} else {
		password = providerConfig.GetAttr("password")
	}
	if !password.IsNull() {
		overrides.AuthInfo.Password = password.AsString()
	}

	var token tftypes.Value
	if tokenEnv, ok := os.LookupEnv("KUBE_TOKEN"); ok && tokenEnv != "" {
		token = tftypes.NewValue(tftypes.String, tokenEnv)
	} else {
		token = providerConfig.GetAttr("token")
	}
	if !token.IsNull() && token.IsKnown() {
		overrides.AuthInfo.Token = token.AsString()
	}

	execObj := providerConfig.GetAttr("exec")
	if !execObj.IsNull() && execObj.IsKnown() {
		execCfg := clientcmdapi.ExecConfig{}
		apiv := execObj.GetAttr("api_version")
		if !apiv.IsNull() {
			execCfg.APIVersion = apiv.AsString()
		}
		cmd := execObj.GetAttr("command")
		if !cmd.IsNull() {
			execCfg.Command = cmd.AsString()
		}
		xcmdArgs := execObj.GetAttr("args")
		if !xcmdArgs.IsNull() && xcmdArgs.LengthInt() > 0 {
			execCfg.Args = make([]string, 0, xcmdArgs.LengthInt())
			for ait := xcmdArgs.ElementIterator(); ait.Next(); {
				_, v := ait.Element()
				execCfg.Args = append(execCfg.Args, v.AsString())
			}
		}
		xcmdEnvs := execObj.GetAttr("env")
		if !xcmdEnvs.IsNull() && xcmdEnvs.LengthInt() > 0 {
			execCfg.Env = make([]clientcmdapi.ExecEnvVar, 0, xcmdEnvs.LengthInt())
			for eit := xcmdEnvs.ElementIterator(); eit.Next(); {
				k, v := eit.Element()
				execCfg.Env = append(execCfg.Env, clientcmdapi.ExecEnvVar{
					Name:  k.AsString(),
					Value: v.AsString(),
				})
			}
		}
		overrides.AuthInfo.Exec = &execCfg
	}

	cc := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loader, overrides)
	clientConfig, err := cc.ClientConfig()
	if err != nil {
		Dlog.Printf("[Configure] Failed to load config:\n%s\n", spew.Sdump(cc))
		if errors.Is(err, clientcmd.ErrEmptyConfig) {
			// this is a terrible fix for if the configuration is a calculated value
			return response, nil
		}
		return response, fmt.Errorf("cannot load Kubernetes client config: %s", err)
	}

	Dlog.Printf("[Configure][ClientConfig] %s\n", spew.Sdump(*clientConfig))

	if logging.IsDebugOrHigher() {
		clientConfig.WrapTransport = func(rt http.RoundTripper) http.RoundTripper {
			return logging.NewTransport("Kubernetes", rt)
		}
	}

	codec := runtime.NoopEncoder{Decoder: scheme.Codecs.UniversalDecoder()}
	clientConfig.NegotiatedSerializer = serializer.NegotiatedSerializerWrapper(runtime.SerializerInfo{Serializer: codec})

	ps[ClientConfig] = clientConfig

	return response, nil
}

// ReadResource function
func (s *RawProviderServer) ReadResource(ctx context.Context, req *tfprotov5.ReadResourceRequest) (*tfprotov5.ReadResourceResponse, error) {
	resp := &tfprotov5.ReadResourceResponse{}

	currentState, err := UnmarshalResource(req.TypeName, req.GetCurrentState().GetMsgpack())
	if err != nil {
		return resp, fmt.Errorf("Failed to extract resource from current state: %#v", err)
	}
	if !currentState.Type().HasAttribute("object") {
		return resp, fmt.Errorf("existing state of resource %s has no 'object' attribute", req.TypeName)
	}

	co := currentState.GetAttr("object")
	mo, err := CtyObjectToUnstructured(&co)
	if err != nil {
		return resp, fmt.Errorf("failed to convert current state to unstructured: %s", err)
	}

	uo := unstructured.Unstructured{Object: mo}
	client, err := GetDynamicClient()
	if err != nil {
		return resp, err
	}
	cGVR, err := GVRFromCtyUnstructured(&uo)
	if err != nil {
		return resp, err
	}
	ns, err := IsResourceNamespaced(cGVR)
	if err != nil {
		return resp, err
	}
	rcl := client.Resource(cGVR)

	rnamespace := uo.GetNamespace()
	rname := uo.GetName()

	var ro *unstructured.Unstructured
	if ns {
		ro, err = rcl.Namespace(rnamespace).Get(ctx, rname, v1.GetOptions{})
	} else {
		ro, err = rcl.Get(ctx, rname, v1.GetOptions{})
	}
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return resp, nil
		}
		d := tfprotov5.Diagnostic{
			Severity: tfprotov5.DiagnosticSeverityError,
			Summary:  fmt.Sprintf("Cannot GET resource %s", spew.Sdump(co)),
			Detail:   err.Error(),
		}
		resp.Diagnostics = append(resp.Diagnostics, &d)
		return resp, err
	}

	gvk, err := GVKFromCtyObject(&co)
	if err != nil {
		return resp, fmt.Errorf("failed to determine resource GVR: %s", err)
	}

	tsch, err := resourceTypeFromOpenAPI(gvk)
	if err != nil {
		return resp, fmt.Errorf("failed to determine resource type ID: %s", err)
	}

	fo := FilterEphemeralFields(ro.Object)
	nobj, err := UnstructuredToCty(fo, tsch)
	if err != nil {
		return resp, err
	}

	newstate, err := cty.Transform(currentState, ResourceDeepUpdateObjectAttr(cty.GetAttrPath("object"), &nobj))
	if err != nil {
		return resp, err
	}
	newStatePacked, err := MarshalResource(req.TypeName, &newstate)
	if err != nil {
		return resp, err
	}
	resp.NewState = &tfprotov5.DynamicValue{Msgpack: newStatePacked}
	return resp, nil
}

// PlanResourceChange function
func (s *RawProviderServer) PlanResourceChange(ctx context.Context, req *tfprotov5.PlanResourceChangeRequest) (*tfprotov5.PlanResourceChangeResponse, error) {
	resp := &tfprotov5.PlanResourceChange_Response{}

	proposedState, err := UnmarshalResource(req.TypeName, req.GetProposedNewState().GetMsgpack())
	if err != nil {
		return resp, fmt.Errorf("Failed to extract resource from proposed plan: %#v", err)
	}

	priorState, err := UnmarshalResource(req.TypeName, req.GetPriorState().GetMsgpack())
	if err != nil {
		return resp, fmt.Errorf("Failed to extract resource from prior state: %#v", err)
	}

	if proposedState.IsNull() {
		// we plan to delete the resource
		if !priorState.Type().HasAttribute("object") {
			return resp, fmt.Errorf("cannot find existing object state before delete")
		}
		resp.PlannedState = req.ProposedNewState
		return resp, nil
	}

	ps := GetGlobalState()
	var planned tftypes.Value

	if ps[SSPlanning].(bool) {
		planned, err = PlanUpdateResourceServerSide(ctx, &proposedState)
	} else {
		planned, err = PlanUpdateResourceLocal(ctx, &proposedState)
	}

	Dlog.Printf("[PlanResourceChange] planned state: %s\n", spew.Sdump(planned))

	if err != nil {
		resp.Diagnostics = append(resp.Diagnostics,
			&tfprotov5.Diagnostic{
				Severity: tfprotov5.DiagnosticSeverityError,
				Summary:  err.Error(),
			})
		return resp, err
	}

	plannedState, err := MarshalResource(req.TypeName, &planned)
	if err != nil {
		return resp, err
	}

	resp.PlannedState = &tfprotov5.DynamicValue{
		Msgpack: plannedState,
	}
	return resp, nil
}

/*
func (s *RawProviderServer) waitForCompletion(ctx context.Context, applyPlannedState tftypes.Value, rs dynamic.ResourceInterface, rname string, rtype tftypes.Type) error {
	if applyPlannedState.IsNull() {
		return nil
	}
	waitForBlock := applyPlannedState.GetAttr("wait_for")
	if waitForBlock.IsNull() || !waitForBlock.IsKnown() {
		return nil
	}

	waiter, err := NewResourceWaiter(rs, rname, rtype, waitForBlock)
	if err != nil {
		return err
	}
	return waiter.Wait(ctx)
}
*/
// ApplyResourceChange function
func (s *RawProviderServer) ApplyResourceChange(ctx context.Context, req *tfprotov5.ApplyResourceChangeRequest) (*tfprotov5.ApplyResourceChangeResponse, error) {
	resp := &tfprotov5.ApplyResourceChange_Response{}

	applyPlannedState, err := UnmarshalResource(req.TypeName, (*req.PlannedState).Msgpack)
	if err != nil {
		return resp, err
	}
	Dlog.Printf("[ApplyResourceChange][Request][PlannedState]\n%s\n", spew.Sdump(applyPlannedState))

	sanitizedPlannedState := cty.UnknownAsNull(applyPlannedState)

	Dlog.Printf("[ApplyResourceChange][Request][SanitizedPlannedState]\n%s\n", spew.Sdump(sanitizedPlannedState))

	applyPriorState, err := UnmarshalResource(req.TypeName, (*req.PriorState).Msgpack)
	if err != nil {
		return resp, err
	}

	c, err := GetDynamicClient()
	if err != nil {
		if resp.Diagnostics == nil {
			resp.Diagnostics = make([]*tfprotov5.Diagnostic, 1)
		}
		resp.Diagnostics = append(resp.Diagnostics,
			&tfprotov5.Diagnostic{
				Severity: tfprotov5.Diagnostic_ERROR,
				Summary:  err.Error(),
			})
		return resp, err
	}
	var rs dynamic.ResourceInterface

	switch {
	case applyPriorState.IsNull() || (!applyPlannedState.IsNull() && !applyPriorState.IsNull()):
		{ // Apply resource
			o := sanitizedPlannedState.GetAttr("object")

			gvr, err := GVRFromCtyObject(&o)
			if err != nil {
				return resp, fmt.Errorf("failed to determine resource GVR: %s", err)
			}

			gvk, err := GVKFromCtyObject(&o)
			if err != nil {
				return resp, fmt.Errorf("failed to determine resource GVK: %s", err)
			}

			tsch, err := resourceTypeFromOpenAPI(gvk)
			if err != nil {
				return resp, fmt.Errorf("failed to determine resource type ID: %s", err)
			}

			pu, err := CtyObjectToUnstructured(&o)
			if err != nil {
				return resp, err
			}
			ns, err := IsResourceNamespaced(gvr)
			if err != nil {
				return resp, err
			}
			// remove null attributes - the API doesn't appreciate requests that include them
			uo := unstructured.Unstructured{Object: mapRemoveNulls(pu)}
			rnamespace := uo.GetNamespace()
			rname := uo.GetName()

			if ns {
				rs = c.Resource(gvr).Namespace(rnamespace)
			} else {
				rs = c.Resource(gvr)
			}
			jd, err := uo.MarshalJSON()
			if err != nil {
				return resp, err
			}

			// Call the Kubernetes API to create the new resource
			result, err := rs.Patch(ctx, rname, types.ApplyPatchType, jd, v1.PatchOptions{FieldManager: "Terraform"})
			if err != nil {
				Dlog.Printf("[ApplyResourceChange][Apply] Error: %s\n%s\n", spew.Sdump(err), spew.Sdump(result))
				rn := types.NamespacedName{Namespace: rnamespace, Name: rname}.String()
				resp.Diagnostics = append(resp.Diagnostics,
					&tfprotov5.Diagnostic{
						Severity: tfprotov5.DiagnosticSeverityError,
						Detail:   err.Error(),
						Summary:  fmt.Sprintf("PATCH resource %s failed: %s", rn, err),
					})
				return resp, fmt.Errorf("PATCH resource %s failed: %s", rn, err)
			}
			Dlog.Printf("[ApplyResourceChange][Apply] API response:\n%s\n", spew.Sdump(result))

			fo := FilterEphemeralFields(result.Object)
			newResObject, err := UnstructuredToCty(fo, tsch)
			if err != nil {
				return resp, err
			}
			Dlog.Printf("[ApplyResourceChange][Apply][CtyResponse]\n%s\n", spew.Sdump(newResObject))

			// err = s.waitForCompletion(ctx, applyPlannedState, rs, rname, tsch)
			// if err != nil {
			// 	return resp, err
			// }

			newResState, err := cty.Transform(sanitizedPlannedState,
				ResourceDeepUpdateObjectAttr(cty.GetAttrPath("object"), &newResObject),
			)
			if err != nil {
				return resp, err
			}
			Dlog.Printf("[ApplyResourceChange][Create] transformed new state:\n%s", spew.Sdump(newResState))

			mp, err := MarshalResource(req.TypeName, &newResState)
			if err != nil {
				return resp, err
			}
			resp.NewState = &tfprotov5.DynamicValue{Msgpack: mp}
		}
	case applyPlannedState.IsNull():
		{ // Delete the resource
			if !applyPriorState.Type().HasAttribute("object") {
				return resp, fmt.Errorf("existing state of resource %s has no 'object' attribute", req.TypeName)
			}
			pco := applyPriorState.GetAttr("object")
			pu, err := CtyObjectToUnstructured(&pco)
			if err != nil {
				return resp, err
			}
			uo := unstructured.Unstructured{Object: pu}
			gvr, err := GVRFromCtyUnstructured(&uo)
			if err != nil {
				return resp, err
			}
			ns, err := IsResourceNamespaced(gvr)
			if err != nil {
				return resp, err
			}

			gvk, err := GVKFromCtyObject(&pco)
			if err != nil {
				return resp, fmt.Errorf("failed to determine resource GVK: %s", err)
			}

			tsch, err := resourceTypeFromOpenAPI(gvk)
			if err != nil {
				return resp, fmt.Errorf("failed to determine resource type ID: %s", err)
			}

			rnamespace := uo.GetNamespace()
			rname := uo.GetName()

			if ns {
				rs = c.Resource(gvr).Namespace(rnamespace)
			} else {
				rs = c.Resource(gvr)
			}
			err = rs.Delete(ctx, rname, v1.DeleteOptions{})
			if err != nil {
				rn := types.NamespacedName{Namespace: rnamespace, Name: rname}.String()
				resp.Diagnostics = append(resp.Diagnostics,
					&tfprotov5.Diagnostic{
						Severity: tfprotov5.Diagnostic_ERROR,
						Detail:   err.Error(),
						Summary:  fmt.Sprintf("DELETE resource %s failed: %s", rn, err),
					})
				return resp, fmt.Errorf("DELETE resource %s failed: %s", rn, err)
			}

			// err = s.waitForCompletion(ctx, applyPlannedState, rs, rname, tsch)
			// if err != nil {
			// 	return resp, err
			// }

			resp.NewState = req.PlannedState
		}
	}

	return resp, nil
}

// ImportResourceState function
func (*RawProviderServer) ImportResourceState(ctx context.Context, req *tfprotov5.ImportResourceStateRequest) (*tfprotov5.ImportResourceStateResponse, error) {
	// Terraform only gives us the schema name of the resource and an ID string, as passed by the user on the command line.
	// The ID should be a combination of a Kubernetes GRV and a namespace/name type of resource identifier.
	// Without the user supplying the GRV there is no way to fully identify the resource when making the Get API call to K8s.
	// Presumably the Kubernetes API machinery already has a standard for expressing such a group. We should look there first.
	return nil, status.Errorf(codes.Unimplemented, "method ImportResourceState not implemented")
}

// ReadDataSource function
func (s *RawProviderServer) ReadDataSource(ctx context.Context, req *tfprotov5.ReadDataSourceRequest) (*tfprotov5.ReadDataSourceResponse, error) {
	//	Dlog.Printf("[ReadDataSource][Request]\n%s\n", spew.Sdump(*req))

	return nil, status.Errorf(codes.Unimplemented, "method ReadDataSource not implemented")
}

// StopProvider function
func (s *RawProviderServer) StopProvider(ctx context.Context, req *tfprotov5.StopProviderRequest) (*tfprotov5.StopProviderResponse, error) {
	//	Dlog.Printf("[Stop][Request]\n%s\n", spew.Sdump(*req))

	return nil, status.Errorf(codes.Unimplemented, "method Stop not implemented")
}
