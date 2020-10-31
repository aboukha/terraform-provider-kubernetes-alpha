module github.com/hashicorp/terraform-provider-kubernetes-alpha

go 1.15

require (
	github.com/alexsomesan/openapi-cty v0.0.6
	github.com/davecgh/go-spew v1.1.1
	github.com/fatih/color v1.9.0 // indirect
	github.com/golang/protobuf v1.4.3
	github.com/googleapis/gnostic v0.4.0 // indirect
	github.com/hashicorp/go-cty v1.4.1-0.20200723130312-85980079f637
	github.com/hashicorp/go-hclog v0.14.1
	github.com/hashicorp/go-plugin v1.3.0
	github.com/hashicorp/hcl/v2 v2.0.0
	github.com/hashicorp/terraform-exec v0.10.0
	github.com/hashicorp/terraform-json v0.5.0
	github.com/hashicorp/terraform-plugin-go v0.0.0-20201030231218-5e8aeb3ae852
	github.com/hashicorp/terraform-plugin-sdk v1.4.1
	github.com/hashicorp/terraform-plugin-test/v2 v2.1.2
	github.com/hashicorp/yamux v0.0.0-20200609203250-aecfd211c9ce // indirect
	github.com/mattn/go-colorable v0.1.7 // indirect
	github.com/mitchellh/go-homedir v1.1.0
	github.com/mitchellh/go-testing-interface v1.14.1 // indirect
	github.com/oklog/run v1.1.0 // indirect
	github.com/pkg/errors v0.8.1
	github.com/stretchr/testify v1.4.0
	github.com/zclconf/go-cty v1.2.1
	golang.org/x/net v0.0.0-20201029221708-28c70e62bb1d // indirect
	golang.org/x/sys v0.0.0-20201029080932-201ba4db2418 // indirect
	golang.org/x/text v0.3.4 // indirect
	golang.org/x/time v0.0.0-20191024005414-555d28b269f0 // indirect
	google.golang.org/genproto v0.0.0-20201030142918-24207fddd1c3 // indirect
	google.golang.org/grpc v1.33.1
	google.golang.org/protobuf v1.25.0
	k8s.io/apiextensions-apiserver v0.18.0
	k8s.io/apimachinery v0.18.0
	k8s.io/client-go v0.18.0
)

replace google.golang.org/grpc => google.golang.org/grpc v1.29.1
