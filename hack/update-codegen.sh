$GOPATH/src/k8s.io/code-generator/generate-groups.sh all \
user/pkg/client \
user/pkg/apis \
user:v1 \
--go-header-file ./hack/boilerplate.go.txt
