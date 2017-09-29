IMAGE_REPO?=quay.io/colin_hom/bgp-reflector-agent
CONTAINER_IMAGE?=$(IMAGE_REPO):`git rev-parse HEAD`
GOFILES=$(shell find cmd/ pkg/ -name "*.go" -type f)
push: image
	docker push $(CONTAINER_IMAGE)

image: bin/bgp-reflector-agent Dockerfile
	docker build -t $(CONTAINER_IMAGE) .

test: vendor gen $(GOFILES)
	go test -v ./cmd/* ./pkg/*

bin/bgp-reflector-agent: vendor gen $(GOFILES)
	CGO_ENABLED=0 go build -v -i -a -tags netgo -installsuffix netgo -o ./bin/bgp-reflector-agent ./cmd/reflector-agent

gen: vendor $(shell find ./pkg/libcalicostub -name "*.go" -type f)
	deepcopy-gen -i './pkg/libcalicostub/'  -p ./pkg/libcalicostub/ -O stub-generated
	gofmt -w ./pkg/libcalicostub/stub-generated.go
	goimports -w ./pkg/libcalicostub/stub-generated.go

vendor: glide.yaml glide.lock
	glide install -v
