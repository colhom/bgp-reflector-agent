IMAGE_REPO?=quay.io/colin_hom/bgp-reflector-agent
CONTAINER_IMAGE?=$(IMAGE_REPO):`git rev-parse HEAD`
GOFILES=$(wildcard cmd/*) $(wildcard pkg/*)
push: image
	docker push $(CONTAINER_IMAGE)

image: bin/bgp-reflector-agent Dockerfile
	docker build -t $(CONTAINER_IMAGE) .

bin/bgp-reflector-agent: vendor $(GOFILES)
	deepcopy-gen -i './pkg/libcalicostub/'  -p ./pkg/libcalicostub/ -O stub-generated
	CGO_ENABLED=0 go build -v -i -a -tags netgo -installsuffix netgo -o ./bin/bgp-reflector-agent ./cmd/reflector-agent

vendor: glide.yaml glide.lock
	glide install -v
