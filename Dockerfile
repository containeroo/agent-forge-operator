# Build the manager binary
FROM golang:1.26 AS builder
ARG TARGETOS
ARG TARGETARCH
ARG LDFLAGS
ARG GOVC_VERSION=v0.54.1

WORKDIR /workspace
# Copy the Go Modules manifests
COPY go.mod go.mod
COPY go.sum go.sum
# cache deps before building and copying source so that we don't need to re-download as much
# and so that source changes don't invalidate our downloaded layer
RUN go mod download
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl && rm -rf /var/lib/apt/lists/*

# Copy the go source
COPY cmd/main.go cmd/main.go
COPY api/ api/
COPY internal/ internal/

# Build
# the GOARCH has not a default value to allow the binary be built according to the host where the command
# was called. For example, if we call make docker-build in a local env which has the Apple Silicon M1 SO
# the docker BUILDPLATFORM arg will be linux/arm64 when for Apple x86 it will be linux/amd64. Therefore,
# by leaving it empty we can ensure that the container and binary shipped on it will have the same platform.
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build -a -ldflags="${LDFLAGS}" -o manager cmd/main.go
RUN set -eux; \
    arch="${TARGETARCH:-$(go env GOARCH)}"; \
    case "${arch}" in \
      amd64) govc_arch="x86_64" ;; \
      arm64) govc_arch="arm64" ;; \
      *) echo "unsupported govc architecture: ${arch}" >&2; exit 1 ;; \
    esac; \
    mkdir -p /workspace/bin; \
    curl -fsSL "https://github.com/vmware/govmomi/releases/download/${GOVC_VERSION}/govc_Linux_${govc_arch}.tar.gz" \
      | tar -xz -C /workspace/bin govc

# Use distroless as minimal base image to package the manager binary
# Refer to https://github.com/GoogleContainerTools/distroless for more details
FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY --from=builder /workspace/manager .
COPY --from=builder /workspace/bin/govc /usr/local/bin/govc
USER 65532:65532

ENTRYPOINT ["/manager"]
