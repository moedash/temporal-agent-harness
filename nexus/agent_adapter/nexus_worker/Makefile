.PHONY: generate install-nexgen build build-lambda lambda-build test test-contract vet

# Installs the newest nex-gen release into `go env GOPATH`/bin, replacing whatever's
# there if a newer tag exists. We're deliberately tracking latest (not pinning) while
# nex-gen is under active development — no backward-compat concerns here yet.
# (Every nex-gen release is marked "pre-release" on GitHub, so /releases/latest 404s;
# we list releases instead and take the first, which is the newest.)
install-nexgen:
	@BINDIR=$$(go env GOPATH)/bin; \
	OS=$$(uname -s); ARCH=$$(uname -m); \
	case "$$OS-$$ARCH" in \
		Darwin-arm64) TARGET=aarch64-apple-darwin ;; \
		Darwin-x86_64) TARGET=x86_64-apple-darwin ;; \
		Linux-aarch64) TARGET=aarch64-unknown-linux-gnu ;; \
		Linux-x86_64) TARGET=x86_64-unknown-linux-gnu ;; \
		*) echo "error: unsupported platform $$OS-$$ARCH for nex-gen" && exit 1 ;; \
	esac; \
	TAG=$$(curl -sL https://api.github.com/repos/temporalio/nex-gen/releases | grep -o '"tag_name": *"[^"]*"' | head -1 | sed -E 's/.*"([^"]+)"$$/\1/'); \
	if [ -z "$$TAG" ]; then echo "error: could not resolve latest nex-gen release from GitHub" && exit 1; fi; \
	if [ -x "$$BINDIR/nexgen" ] && [ "$$($$BINDIR/nexgen --version 2>/dev/null | awk '{print $$2}')" = "$${TAG#v}" ]; then exit 0; fi; \
	URL="https://github.com/temporalio/nex-gen/releases/download/$$TAG/nexgen-$$TAG-$$TARGET.tar.gz"; \
	echo "Installing nex-gen $$TAG ($$TARGET) to $$BINDIR..."; \
	mkdir -p "$$BINDIR" && curl -sL "$$URL" | tar xz -C "$$BINDIR" nexgen && chmod +x "$$BINDIR/nexgen"

# Regenerates the handler-side Go bindings from the shared agent.nexusrpc.yaml contract.
# Note: nex-gen clears its --output directory before writing, so generated code lives in
# its own handler/generated subpackage rather than alongside hand-written handler.go.
generate: install-nexgen
	$$(go env GOPATH)/bin/nexgen go agent.nexusrpc.yaml --output handler/generated

build:
	go build ./...

# Build an AWS Lambda deployment artifact for a cmd/<name> binary.
# Produces build/<CMD>/bootstrap and build/<CMD>/<CMD>.zip.
#
# LAMBDA_ARCH MUST match `Architectures` in deploy/worker-lambda.cfn.yaml (arm64).
# A mismatch builds and packages fine but fails at invocation with "exec format error".
# GOWORK=off so the artifact resolves against this module's go.mod, not the workspace.
LAMBDA_ARCH ?= arm64

# Reusable: `make lambda-build CMD=worker-lambda`
lambda-build:
	mkdir -p build/$(CMD)
	GOWORK=off GOOS=linux GOARCH=$(LAMBDA_ARCH) CGO_ENABLED=0 \
	  go build -tags lambda.norpc -ldflags="-s -w" -o build/$(CMD)/bootstrap ./cmd/$(CMD)
	cd build/$(CMD) && rm -f $(CMD).zip && zip -q -X $(CMD).zip bootstrap
	@echo "Built build/$(CMD)/$(CMD).zip (GOARCH=$(LAMBDA_ARCH))"

build-lambda:
	$(MAKE) lambda-build CMD=worker-lambda

vet:
	go vet ./...

# Run all tests. Contract tests (requiring uv/Python) skip gracefully if unavailable.
test:
	go test -race -count=1 ./...

# Run only the contract tests; fails loudly if uv is not installed.
test-contract:
	@command -v uv >/dev/null 2>&1 || (echo "error: uv not found — required for contract tests" && exit 1)
	go test -race -count=1 -run 'Contract' ./...
