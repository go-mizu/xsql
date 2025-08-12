# Makefile for github.com/mochilang/xsql
# Focused on library DX: test, coverage, lint, tidy, bench, release.

.DEFAULT_GOAL := help

# --------------------------
# Settings
# --------------------------
GO        := go
PKG       := ./...
GOFLAGS   := -trimpath -mod=readonly
COVERPKG  := ./...
COVERMODE := atomic
COVERFILE := coverage.out
COVERHTML := coverage.html

# Prefer gotestsum if available for nicer output
GOTESTSUM := $(shell command -v gotestsum 2>/dev/null)

# Extra test flags can be passed via GOTESTFLAGS env
GOTESTFLAGS ?=

# --------------------------
# Test & Coverage
# --------------------------

test: ## Run full test suite (shuffle, coverage)
ifdef GOTESTSUM
	@$(GOTESTSUM) --format=short-verbose -- \
	  -shuffle=on -covermode=$(COVERMODE) -coverpkg=$(COVERPKG) \
	  -coverprofile=$(COVERFILE) $(GOTESTFLAGS) $(PKG)
else
	@$(GO) test $(GOFLAGS) \
	  -v -shuffle=on -covermode=$(COVERMODE) -coverpkg=$(COVERPKG) \
	  -coverprofile=$(COVERFILE) $(GOTESTFLAGS) $(PKG)
endif
	@$(GO) tool cover -func=$(COVERFILE) | tail -n1

cover: test ## Generate HTML coverage report
	@$(GO) tool cover -html=$(COVERFILE) -o $(COVERHTML)
	@echo "Coverage HTML: $(COVERHTML)"

update-golden: ## Re-run tests with -update flag (for golden files)
ifdef GOTESTSUM
	@$(GOTESTSUM) --format=short-verbose -- -shuffle=on -update $(GOTESTFLAGS) $(PKG)
else
	@$(GO) test $(GOFLAGS) -v -shuffle=on -update $(GOTESTFLAGS) $(PKG)
endif

bench: ## Run benchmarks with mem stats
	@$(GO) test $(GOFLAGS) -bench=. -benchmem $(PKG)

# --------------------------
# Maintenance
# --------------------------

fmt: ## go fmt
	@$(GO) fmt $(PKG)

vet: ## go vet
	@$(GO) vet $(PKG)

lint: ## golangci-lint if available; fallback to vet
	@if command -v golangci-lint >/dev/null 2>&1; then \
		echo "golangci-lint running..."; \
		golangci-lint run ./... --timeout=5m; \
	else \
		echo "golangci-lint not found; running go vet"; \
		$(GO) vet $(PKG); \
	fi

vuln: ## govulncheck (requires Go 1.20+)
	@$(GO) install golang.org/x/vuln/cmd/govulncheck@latest
	@$$GOPATH/bin/govulncheck $(PKG) || govulncheck $(PKG)

tidy: ## go mod tidy & verify
	@$(GO) mod tidy
	@$(GO) mod verify

clean: ## Remove coverage artifacts
	@rm -f $(COVERFILE) $(COVERHTML)

ci: tidy fmt vet lint test ## Minimal CI pipeline locally

# --------------------------
# Release
# --------------------------

# --- Release settings ---
GIT_REMOTE ?= origin

# release: cut version, tag, push, and publish a GoReleaser release (source-only by default).
# Usage: make release VERSION=X.Y.Z
GIT_REMOTE ?= origin
DEFAULT_BRANCH ?= main

release:
ifndef VERSION
	$(error VERSION not set. Usage: make release VERSION=X.Y.Z)
endif
	@set -euo pipefail; \
	echo "Preparing xsql v$(VERSION)"; \
	echo "$(VERSION)" > VERSION; \
	git add -A; \
	git commit -m "release: v$(VERSION)" || echo "Nothing to commit"; \
	git tag -f v$(VERSION); \
	git push $(GIT_REMOTE) HEAD:$(DEFAULT_BRANCH); \
	git push -f $(GIT_REMOTE) v$(VERSION); \
	if [ -z "$$GITHUB_TOKEN" ]; then echo "GITHUB_TOKEN not set"; exit 1; fi; \
	goreleaser release --clean; \
	echo "Release v$(VERSION) complete"


snapshot: ## Snapshot (dry-run) release via GoReleaser
	@goreleaser release --snapshot --clean 

# --------------------------
# Help
# --------------------------
help: ## Show this help
	@echo ""
	@echo "xsql Makefile — common developer tasks"
	@echo "--------------------------------------"
	@grep -E '^[a-zA-Z0-9_\-]+:.*?## ' $(MAKEFILE_LIST) | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'
