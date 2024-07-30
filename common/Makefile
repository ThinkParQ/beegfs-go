# force the usage of /bin/bash instead of /bin/sh
SHELL := /bin/bash

# Test targets:
# Test targets may make change to the local repository (e.g. running go mod tidy) to
# verify all code required to build the project has been properly committed.
# Commonly this is done by running `make test` in CI, but could also be done locally.
# If you ran `make test` locally you may want to use `git reset` to revert the changes.
.PHONY: test
test: check-go-version check-gofmt check-linters check-go-tidy test-unit check-vulnerabilities check-licenses

.PHONY: check-go-version
check-go-version:
	@echo "Checking Go version..."
	@./hack/check-go-version.sh

# Verify that the code is formatted using gofmt: 
# Don't run on the vendor directory to avoid false positives.
.PHONY: check-gofmt
check-gofmt:
	@echo "Checking 'go fmt' has been run..."
	@if [ -n "$$(gofmt -l ./ | grep -v vendor)" ]; then \
		echo "The following files have not been formatted using gofmt:"; \
		echo -e "\nFix individual files with: \ngofmt -w <file> \n"\
		"\nOr fix all files with:\nfind . -type d \( -path './vendor' \) -prune -o -name '*.go' -print0 | xargs -0 gofmt -w \n\n" \
		exit 1; \
	fi

# Run the linters
.PHONY: check-linters
check-linters:
	staticcheck ./...
	go vet ./...

# Check for vulnerability issues
.PHONY: check-vulnerabilities
check-vulnerabilities:
	govulncheck ./...

# Run the unit tests
.PHONY: test-unit
test-unit: 
	@go test ./...

# Verify that go mod tidy has been run.
.PHONY: check-go-tidy
check-go-tidy: tidy
	@echo " Checking 'go mod tidy' has been run..."
	@if [ -n "$$(git status --porcelain go.mod go.sum)" ]; then \
		echo "go.mod and/or go.sum are not up to date. Please run 'go mod tidy' and commit the changes."; \
		exit 1; \
	fi

# For details on what licenses are disallowed see
# https://github.com/google/go-licenses#check 
#
# IMPORTANT: Any exceptions (using --ignore) such as the one for HCL must be
# manually added AFTER the NOTICE file has been updated and/or other appropriate
# steps have been taken based on the license requirements also ensure to add 
# justification below.
# 
# === Justification for Ignored Licenses === 
# 
# github.com/hashicorp/hcl: Distributed under MPL 2.0 which is a copyleft (reciprocal) license.
#   By default we consider reciprocal licenses a disallowed type because we need
#   to manually verify our use doesn't violate the license terms. Here our use
#   of an MPL 2.0 license is acceptable because we don't modify any of the
#   original source. Note this exists in the project because we use Viper
#   creating an indirect dependancy on HCL. However we don't allow users to use
#   HCL for configuration so technically we don't even use this at all.
.PHONY: check-licenses
check-licenses:
	@echo "Checking license compliance..."
	@go-licenses check ./... \
		--disallowed_types=forbidden,permissive,reciprocal,restricted,unknown \
		--ignore git.beegfs.io \
		--ignore github.com/thinkparq \
		--ignore github.com/hashicorp/hcl

# Targets for installation of various prerequisites:
.PHONY: install-tools
install-tools: 
	go install honnef.co/go/tools/cmd/staticcheck@latest
	go install github.com/google/go-licenses@v1.6.0
	go install golang.org/x/vuln/cmd/govulncheck@latest

.PHONY: tidy
tidy :
	@go mod tidy
