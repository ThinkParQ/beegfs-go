# force the usage of /bin/bash instead of /bin/sh
SHELL := /bin/bash

# Define install/uninstall targets for all binaries (target name:output binary:path to main.go):
INSTALL_TARGETS := \
  ctl:beegfs:./ctl/cmd/beegfs \
  remote:beegfs-remote:./rst/remote/cmd/beegfs-remote \
  sync:beegfs-sync:./rst/sync/cmd/beegfs-sync \
  watch:beegfs-watch:./watch/cmd/beegfs-watch
INSTALL_DIR=$(HOME)/go/bin

.PHONY: all install uninstall
all: install

# Shared install rule:
define INSTALL_RULE
.PHONY: install-$(1)
install-$(1):
	@echo "Installing $(2) from $(3) to $(INSTALL_DIR)"
	go install $(3)
	@echo 'Installation complete! Note you may need to add $(INSTALL_DIR) to your $$$$PATH.'
endef 
# Note: The $$$$ is needed because it's first evaluated by Make as $$, then passed to the shell as
# $$PATH, which is finally evaluated as the literal $PATH.

# Shared uninstall rule:
define UNINSTALL_RULE
.PHONY: uninstall-$(1)
uninstall-$(1):
	@echo "Uninstalling $(2) from $(INSTALL_DIR)"
	rm -f $(INSTALL_DIR)/$(2)
	@echo "Uninstallation complete!"
endef

# Generate install/uninstall targets for each binary:
$(foreach target,$(INSTALL_TARGETS),\
	$(eval NAME := $(word 1,$(subst :, ,$(target))))\
	$(eval BIN := $(word 2,$(subst :, ,$(target))))\
	$(eval SRC := $(word 3,$(subst :, ,$(target))))\
	$(eval $(call INSTALL_RULE,$(NAME),$(BIN),$(SRC)))\
	$(eval $(call UNINSTALL_RULE,$(NAME),$(BIN),$(SRC)))\
)

# Generate targets to install/uninstall all binaries:
INSTALL_ALL_TARGETS := $(foreach t,$(INSTALL_TARGETS),install-$(word 1,$(subst :, ,$(t))))
UNINSTALL_ALL_TARGETS := $(foreach t,$(INSTALL_TARGETS),uninstall-$(word 1,$(subst :, ,$(t))))

install: $(INSTALL_ALL_TARGETS)
uninstall: $(UNINSTALL_ALL_TARGETS)

# Trigger a "local-only" release using goreleaser to generate OS packages that can be used locally
# (Ref: https://goreleaser.com/quick-start/ and https://goreleaser.com/customization/snapshots/):
.PHONY: package-all
package-all:
	@command -v goreleaser >/dev/null 2>&1 || { \
		echo >&2 "ERROR: goreleaser is not installed, it can be installed with 'go install github.com/goreleaser/goreleaser/v2@latest' (see https://goreleaser.com/install/#install for additional options)."; \
		exit 1; \
	}
	@goreleaser --clean --snapshot --skip sign
	@echo "INFO: OS packages and other artifacts are available under dist/ and can be installed with: dpkg -i <PATH>"

# Generate NOTICE file.
.PHONY: generate-notices
generate-notices:
	@go tool go-licenses report ./ctl/... --template ctl/build/notice.tpl > ctl/NOTICE.md --ignore git.beegfs.io --ignore github.com/thinkparq
	@go tool go-licenses report ./rst/remote/... --template rst/remote/build/notice.tpl > rst/remote/NOTICE.md --ignore git.beegfs.io --ignore github.com/thinkparq
	@go tool go-licenses report ./rst/sync/... --template rst/sync/build/notice.tpl > rst/sync/NOTICE.md --ignore git.beegfs.io --ignore github.com/thinkparq	
	@go tool go-licenses report ./watch/... --template watch/build/notice.tpl > watch/NOTICE.md --ignore git.beegfs.io --ignore github.com/thinkparq

# Test targets:
# Test targets may make change to the local repository (e.g. running go mod tidy) to
# verify all code required to build the project has been properly committed.
# Commonly this is done by running `make test` in CI, but could also be done locally.
# If you ran `make test` locally you may want to use `git reset` to revert the changes.
.PHONY: test
test: check-go-version check-gofmt check-linters check-go-tidy test-unit check-vulnerabilities check-licenses

# Verify the installed version of Go matches go.mod. CI installs whatever version of Go is requested
# by go.mod, so this is mostly helpful when testing locally in case tooling breaks between versions.
.PHONY: check-go-version
check-go-version:
	@echo "Checking Go version..."
	@{ \
		GO_MOD_VERSION=go$$(grep '^go ' go.mod | awk '{print $$2}'); \
		INSTALLED_VERSION=$$(go version | { read _ _ ver _; echo $${ver}; }); \
		if [ "$$INSTALLED_VERSION" = "$$GO_MOD_VERSION" ]; then \
			echo "INFO: go.mod requests ($$GO_MOD_VERSION) and the build environment has $$INSTALLED_VERSION."; \
		else \
			echo "ERROR: go.mod requests $$GO_MOD_VERSION but the build environment has $$INSTALLED_VERSION."; \
			exit 1; \
		fi \
	} || { echo >&2 "ERROR: determining version of Go failed"; exit 1; }


# Verify that the code is formatted using gofmt: 
# Don't run on the vendor directory to avoid false positives.
.PHONY: check-gofmt
check-gofmt:
	@echo "Checking 'go fmt' has been run..."
	@if [ -n "$$(gofmt -l ./ | grep -v vendor)" ]; then \
		echo "The following files have not been formatted using gofmt:"; \
		gofmt -l ./ ;\
		echo -e "\nFix all files with: \ngo fmt ./... \n";\
		exit 1; \
	fi

# Run the linters
.PHONY: check-linters
check-linters:
	go tool staticcheck ./...
	go vet ./...

# Check for vulnerability issues
.PHONY: check-vulnerabilities
check-vulnerabilities:
	go tool govulncheck ./...

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
check-licenses: generate-notices
	@echo "Checking license compliance..."
	@go tool go-licenses check ./... \
		--disallowed_types=forbidden,permissive,reciprocal,restricted,unknown \
		--ignore git.beegfs.io \
		--ignore github.com/thinkparq \
		--ignore github.com/hashicorp/hcl
	@if [ -n "$$(git status --porcelain ctl/NOTICE.md)" ]; then \
        echo "BeeGFS CTL NOTICE file is not up to date. Please run 'make generate-notices' and commit the changes."; \
        exit 1; \
    fi
	@if [ -n "$$(git status --porcelain rst/remote/NOTICE.md)" ]; then \
        echo "BeeGFS Remote NOTICE file is not up to date. Please run 'make generate-notices' and commit the changes."; \
        exit 1; \
    fi
	@if [ -n "$$(git status --porcelain rst/sync/NOTICE.md)" ]; then \
        echo "BeeGFS Sync NOTICE file is not up to date. Please run 'make generate-notices' and commit the changes."; \
        exit 1; \
    fi
	@if [ -n "$$(git status --porcelain watch/NOTICE.md)" ]; then \
        echo "NOTICE file is not up to date. Please run 'make generate-notices' and commit the changes."; \
        exit 1; \
    fi

.PHONY: tidy
tidy :
	@go mod tidy
