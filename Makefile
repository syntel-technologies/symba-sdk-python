SYMBA_REPO ?= https://github.com/syntel-technologies/symba.git
SYMBA_TAG  ?= $(shell cat src/symba/_proto/ENGINE_REF 2>/dev/null)
# For local development against a checked-out engine repo, override PROTO_SRC:
#   make proto-gen PROTO_SRC=/path/to/symba/proto
PROTO_SRC  ?=

.PHONY: help
help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

.PHONY: proto-gen
proto-gen:  ## Regenerate stubs from ENGINE_REF or PROTO_SRC=/path/to/engine/proto
	uv run python tools/generate_proto.py $(if $(PROTO_SRC),--source "$(PROTO_SRC)",)

.PHONY: install
install:  ## Install the package with all extras + dev group
	uv sync --all-extras

.PHONY: lint
lint:  ## Ruff lint
	uv run ruff check src tests

.PHONY: format
format:  ## Ruff format
	uv run ruff format src tests

.PHONY: typecheck
typecheck:  ## Pyright strict
	uv run pyright

.PHONY: test
test:  ## Unit tests (no infra)
	uv run pytest tests/unit -q

.PHONY: test-all
test-all:  ## All tests including integration/conformance (needs engine)
	uv run pytest -q

.PHONY: stub-check
stub-check:  ## Verify stubs without rewriting the working tree
	uv run python tools/generate_proto.py --check $(if $(PROTO_SRC),--source "$(PROTO_SRC)",)

.PHONY: build
build:  ## Build sdist + wheel
	uv build

.PHONY: check
check: lint typecheck test  ## Lint + typecheck + unit tests
