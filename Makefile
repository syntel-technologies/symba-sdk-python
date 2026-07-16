SYMBA_REPO ?= https://github.com/amplior-ai/symba.git
SYMBA_TAG  ?= $(shell cat src/symba/_proto/VERSION 2>/dev/null)
# For local development against a checked-out engine repo, override PROTO_SRC:
#   make proto-gen PROTO_SRC=/path/to/symba/proto
PROTO_SRC  ?=

.PHONY: help
help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

.PHONY: proto-gen
proto-gen:  ## Regenerate committed proto stubs. Use SYMBA_TAG=vX.Y.Z or PROTO_SRC=/path
	@if [ -n "$(PROTO_SRC)" ]; then \
		SRC="$(PROTO_SRC)"; \
	else \
		rm -rf /tmp/symba-proto && \
		git clone --depth 1 --branch $(SYMBA_TAG) $(SYMBA_REPO) /tmp/symba-proto && \
		SRC=/tmp/symba-proto/proto; \
	fi; \
	rm -f src/symba/_proto/*_pb2.py src/symba/_proto/*_pb2.pyi src/symba/_proto/*_pb2_grpc.py && \
	rm -rf /tmp/symba-proto-out && mkdir -p /tmp/symba-proto-out && \
	uv run python -m grpc_tools.protoc -I$$SRC \
		--python_out=/tmp/symba-proto-out \
		--grpc_python_out=/tmp/symba-proto-out \
		--pyi_out=/tmp/symba-proto-out \
		$$SRC/symba/v1/*.proto && \
	cp /tmp/symba-proto-out/symba/v1/*_pb2.py \
	   /tmp/symba-proto-out/symba/v1/*_pb2.pyi \
	   /tmp/symba-proto-out/symba/v1/*_pb2_grpc.py src/symba/_proto/ && \
	uv run python tools/fix_proto_imports.py && \
	if [ -n "$(SYMBA_TAG)" ]; then echo "$(SYMBA_TAG)" > src/symba/_proto/VERSION; fi

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
stub-check:  ## Fail if committed proto stubs drift from the pinned engine tag
	@echo "Regenerating stubs from tag $(SYMBA_TAG) and diffing against committed copies..."
	@$(MAKE) proto-gen
	@if ! git diff --quiet -- src/symba/_proto; then \
		echo "ERROR: committed proto stubs are stale — run 'make proto-gen' and commit the result." >&2; \
		git --no-pager diff --stat -- src/symba/_proto >&2; \
		exit 1; \
	fi
	@echo "Proto stubs are up to date."

.PHONY: build
build:  ## Build sdist + wheel
	uv build

.PHONY: check
check: lint typecheck test  ## Lint + typecheck + unit tests
