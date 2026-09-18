# Chronos developer workflow.
#
# Containers are managed with podman / podman-compose.
# Dependencies are vendored, so all Go commands work with no network access.

SHELL := /bin/bash
.DEFAULT_GOAL := help

# Vendored builds: no module proxy is consulted.
export GOFLAGS := -mod=vendor

# Postgres is published on a non-default loopback port so Chronos never
# collides with a system Postgres or another local stack on 5432.
PG_PORT          ?= 55432
HTTP_PORT        ?= 8088
DATABASE_URL     ?= postgres://chronos:chronos@127.0.0.1:$(PG_PORT)/chronos?sslmode=disable
SERVER_URL       ?= http://127.0.0.1:$(HTTP_PORT)
COMPOSE          ?= podman-compose
IMAGE            ?= localhost/chronos:dev
VERSION          ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)

export CHRONOS_DATABASE_URL      := $(DATABASE_URL)
export CHRONOS_TEST_DATABASE_URL := $(DATABASE_URL)

.PHONY: help
help: ## Show available targets
	@echo "Chronos — distributed workflow engine"
	@echo
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| sort \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
	@echo
	@echo "Quick start:  make db-up && make demo"

# ---------------------------------------------------------------------------
# Build and code quality
# ---------------------------------------------------------------------------

.PHONY: build
build: ## Build all three binaries into ./bin
	@mkdir -p bin
	go build -trimpath -ldflags="-X main.version=$(VERSION)" -o bin/chronos-server ./cmd/chronos-server
	go build -trimpath -ldflags="-X main.version=$(VERSION)" -o bin/chronos-worker ./cmd/chronos-worker
	go build -trimpath -o bin/chronos-loadtest ./cmd/chronos-loadtest
	@echo "built bin/chronos-server, bin/chronos-worker and bin/chronos-loadtest ($(VERSION))"

.PHONY: fmt
fmt: ## Format all Go source
	gofmt -l -w ./cmd ./internal

.PHONY: vet
vet: ## Run go vet
	go vet ./...

.PHONY: check
check: fmt vet test ## Format, vet, and run unit tests

# GO_DIRECTIVE is pinned so `go mod tidy` cannot quietly raise the module's
# minimum Go version to whatever the local toolchain happens to be, which would
# break the container build image.
GO_DIRECTIVE ?= 1.26.0

.PHONY: tidy
tidy: ## Re-resolve and re-vendor dependencies
	GOFLAGS=-mod=mod GOPROXY=off go mod tidy -go=$(GO_DIRECTIVE)
	GOFLAGS=-mod=mod GOPROXY=off go mod vendor

.PHONY: clean
clean: ## Remove build artifacts
	rm -rf bin coverage.out coverage.html

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

# Packages with no database dependency. Listed explicitly rather than relying on
# the integration tests' self-skip, so `make test` is fast and deterministic even
# when a Chronos PostgreSQL happens to be running.
UNIT_PKGS := ./internal/domain/... ./internal/api/... ./internal/worker/... \
             ./internal/config/... ./internal/client/... ./internal/telemetry/...

.PHONY: test
test: ## Run unit tests (no database required)
	go test -count=1 -race $(UNIT_PKGS)

.PHONY: test-integration
test-integration: db-up wait-db ## Run every test, including database integration tests
	go test -count=1 -race ./...

.PHONY: test-all
test-all: test-integration ## Alias for test-integration

.PHONY: cover
cover: db-up wait-db ## Produce an HTML coverage report
	go test -count=1 -coverprofile=coverage.out -coverpkg=./internal/... ./...
	go tool cover -html=coverage.out -o coverage.html
	@go tool cover -func=coverage.out | tail -1
	@echo "wrote coverage.html"

# ---------------------------------------------------------------------------
# Database (podman)
# ---------------------------------------------------------------------------

.PHONY: db-up
db-up: ## Start PostgreSQL in podman
	$(COMPOSE) up -d postgres

.PHONY: wait-db
wait-db: ## Block until PostgreSQL accepts connections
	@echo -n "waiting for postgres on 127.0.0.1:$(PG_PORT) "
	@for i in $$(seq 1 60); do \
		if podman exec chronos-postgres pg_isready -U chronos -d chronos >/dev/null 2>&1; then \
			echo "ready"; exit 0; \
		fi; \
		echo -n "."; sleep 1; \
	done; \
	echo " timed out"; exit 1

.PHONY: db-down
db-down: ## Stop PostgreSQL (data volume is kept)
	$(COMPOSE) stop postgres || true
	podman rm -f chronos-postgres 2>/dev/null || true

.PHONY: db-reset
db-reset: ## Destroy and recreate the database volume
	$(COMPOSE) down -v || true
	podman rm -f chronos-postgres 2>/dev/null || true
	podman volume rm -f chronos_chronos-pgdata 2>/dev/null || true
	$(MAKE) db-up wait-db

.PHONY: psql
psql: ## Open a psql shell against the Chronos database
	podman exec -it chronos-postgres psql -U chronos -d chronos

.PHONY: db-state
db-state: ## Print a summary of persisted workflow state
	@podman exec chronos-postgres psql -U chronos -d chronos -c \
		"SELECT workflow_name, state, count(*) FROM workflow_executions GROUP BY 1,2 ORDER BY 1,2;" -c \
		"SELECT name, state, attempt, worker_id FROM tasks ORDER BY execution_id, name LIMIT 40;"

# ---------------------------------------------------------------------------
# Running locally
# ---------------------------------------------------------------------------

.PHONY: run-server
run-server: ## Run the API + engine on the host (needs make db-up)
	CHRONOS_HTTP_ADDR=":$(HTTP_PORT)" CHRONOS_LOG_LEVEL=$${CHRONOS_LOG_LEVEL:-info} \
		go run ./cmd/chronos-server

.PHONY: run-worker
run-worker: ## Run a worker on the host (needs make run-server)
	CHRONOS_SERVER_URL=$(SERVER_URL) \
	CHRONOS_WORKER_NAME=$${CHRONOS_WORKER_NAME:-worker-local-1} \
	CHRONOS_LOG_LEVEL=$${CHRONOS_LOG_LEVEL:-info} \
		go run ./cmd/chronos-worker

.PHONY: demo
demo: ## End-to-end demo: start server + worker, run Task A -> B -> C
	./scripts/demo.sh

.PHONY: recovery-demo
recovery-demo: ## Kill the engine mid-workflow and show it resume from persisted state
	./scripts/recovery-demo.sh

.PHONY: failure-demo
failure-demo: ## Kill a worker mid-task; show lease reclaim, retries, dead-letter, replay
	./scripts/failure-demo.sh

# ---------------------------------------------------------------------------
# Observability and performance
# ---------------------------------------------------------------------------

.PHONY: metrics
metrics: ## Show the Chronos metrics a running server is exporting
	@curl -fsS $(SERVER_URL)/metrics | grep '^chronos_' | grep -v '^chronos_[a-z_]*_bucket'

.PHONY: queue
queue: ## Show live queue depth, backlog age and pool saturation
	@curl -fsS $(SERVER_URL)/metrics | grep -E \
		'^chronos_(queue_depth|queue_backoff_depth|queue_oldest_claimable_age_seconds|tasks_running|tasks\{|workflow_executions\{|dead_letter_depth|workers_registered|db_pool_)' \
		|| echo "no queue metrics yet — is the server running with metrics enabled?"

.PHONY: trace-demo
trace-demo: ## Run one workflow with tracing on and print the resulting spans
	./scripts/trace-demo.sh

.PHONY: loadtest
loadtest: ## Full performance suite: scaling, latency floor, saturation, recovery
	./scripts/loadtest.sh $(EXECUTIONS)

# Enough work to amortize per-run startup without making the suite slow.
EXECUTIONS ?= 600

.PHONY: loadtest-quick
loadtest-quick: ## Shorter performance run, for checking the harness works
	./scripts/loadtest.sh 100

.PHONY: dashboard
dashboard: ## Validate the Grafana dashboard JSON and list its panels
	@jq -e . deploy/observability/grafana-dashboard.json >/dev/null \
		&& echo "dashboard JSON is valid"
	@jq -r '.panels[] | select(.type != "row") | "  \(.type)\t\(.title)"' \
		deploy/observability/grafana-dashboard.json

.PHONY: dlq
dlq: ## Show the dead letter queue (needs a running server)
	@curl -fsS $(SERVER_URL)/v1/dead-letter | jq '{total, items: [.items[] | \
		{task: .task.name, activity: .task.activity, attempts: .task.attempt, \
		 failureReason, leaseExpiryCount, error: .task.error}]}'

# ---------------------------------------------------------------------------
# Containers
# ---------------------------------------------------------------------------

.PHONY: image
image: ## Build the container image with podman
	podman build --build-arg VERSION=$(VERSION) -t $(IMAGE) -f Containerfile .

.PHONY: stack-up
stack-up: image ## Run the full stack (postgres + server + 2 workers) in podman
	$(COMPOSE) --profile app up -d
	@echo "API on $(SERVER_URL)"

.PHONY: stack-down
stack-down: ## Stop the full stack
	$(COMPOSE) --profile app down || true

.PHONY: stack-logs
stack-logs: ## Tail logs from the containerized stack
	$(COMPOSE) --profile app logs -f

# ---------------------------------------------------------------------------
# Deployment (Terraform + Kubernetes)
# ---------------------------------------------------------------------------

TF_DIR ?= deploy/terraform
ENV    ?= dev

.PHONY: tf-validate
tf-validate: ## Format-check and validate Terraform (no credentials needed)
	cd $(TF_DIR) && terraform fmt -check -recursive -diff
	cd $(TF_DIR) && terraform init -backend=false -input=false >/dev/null
	cd $(TF_DIR) && terraform validate

.PHONY: tf-fmt
tf-fmt: ## Format Terraform files
	cd $(TF_DIR) && terraform fmt -recursive

.PHONY: tf-plan
tf-plan: ## Plan infrastructure for ENV (needs AWS credentials)
	cd $(TF_DIR) && terraform init -input=false -backend-config=envs/$(ENV).backend.hcl
	cd $(TF_DIR) && terraform plan -input=false -var-file=envs/$(ENV).tfvars

.PHONY: k8s-render
k8s-render: ## Render the Kustomize overlay for ENV to stdout
	@kubectl kustomize deploy/k8s/overlays/$(ENV)

.PHONY: k8s-validate
k8s-validate: ## Render both overlays and assert probes, limits, and no secrets
	@./scripts/validate-deploy.sh

.PHONY: deploy-validate
deploy-validate: tf-validate k8s-validate ## Validate all deployment configuration
