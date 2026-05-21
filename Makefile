VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
GIT_COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_DATE ?= $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
LDFLAGS := -s -w \
	-X github.com/dispatchhub/dispatchhub/internal/shared/infrastructure/version.Version=$(VERSION) \
	-X github.com/dispatchhub/dispatchhub/internal/shared/infrastructure/version.GitCommit=$(GIT_COMMIT) \
	-X github.com/dispatchhub/dispatchhub/internal/shared/infrastructure/version.BuildDate=$(BUILD_DATE)

REGISTRY ?= dispatchhub
COMPONENTS := scheduler worker apiserver

.PHONY: all build test lint clean docker helm

all: build

## 构建所有组件
build: $(COMPONENTS)

$(COMPONENTS):
	@echo "Building $@..."
	CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o bin/$@ ./cmd/$@

## 运行测试
test:
	go test -race -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out

## 使用 short flag 运行测试（跳过集成测试）
test-unit:
	go test -short -race ./...

## 检查代码
lint:
	golangci-lint run ./...

## 生成 protobuf 代码
proto:
	protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		api/proto/dispatch.proto

## 为所有组件构建 Docker 镜像
docker: $(addprefix docker-,$(COMPONENTS))

docker-%:
	docker build \
		--build-arg VERSION=$(VERSION) \
		--build-arg GIT_COMMIT=$(GIT_COMMIT) \
		--build-arg BUILD_DATE=$(BUILD_DATE) \
		--build-arg COMPONENT=$* \
		-t $(REGISTRY)/$*:$(VERSION) \
		-t $(REGISTRY)/$*:latest \
		.

## 推送 Docker 镜像
push: $(addprefix push-,$(COMPONENTS))

push-%:
	docker push $(REGISTRY)/$*:$(VERSION)
	docker push $(REGISTRY)/$*:latest

## 安装 Helm chart
helm-install:
	helm upgrade --install dispatchhub deploy/helm/dispatchhub \
		--namespace dispatchhub --create-namespace

## 渲染 Helm chart（试运行）
helm-template:
	helm template dispatchhub deploy/helm/dispatchhub

## 本地运行（需要 Redis、etcd、MySQL）
run-scheduler:
	go run ./cmd/scheduler --config=config/scheduler.yaml

run-worker:
	go run ./cmd/worker --config=config/worker.yaml

run-apiserver:
	go run ./cmd/apiserver --config=config/apiserver.yaml

## 运行基线性能 benchmark 套件（open-model RPS，wrk2 + ghz）
bench:
	bash test/perf/run.sh

## 安装性能 benchmark 工具（wrk2 + ghz）
bench-deps:
	@command -v wrk2 >/dev/null || { \
		echo "wrk2 not found. Build from source:"; \
		echo "  sudo apt-get install -y build-essential libssl-dev zlib1g-dev"; \
		echo "  git clone https://github.com/giltene/wrk2.git /tmp/wrk2 && \\"; \
		echo "    make -C /tmp/wrk2 && sudo install /tmp/wrk2/wrk /usr/local/bin/wrk2"; \
		exit 1; \
	}
	go install github.com/bojand/ghz/cmd/ghz@latest

## 仅采集环境元数据
bench-env:
	bash test/perf/env/capture-server.sh
	bash test/perf/env/capture-client.sh

## 整理依赖
tidy:
	go mod tidy

## 清理构建产物
clean:
	rm -rf bin/ coverage.out

## 显示帮助
help:
	@echo "DispatchHub - Cloud-native Task Scheduling System"
	@echo ""
	@echo "Usage: make <target>"
	@echo ""
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/^## /  /'
