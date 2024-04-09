version := $(shell git describe --dirty --always --long --abbrev=7)
commit := $(shell git rev-parse --short HEAD)
commit_long := $(shell git rev-parse HEAD)
branch := $(shell git rev-parse --abbrev-ref HEAD)
curTime := $(shell date +%s)

BINARY=prism-bin
IMPORT_PATH = github.com/lbryio/reflector.go
LDFLAGS="-X ${IMPORT_PATH}/meta.version=$(version) -X ${IMPORT_PATH}/meta.commit=$(commit) -X ${IMPORT_PATH}/meta.commitLong=$(commit_long) -X ${IMPORT_PATH}/meta.branch=$(branch) -X '${IMPORT_PATH}/meta.Time=$(curTime)'"
DIR = $(shell cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd)
BIN_DIR = $(DIR)/dist

.DEFAULT_GOAL := linux

.PHONY: test
test:
	go test -cover -v ./...

.PHONY: lint
lint:
	./scripts/lint.sh

.PHONY: linux
linux:
	GOARCH=amd64 CGO_ENABLED=0 GOOS=linux go build -ldflags ${LDFLAGS} -asmflags -trimpath=${DIR} -o ${BIN_DIR}/linux_amd64/${BINARY}

.PHONY: macos
macos:
	GOARCH=amd64 GOOS=darwin go build -ldflags ${LDFLAGS} -asmflags -trimpath=${DIR} -o ${BIN_DIR}/darwin_amd64/${BINARY}

.PHONY: image
image:
	docker buildx build -t lbry/reflector:$(version) -t lbry/reflector:latest --platform linux/amd64 .