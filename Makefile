BINARY=prism-bin

DIR = $(shell cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd)
VENDOR_DIR = vendor
IMPORT_PATH = github.com/lbryio/reflector.go

VERSION = $(shell git --git-dir=${DIR}/.git describe --dirty --always --long --abbrev=7)
LDFLAGS = -ldflags "-X ${IMPORT_PATH}/meta.Version=${VERSION}"


.PHONY: build dep clean test
.DEFAULT_GOAL: build


build: dep
	CGO_ENABLED=0 go build ${LDFLAGS} -asmflags -trimpath=${DIR} -o ${DIR}/${BINARY} main.go

dep: | $(VENDOR_DIR)

$(VENDOR_DIR):
	go get github.com/golang/dep/cmd/dep && dep ensure

clean:
	if [ -f ${DIR}/${BINARY} ]; then rm ${DIR}/${BINARY}; fi

test:
	go test ./... -v -cover
