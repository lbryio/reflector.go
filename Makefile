.PHONY: test
.DEFAULT_GOAL: test

test:
	go test ./... -v -cover
