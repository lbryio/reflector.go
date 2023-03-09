#!/usr/bin/env bash

err=0
trap 'err=1' ERR
# All the .go files, excluding auto generated folders
GO_FILES=$(find . -iname '*.go' -type f)
(
	go install golang.org/x/tools/cmd/goimports@latest                   # Used in build script for generated files
#	go install golang.org/x/lint/golint@latest                           # Linter
	go install github.com/jgautheron/gocyclo@latest                      # Check against high complexity
	go install github.com/mdempsky/unconvert@latest                      # Identifies unnecessary type conversions
	go install github.com/kisielk/errcheck@latest                        # Checks for unhandled errors
	go install github.com/opennota/check/cmd/varcheck@latest             # Checks for unused vars
	go install github.com/opennota/check/cmd/structcheck@latest          # Checks for unused fields in structs
)
echo "Running varcheck..." && varcheck $(go list ./...)
echo "Running structcheck..." && structcheck $(go list ./...)
# go vet is the official Go static analyzer
echo "Running go vet..." && go vet $(go list ./...)
# checks for unhandled errors
echo "Running errcheck..." && errcheck $(go list ./...)
# check for unnecessary conversions - ignore autogen code
echo "Running unconvert..." && unconvert -v $(go list ./...)
echo "Running gocyclo..." && gocyclo -ignore "_test" -avg -over 28 $GO_FILES
#echo "Running golint..." && golint -set_exit_status $(go list ./...)
test $err = 0 # Return non-zero if any command failed