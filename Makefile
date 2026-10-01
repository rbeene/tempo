.PHONY: build test check schema clean
VERSION ?= dev
build:
	go build -trimpath -ldflags='-s -w -X github.com/rbeene/tempo/internal/cli.Version=$(VERSION)' -o bin/tempo ./cmd/tempo

test:
	go test -race ./...

check:
	test -z "$$(gofmt -l cmd internal)"
	go vet ./...
	go test -race -coverprofile=coverage.out ./...

schema: build
	bin/tempo schema > docs/cli-schema.json

clean:
	rm -rf bin coverage.out
