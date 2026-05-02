#!/bin/bash

# Build P2P CDN for Windows & Linux (AMD64)

set -e

export CGO_ENABLED=0
export GOARCH=amd64

mkdir -p bin

go mod download

echo "Building for WINDOWS..."
GOOS=windows go build -buildvcs=false -ldflags='-w -s' -o bin/tracker.exe ./cmd/tracker
GOOS=windows go build -buildvcs=false -ldflags='-w -s' -o bin/p2pcdn.exe ./cmd/cli
GOOS=windows go build -buildvcs=false -ldflags='-w -s' -o bin/p2pcdn-daemon.exe ./cmd/daemon
GOOS=windows go build -buildvcs=false -ldflags='-w -s' -o bin/benchmark.exe ./cmd/benchmark

echo "Building for LINUX..."
GOOS=linux go build -buildvcs=false -ldflags='-w -s' -o bin/tracker ./cmd/tracker
GOOS=linux go build -buildvcs=false -ldflags='-w -s' -o bin/p2pcdn ./cmd/cli
GOOS=linux go build -buildvcs=false -ldflags='-w -s' -o bin/p2pcdn-daemon ./cmd/daemon
GOOS=linux go build -buildvcs=false -ldflags='-w -s' -o bin/benchmark ./cmd/benchmark

echo ""
echo "All builds successful"