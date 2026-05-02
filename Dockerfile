# Multi-stage build for P2P CDN Tracker
# Optimized for Pterodactyl deployment

# Build stage
FROM --platform=$BUILDPLATFORM golang:1.25-alpine AS builder

ARG TARGETOS TARGETARCH

# Install build dependencies
RUN apk add --no-cache git gcc musl-dev

# Set working directory
WORKDIR /build

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build tracker with platform-specific optimizations
RUN CGO_ENABLED=1 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -a -installsuffix cgo \
    -ldflags="-w -s -X main.version=1.0.0 -X main.buildTime=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    -o tracker ./cmd/tracker/main.go

# Runtime stage - minimal Alpine image
FROM alpine:3.19

# Install runtime dependencies
RUN apk --no-cache add \
    ca-certificates \
    tzdata \
    curl \
    && rm -rf /var/cache/apk/*

# Create app user and group
RUN addgroup -g 1000 container && \
    adduser -D -u 1000 -G container container

# Set working directory
WORKDIR /home/container

# Copy binary from builder
COPY --from=builder --chown=container:container /build/tracker /home/container/tracker

# Copy default config (can be overridden by volume mount)
COPY --from=builder --chown=container:container /build/configs/tracker.yaml /home/container/tracker.yaml

# Switch to app user
USER container

# Expose default port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Default command with config file in current directory
ENTRYPOINT ["/home/container/tracker"]
CMD ["--config", "/home/container/tracker.yaml"]
