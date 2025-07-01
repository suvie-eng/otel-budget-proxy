# --- Builder Stage ---
# Use a specific Go version and an Alpine base for smaller size.
FROM golang:1.23-alpine AS builder

# Set build arguments for a static, optimized binary.
ENV CGO_ENABLED=0 GOOS=linux
WORKDIR /app

# Copy go.mod and go.sum first to leverage Docker layer caching.
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the source code.
COPY . .

# Build the application.
# -ldflags "-w -s" strips debug information, reducing binary size.
RUN go build -ldflags="-w -s" -o /otel-budget-proxy main.go

# --- Final Stage ---
# Use a minimal, non-root base image.
FROM alpine:3.19

# Create a non-root user and group.
RUN addgroup -S appgroup && adduser -S appuser -G appgroup

WORKDIR /home/appuser
COPY --from=builder /otel-budget-proxy .

# Change ownership to the new user.
RUN chown appuser:appgroup /home/appuser/otel-budget-proxy

# Switch to the non-root user.
USER appuser

# Expose the port the proxy listens on.
EXPOSE 4318

# Add a healthcheck.
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD wget -q --spider http://localhost:4318/_healthz || exit 1

# Set the entrypoint.
ENTRYPOINT ["./otel-budget-proxy"]

