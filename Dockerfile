FROM golang:1.23-alpine AS builder
WORKDIR /app
COPY . .
RUN go build -o otel-proxy main.go

FROM alpine:3.19
WORKDIR /root/
COPY --from=builder /app/otel-proxy .
ENTRYPOINT ["./otel-proxy"]
