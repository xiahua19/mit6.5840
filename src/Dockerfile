# Build stage
FROM golang:1.19 AS builder

WORKDIR /app
COPY . .

RUN go build -o kvserver ./main/server/k8s_server.go

# Runtime stage
FROM alpine:latest

WORKDIR /app
COPY --from=builder /app/kvserver /app/kvserver

# Environment variables for configuration
ENV KV_SERVER_INDEX=0 \
    KV_SERVER_ADDRESSES="" \
    KV_MAXRAFTSTATE=-1 \
    KV_BACKEND_TYPE=0

CMD ["/app/kvserver"]