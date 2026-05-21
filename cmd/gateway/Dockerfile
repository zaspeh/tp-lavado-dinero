# ETAPA DE COMPILACION
FROM golang:1.26.1-alpine3.23 AS builder

LABEL intermediateStageToBeDeleted=true

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

# COPY internal/common ./internal/common
# COPY internal/factory ./internal/factory
# COPY internal/gateway ./internal/gateway
COPY internal ./internal
COPY cmd/gateway ./cmd/gateway

RUN CGO_ENABLED=0 GOOS=linux go build -o /build/bin/gateway ./cmd/gateway

# ETAPA DE EJECUCION
FROM busybox:latest
WORKDIR /app
COPY --from=builder /build/bin/gateway /app/gateway
ENTRYPOINT ["/app/gateway"]