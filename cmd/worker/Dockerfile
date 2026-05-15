# ETAPA DE COMPILACION
FROM golang:1.26.1-alpine3.23 AS builder

LABEL intermediateStageToBeDeleted=true

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY internal/common/inner ./internal/common/inner
COPY internal/workers ./internal/workers
COPY cmd/worker ./cmd/worker

RUN CGO_ENABLED=0 GOOS=linux go build -o /build/bin/worker ./cmd/worker

# ETAPA DE EJECUCION
FROM busybox:latest
WORKDIR /app
COPY --from=builder /build/bin/worker /app/worker
ENTRYPOINT ["/app/worker"]