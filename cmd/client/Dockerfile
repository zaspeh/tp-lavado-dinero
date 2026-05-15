# ETAPA DE COMPILACION
FROM golang:1.26.1-alpine3.23 AS builder

LABEL intermediateStageToBeDeleted=true

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY internal/common/external ./internal/common/external
COPY internal/client ./internal/client
COPY cmd/client ./cmd/client

RUN CGO_ENABLED=0 GOOS=linux go build -o /build/bin/client ./cmd/client

# ETAPA DE EJECUCION
FROM busybox:latest
WORKDIR /app
COPY --from=builder /build/bin/client /app/client
ENTRYPOINT ["/app/client"]