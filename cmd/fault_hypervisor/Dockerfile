# ETAPA DE COMPILACION
FROM golang:1.26.1-alpine3.23 AS builder

LABEL intermediateStageToBeDeleted=true

WORKDIR /build

RUN apk add --no-cache ca-certificates

COPY go.mod go.sum ./
RUN go mod download

COPY internal ./internal
COPY cmd/fault_hypervisor ./cmd/fault_hypervisor

RUN CGO_ENABLED=0 GOOS=linux go build -o /build/bin/fault_hypervisor ./cmd/fault_hypervisor

# ETAPA DE EJECUCION
FROM docker:28-dind

WORKDIR /app

COPY --from=builder /build/bin/fault_hypervisor /app/fault_hypervisor

COPY internal/fault_hypervisor/scripts/docker-entrypoint.sh /docker-entrypoint.sh

RUN chmod +x /docker-entrypoint.sh

ENTRYPOINT ["/docker-entrypoint.sh"]
