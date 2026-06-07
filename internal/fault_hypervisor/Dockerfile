# ETAPA DE COMPILACION
FROM golang:1.26.1-alpine3.23 AS builder

LABEL intermediateStageToBeDeleted=true

WORKDIR /build

RUN apk add --no-cache ca-certificates

COPY go.mod go.sum ./
RUN go mod download

COPY internal ./internal
COPY internal/fault_hypervisor ./internal/fault_hypervisor

RUN CGO_ENABLED=0 GOOS=linux go build -o /build/bin/fault_hypervisor ./internal/fault_hypervisor

# ETAPA DE EJECUCION
FROM busybox:latest

WORKDIR /app

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

COPY --from=builder /build/bin/fault_hypervisor /app/fault_hypervisor

ENTRYPOINT ["/app/fault_hypervisor"]