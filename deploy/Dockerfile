FROM golang:1.27-alpine AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /engramd ./cmd/engramd/
RUN CGO_ENABLED=0 go build -o /engramctl ./cmd/engramctl/

FROM alpine:3.21
RUN apk add --no-cache ca-certificates
COPY --from=builder /engramd /usr/local/bin/engramd
COPY --from=builder /engramctl /usr/local/bin/engramctl

EXPOSE 50051 50052 9090

ENTRYPOINT ["engramd"]
