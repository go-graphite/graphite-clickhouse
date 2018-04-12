FROM golang:alpine as builder

WORKDIR /go/src/github.com/lomik/graphite-clickhouse
COPY . .

ENV GOPATH=/go

RUN go build -ldflags '-extldflags "-static"' github.com/lomik/graphite-clickhouse

FROM alpine:latest

RUN apk --no-cache add ca-certificates
WORKDIR /

COPY --from=builder /go/src/github.com/lomik/graphite-clickhouse/graphite-clickhouse /usr/bin/graphite-clickhouse

CMD ["graphite-clickhouse"]

