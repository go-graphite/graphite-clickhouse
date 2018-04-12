FROM golang:alpine as builder

WORKDIR /go/src/github.com/lomik/graphite-clickhouse

COPY . .

RUN apk --no-cache add make

RUN make

FROM alpine:latest

RUN apk --no-cache add ca-certificates
WORKDIR /

COPY --from=builder /go/src/github.com/lomik/graphite-clickhouse/graphite-clickhouse /usr/bin/graphite-clickhouse

CMD ["graphite-clickhouse"]

