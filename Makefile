all: graphite-clickhouse

GO ?= go
export GOPATH := $(CURDIR)/_vendor

submodules:
	git submodule init
	git submodule update --recursive

graphite-clickhouse:
	$(GO) build github.com/lomik/graphite-clickhouse
