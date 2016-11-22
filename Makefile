NAME:=graphite-clickhouse
MAINTAINER:="Roman Lomonosov <r.lomonosov@gmail.com>"
DESCRIPTION:="Graphite cluster backend with ClickHouse support"

GO ?= go
export GOPATH := $(CURDIR)/_vendor
TEMPDIR:=$(shell mktemp -d)
VERSION:=$(shell sh -c 'grep "const Version" $(NAME).go  | cut -d\" -f2')

all: $(NAME)

submodules:
	git submodule init
	git submodule update --recursive

$(NAME):
	$(GO) build github.com/lomik/$(NAME).go

gox-build:
	rm -rf out
	mkdir -p out
	gox -os="linux" -arch="amd64" -arch="386" -output="out/$(NAME)-{{.OS}}-{{.Arch}}"  github.com/lomik/$(NAME)
	ls -la out/
	mkdir -p out/root/etc/$(NAME)/
	./out/$(NAME)-linux-amd64 -config-print-default > out/root/etc/$(NAME)/$(NAME).conf

fpm-deb:
	make fpm-build-deb ARCH=amd64
	make fpm-build-deb ARCH=386
fpm-rpm:
	make fpm-build-rpm ARCH=amd64
	make fpm-build-rpm ARCH=386

fpm-build-deb:
	fpm -s dir -t deb -n $(NAME) -v $(VERSION) \
		--deb-priority optional --category admin \
		--force \
		--deb-compression bzip2 \
		--url https://github.com/lomik/$(NAME) \
		--description $(DESCRIPTION) \
		-m $(MAINTAINER) \
		--license "MIT" \
		-a $(ARCH) \
		--config-files /etc/$(NAME)/$(NAME).conf \
		out/$(NAME)-linux-$(ARCH)=/usr/bin/$(NAME) \
		deploy/systemd/$(NAME).service=/usr/lib/systemd/system/$(NAME).service \
		out/root/=/


fpm-build-rpm:
	fpm -s dir -t rpm -n $(NAME) -v $(VERSION) \
		--force \
		--rpm-compression bzip2 --rpm-os linux \
		--url https://github.com/lomik/$(NAME) \
		--description $(DESCRIPTION) \
		-m $(MAINTAINER) \
		--license "MIT" \
		-a $(ARCH) \
		--config-files /etc/$(NAME)/$(NAME).conf \
		out/$(NAME)-linux-$(ARCH)=/usr/bin/$(NAME) \
		deploy/systemd/$(NAME).service=/usr/lib/systemd/system/$(NAME).service \
		out/root/=/
