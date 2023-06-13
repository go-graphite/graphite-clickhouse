NAME:=graphite-clickhouse
MAINTAINER:="Roman Lomonosov <r.lomonosov@gmail.com>"
DESCRIPTION:="Graphite cluster backend with ClickHouse support"
MODULE:=github.com/lomik/graphite-clickhouse

GO ?= go
export GOFLAGS +=  -mod=vendor
export GO111MODULE := on
TEMPDIR:=$(shell mktemp -d)

DEVEL ?= 0
ifeq ($(DEVEL), 0)
VERSION:=$(shell sh -c 'grep "const Version" $(NAME).go  | cut -d\" -f2')
else
VERSION:=$(shell sh -c 'git describe --always --tags | sed -e "s/^v//i"')
endif

SRCS:=$(shell find . -name '*.go')

all: $(NAME)

.PHONY: clean
clean:
	rm -f $(NAME) $(NAME)-client
	rm -rf out
	rm -f *deb *rpm
	rm -f sha256sum md5sum

$(NAME): $(SRCS)
	$(GO) build -tags builtinassets -ldflags '-X main.BuildVersion=$(VERSION)' $(MODULE)

debug: $(SRCS)
	$(GO) build -tags builtinassets -ldflags '-X main.BuildVersion=$(VERSION)' -gcflags=all='-N -l' $(MODULE)

deploy/doc/graphite-clickhouse.conf: $(NAME)
	./$(NAME) -config-print-default > $@

doc/config.md: deploy/doc/graphite-clickhouse.conf deploy/doc/config.md
	@echo 'Generating $@...'
	@printf '[//]: # (This file is built out of deploy/doc/config.md, please do not edit it manually)  \n' > $@
	@printf '[//]: # (To rebuild it run `make config`)\n\n' >> $@
	@cat deploy/doc/config.md >> $@
	@printf '\n```toml\n' >> $@
	@cat deploy/doc/graphite-clickhouse.conf >> $@
	@printf '```\n' >> $@

config: doc/config.md

test:
	$(GO) test -race ./...

e2e-test: $(NAME)
	$(GO) build $(MODULE)/cmd/e2e-test
	
client: $(NAME)
	$(GO) build $(MODULE)/cmd/graphite-clickhouse-client

gox-build:
	rm -rf out
	mkdir -p out
	gox -ldflags '-X main.BuildVersion=$(VERSION)' -os="linux" -arch="amd64" -arch="arm64" -output="out/$(NAME)-{{.OS}}-{{.Arch}}"  github.com/lomik/$(NAME)
	ls -la out/
	mkdir -p out/root/etc/$(NAME)/
	./out/$(NAME)-linux-amd64 -config-print-default > out/root/etc/$(NAME)/$(NAME).conf
	install -D --mode=0644 --owner=root --group=root \
		deploy/root/etc/logrotate.d/graphite-clickhouse \
		out/root/etc/logrotate.d/graphite-clickhouse

fpm-deb:
	$(MAKE) fpm-build-deb ARCH=amd64
	$(MAKE) fpm-build-deb ARCH=arm64
fpm-rpm:
	$(MAKE) fpm-build-rpm ARCH=amd64
	$(MAKE) fpm-build-rpm ARCH=arm64

fpm-build-deb:
	fpm -s dir -t deb -n $(NAME) -v $(VERSION) \
		--deb-priority optional --category admin \
		--force \
		--url https://github.com/lomik/$(NAME) \
		--description $(DESCRIPTION) \
		-m $(MAINTAINER) \
		--license "MIT" \
		-a $(ARCH) \
		--config-files /etc \
		out/$(NAME)-linux-$(ARCH)=/usr/bin/$(NAME) \
		deploy/root/=/ \
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
		--config-files /etc \
		out/$(NAME)-linux-$(ARCH)=/usr/bin/$(NAME) \
		deploy/root/=/ \
		out/root/=/

.ONESHELL:
RPM_VERSION:=$(subst -,_,$(VERSION))
packagecloud-push-rpm: $(wildcard $(NAME)-$(RPM_VERSION)-1.*.rpm)
	for pkg in $^; do
		package_cloud push $(REPO)/el/7 $${pkg} || true
		package_cloud push $(REPO)/el/8 $${pkg} || true
	done

.ONESHELL:
packagecloud-push-deb: $(wildcard $(NAME)_$(VERSION)_*.deb)
	for pkg in $^; do
		package_cloud push $(REPO)/ubuntu/xenial   $${pkg} || true
		package_cloud push $(REPO)/ubuntu/bionic   $${pkg} || true
		package_cloud push $(REPO)/ubuntu/focal    $${pkg} || true
		package_cloud push $(REPO)/debian/stretch  $${pkg} || true
		package_cloud push $(REPO)/debian/buster   $${pkg} || true
		package_cloud push $(REPO)/debian/bullseye $${pkg} || true
	done

packagecloud-push:
	@$(MAKE) packagecloud-push-rpm
	@$(MAKE) packagecloud-push-deb

packagecloud-autobuilds:
	$(MAKE) packagecloud-push REPO=go-graphite/autobuilds

packagecloud-stable:
	$(MAKE) packagecloud-push REPO=go-graphite/stable

sum-files: | sha256sum md5sum

md5sum:
	md5sum $(wildcard $(NAME)_$(VERSION)*.deb) $(wildcard $(NAME)-$(VERSION)*.rpm) > md5sum

sha256sum:
	sha256sum $(wildcard $(NAME)_$(VERSION)*.deb) $(wildcard $(NAME)-$(VERSION)*.rpm) > sha256sum
