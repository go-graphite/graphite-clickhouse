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

all: $(NAME)

.PHONY: clean
clean:
	rm -rf out
	rm -f *deb *rpm
	rm -f sha256sum md5sum

.PHONY: $(NAME)
$(NAME):
	$(GO) build $(MODULE)

test:
	$(GO) test ./...

gox-build:
	rm -rf out
	mkdir -p out
	gox -os="linux" -arch="amd64" -arch="arm64" -output="out/$(NAME)-{{.OS}}-{{.Arch}}"  github.com/lomik/$(NAME)
	ls -la out/
	mkdir -p out/root/etc/$(NAME)/
	./out/$(NAME)-linux-amd64 -config-print-default > out/root/etc/$(NAME)/$(NAME).conf

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
		--config-files /etc/$(NAME)/$(NAME).conf \
		--config-files /etc/logrotate.d/$(NAME) \
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
		--config-files /etc/$(NAME)/$(NAME).conf \
		--config-files /etc/logrotate.d/$(NAME) \
		out/$(NAME)-linux-$(ARCH)=/usr/bin/$(NAME) \
		deploy/root/=/ \
		out/root/=/

packagecloud-push:
	package_cloud push $(REPO)/el/8 $(NAME)-$(VERSION)-1.x86_64.rpm || true
	package_cloud push $(REPO)/el/7 $(NAME)-$(VERSION)-1.x86_64.rpm || true
	package_cloud push $(REPO)/ubuntu/xenial $(NAME)_$(VERSION)_amd64.deb || true
	package_cloud push $(REPO)/ubuntu/bionic $(NAME)_$(VERSION)_amd64.deb || true
	package_cloud push $(REPO)/ubuntu/disco $(NAME)_$(VERSION)_amd64.deb || true
	package_cloud push $(REPO)/ubuntu/eoan $(NAME)_$(VERSION)_amd64.deb || true
	package_cloud push $(REPO)/debian/buster $(NAME)_$(VERSION)_amd64.deb || true
	package_cloud push $(REPO)/debian/stretch $(NAME)_$(VERSION)_amd64.deb || true
	package_cloud push $(REPO)/debian/jessie $(NAME)_$(VERSION)_amd64.deb || true

packagecloud-autobuilds:
	$(MAKE) packagecloud-push REPO=go-graphite/autobuilds

packagecloud-stable:
	$(MAKE) packagecloud-push REPO=go-graphite/stable

sum-files: | sha256sum md5sum

md5sum:
	md5sum $(wildcard $(NAME)_$(VERSION)*.deb) $(wildcard $(NAME)-$(VERSION)*.rpm) > md5sum

sha256sum:
	sha256sum $(wildcard $(NAME)_$(VERSION)*.deb) $(wildcard $(NAME)-$(VERSION)*.rpm) > sha256sum
