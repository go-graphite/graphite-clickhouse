#!/bin/sh -e

cd "$( dirname "$0" )"
ROOT=$PWD

docker run -i -e "DEVEL=${DEVEL:-0}"  --rm -v "$ROOT:/root/go/src/github.com/lomik/graphite-clickhouse" golang bash -e << 'EOF'
    cd /root/
    export TZ=Europe/Moscow
    ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

    go install github.com/goreleaser/nfpm/v2/cmd/nfpm@v2.40.0

    cd /root/go/src/github.com/lomik/graphite-clickhouse

    # go reads the VCS state
    git config --global --add safe.directory "$PWD"

    make nfpm-deb nfpm-rpm
    chmod -R a+w *.deb *.rpm out/
EOF
