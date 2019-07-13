#!/bin/bash

cd `dirname $0`
ROOT=`pwd`

PROMETHEUS_VERSION="v2.11.1"

rm -rf vendor
rm -rf vendor_tmp
mkdir -p vendor_tmp
git clone --depth=1 --branch $PROMETHEUS_VERSION https://github.com/prometheus/prometheus vendor_tmp/prometheus
mv vendor_tmp/prometheus/vendor vendor
rm -rf vendor_tmp/prometheus/.git
mv vendor_tmp/prometheus vendor/github.com/prometheus/prometheus


function clone {
    rm -rf vendor/$1
    git clone --depth=1 --branch $2 https://$1/ vendor/$1
    rm -rf vendor/$1/.git
    rm -rf vendor/$1/vendor
}

function clone_into {
    rm -rf vendor/$3
    git clone --depth=1 --branch $2 https://$1/ vendor/$3
    rm -rf vendor/$3/.git
    rm -rf vendor/$3/vendor
}

clone github.com/BurntSushi/toml v0.3.0
clone_into github.com/uber-go/zap v1.7.1 go.uber.org/zap
clone_into github.com/uber-go/atomic master go.uber.org/atomic
clone_into github.com/uber-go/multierr master go.uber.org/multierr
clone github.com/lomik/zapwriter master
clone github.com/lomik/graphite-pickle master
clone github.com/lomik/stop master
clone github.com/lomik/og-rek master

rm -rf vendor_tmp/carbonapi
git clone --depth=1 --branch 0.9.2 https://github.com/go-graphite/carbonapi/ vendor_tmp/carbonapi
rm -rf vendor/github.com/go-graphite/carbonapi
mkdir -p vendor/github.com/go-graphite/carbonapi/pkg
mv vendor_tmp/carbonapi/pkg/parser vendor/github.com/go-graphite/carbonapi/pkg/parser
mv vendor_tmp/carbonapi/LICENSE vendor/github.com/go-graphite/carbonapi/LICENSE

rm -rf vendor_tmp
