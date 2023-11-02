#!/bin/sh

cd $(dirname $0)

BRANCH=$(cat go.mod  | grep 'github.com/prometheus/prometheus' | awk '{print $2}')

rm -rf tmp || exit 1
mkdir tmp || exit 1
git clone --depth 1 --branch $BRANCH https://github.com/prometheus/prometheus.git tmp || exit 1

cd tmp || exit 1
make assets-compress || exit 1

cd .. || exit 1

cp tmp/web/ui/embed.go vendor/github.com/prometheus/prometheus/web/ui/embed.go || exit 1
rm -rf vendor/github.com/prometheus/prometheus/web/ui/static || exit 1
cp -a tmp/web/ui/static vendor/github.com/prometheus/prometheus/web/ui/static || exit 1
rm -rf vendor/github.com/prometheus/prometheus/web/ui/.gitignore tmp || exit 1
