#!/bin/sh

cd $(dirname $0)

BRANCH=$(cat go.mod  | grep 'github.com/prometheus/prometheus' | awk '{print $2}')

rm -rf tmp
mkdir tmp
git clone --depth 1 --branch $BRANCH https://github.com/prometheus/prometheus.git tmp

cd tmp
make assets-compress

cd ..

cp tmp/web/ui/embed.go vendor/github.com/prometheus/prometheus/web/ui/embed.go
rm -rf vendor/github.com/prometheus/prometheus/web/ui/static
cp -a tmp/web/ui/static vendor/github.com/prometheus/prometheus/web/ui/static
rm -f vendor/github.com/prometheus/prometheus/web/ui/.gitignore
rm -rf tmp
