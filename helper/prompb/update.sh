#!/bin/sh

cd `dirname $0`
PROTO_PATH=`pwd`
echo $PWD

cd ../../
ROOT=`pwd`
echo $PWD
cd ${PROTO_PATH}

wget https://raw.githubusercontent.com/prometheus/prometheus/master/prompb/remote.proto -O remote.proto
wget https://raw.githubusercontent.com/prometheus/prometheus/master/prompb/types.proto -O types.proto

GOGOPROTO_ROOT="${ROOT}/vendor/github.com/gogo/protobuf"
GOGOPROTO_PATH="${GOGOPROTO_ROOT}:${GOGOPROTO_ROOT}/protobuf"

protoc --gogofast_out=. *.proto -I=. -I="${GOGOPROTO_PATH}"