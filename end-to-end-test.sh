#!/bin/bash

set -u -o pipefail

tmpdir=$(mktemp -d /tmp/pushprox_e2e_test.XXXXXX)

cleanup() {
  for f in "${tmpdir}"/*.pid ; do
    kill -9 "$(< $f)"
  done
  rm -r "${tmpdir}"
}

trap cleanup EXIT

./node_exporter &
echo $! > "${tmpdir}/node_exporter.pid"
while ! curl -s -f -L http://localhost:9100; do
  echo 'Waiting for node_exporter'
  sleep 2
done

./pushprox-proxy --log.level=debug --auth.tokens=my-pwd,phosae &
echo $! > "${tmpdir}/proxy.pid"
while ! curl -s -f -L http://localhost:8080/targets; do
  echo 'Waiting for proxy'
  sleep 2
done

./pushprox-client  --log.level=debug  --fqdn mac.client --proxy-addr 127.0.0.1:7080 --auth-token my-pwd --metrics http://127.0.0.1:9100/metrics,http://localhost:9100/metrics &
echo $! > "${tmpdir}/client.pid"
while [ "$(curl -s -L 'http://localhost:8080/targets' | jq 'length')" != '2' ] ; do
  echo 'Waiting for client'
  sleep 2
done

./prometheus --web.listen-address=0.0.0.0:19090 --config.file=prometheus.yml --log.level=debug &
echo $! > "${tmpdir}/prometheus.pid"
while ! curl -s -f -L http://localhost:19090/-/ready; do
  echo 'Waiting for Prometheus'
  sleep 3
done
sleep 15

query='http://localhost:19090/api/v1/query?query=node_exporter_build_info'
while [ $(curl -s -L "${query}" | jq '.data.result | length') != '1' ]; do
  echo 'Waiting for results'
  sleep 2
done
