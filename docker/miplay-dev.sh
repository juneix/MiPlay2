#!/bin/sh
set -eu

rm -f /tmp/shairport/miplay-ready
shairport_pid=""

if [ "${ENABLE_AVAHI:-1}" = "1" ]; then
  rm -f /run/dbus/dbus.pid /run/avahi-daemon/pid
  dbus-uuidgen --ensure
  dbus-daemon --system
  avahi-daemon --daemonize --no-chroot
fi

/usr/local/bin/nqptp &
nqptp_pid=$!
python miplay.py --dev &
miplay_pid=$!

cleanup() {
  kill "$miplay_pid" ${shairport_pid:-} "$nqptp_pid" 2>/dev/null || true
  wait "$miplay_pid" ${shairport_pid:-} "$nqptp_pid" 2>/dev/null || true
}
trap cleanup INT TERM EXIT

for _ in $(seq 1 100); do
  if [ -f /tmp/shairport/miplay-ready ]; then
    break
  fi
  if ! kill -0 "$miplay_pid" 2>/dev/null; then
    echo "MiPlay exited before FIFO readers became ready" >&2
    exit 1
  fi
  sleep 0.1
done

if [ ! -f /tmp/shairport/miplay-ready ]; then
  echo "Timed out waiting for MiPlay FIFO readers" >&2
  exit 1
fi

/usr/local/bin/shairport-sync -c /etc/shairport-sync.conf &
shairport_pid=$!
wait -n "$miplay_pid" "$shairport_pid"
