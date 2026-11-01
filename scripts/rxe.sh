#!/usr/bin/env bash
# The scripts given to as_root expand their variables in the container.
# shellcheck disable=SC2016
#
# Soft-RoCE (rdma_rxe) test device for safeverbs's tests, on a private dummy
# interface that no real traffic uses.
#
#   scripts/rxe.sh up       load rdma_rxe, create the interface and the device
#   scripts/rxe.sh down     remove the device and the interface
#   scripts/rxe.sh status   show the device, its interface and its GIDs
#
# Root comes from a privileged docker container on the host's network with
# /dev and /lib/modules mounted, so only membership in the docker group is
# needed. Nothing touches the host's real interfaces.
#
# Environment (defaults in brackets):
#   SV_RXE_NETDEV  the dummy interface   [sv-rxe0]
#   SV_RXE_DEV     the RDMA device       [svrxe0]
#   SV_RXE_ADDR    its IPv4 address      [10.233.0.1/24]
set -euo pipefail

IMAGE=safeverbs-rxe
NETDEV=${SV_RXE_NETDEV:-sv-rxe0}
DEV=${SV_RXE_DEV:-svrxe0}
ADDR=${SV_RXE_ADDR:-10.233.0.1/24}

# Builds the helper image once: kmod for modprobe, iproute2 for ip and rdma.
image() {
	docker image inspect "$IMAGE" >/dev/null 2>&1 && return
	docker build -q -t "$IMAGE" - >/dev/null <<'EOF'
FROM debian:stable-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends kmod iproute2 \
 && rm -rf /var/lib/apt/lists
EOF
}

# Runs a shell script as root in the helper container.
as_root() {
	image
	docker run --rm --privileged --net=host \
		-v /lib/modules:/lib/modules:ro -v /dev:/dev \
		-e NETDEV="$NETDEV" -e DEV="$DEV" -e ADDR="$ADDR" \
		"$IMAGE" sh -euc "$1"
}

up() {
	as_root '
		modprobe ib_uverbs
		modprobe rdma_rxe
		if ! ip link show "$NETDEV" >/dev/null 2>&1; then
			# Refuse an address that some route other than the default
			# covers: it belongs to a real network.
			if ip -4 route show to match "${ADDR%/*}" | grep -v "^default" | grep -q .; then
				echo "$ADDR overlaps a route of this host; pick another SV_RXE_ADDR" >&2
				exit 1
			fi
			ip link add "$NETDEV" type dummy
		fi
		ip addr replace "$ADDR" dev "$NETDEV"
		ip link set "$NETDEV" up
		if ! rdma link show "$DEV/1" >/dev/null 2>&1; then
			rdma link add "$DEV" type rxe netdev "$NETDEV"
		fi
		# Let the user open the device: find its uverbs node.
		for node in /sys/class/infiniband_verbs/uverbs*; do
			if [ "$(cat "$node/ibdev")" = "$DEV" ]; then
				chmod 666 "/dev/infiniband/${node##*/}"
			fi
		done
	'
	status
}

down() {
	as_root '
		if rdma link show "$DEV/1" >/dev/null 2>&1; then
			rdma link delete "$DEV"
		fi
		if ip link show "$NETDEV" >/dev/null 2>&1; then
			ip link delete "$NETDEV"
		fi
	'
	echo "removed $DEV and $NETDEV (rdma_rxe stays loaded)"
}

status() {
	as_root '
		rdma link show "$DEV/1" 2>/dev/null || { echo "$DEV: not present"; exit 0; }
		ip -br addr show "$NETDEV"
		for node in /sys/class/infiniband_verbs/uverbs*; do
			if [ "$(cat "$node/ibdev")" = "$DEV" ]; then
				ls -l "/dev/infiniband/${node##*/}"
			fi
		done
		gids=/sys/class/infiniband/$DEV/ports/1/gids
		types=/sys/class/infiniband/$DEV/ports/1/gid_attrs/types
		for i in $(ls "$gids"); do
			gid=$(cat "$gids/$i")
			case $gid in 0000:0000:0000:0000:0000:0000:0000:0000) continue ;; esac
			echo "gid $i $gid $(cat "$types/$i" 2>/dev/null)"
		done
	'
}

case ${1:-} in
up) up ;;
down) down ;;
status) status ;;
*)
	echo "usage: $0 up|down|status" >&2
	exit 2
	;;
esac
