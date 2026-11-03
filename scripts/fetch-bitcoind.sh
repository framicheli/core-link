#!/usr/bin/env bash
# Downloads a Bitcoin Core release for the live tests and prints its bitcoind path:
#   BITCOIND_EXE=$(scripts/fetch-bitcoind.sh 31.2) cargo test -p bitcoin-core-rpc --test live -- --ignored
# Hashes are from each release's SHA256SUMS, checked against the Guix builder
# signatures (bitcoin-core/guix.sigs) when added. Linux x86_64 only.
# Downloads go to $BITCOIND_DIR (default target/bitcoind).
set -euo pipefail

version=${1:?usage: fetch-bitcoind.sh <version>}
case $version in
  29.5) sha=09a6d539c32186efc1dbda8808a70be35b8f0b44632cb51c77181d6caa0bd791 ;;
  30.4) sha=051a18c595d9a27df23cb7187ff9fb343e90943cb142af751e12787806500c3d ;;
  31.0) sha=d3e4c58a35b1d0a97a457462c94f55501ad167c660c245cb1ffa565641c65074 ;;
  31.2) sha=2f38996afaa74dfa469a23bf4727887133dfc09784df1f21ddf1c74d0dc82300 ;;
  *) echo "no pinned hash for Core $version" >&2; exit 1 ;;
esac

root=$(cd "$(dirname "$0")/.." && pwd)
dir=${BITCOIND_DIR:-$root/target/bitcoind}/$version
exe=$dir/bitcoin-$version/bin/bitcoind
if [[ ! -x $exe ]]; then
  tarball=bitcoin-$version-x86_64-linux-gnu.tar.gz
  mkdir -p "$dir"
  curl -fsSL -o "$dir/$tarball" "https://bitcoincore.org/bin/bitcoin-core-$version/$tarball"
  echo "$sha  $dir/$tarball" | sha256sum --check --quiet
  tar -xzf "$dir/$tarball" -C "$dir"
  rm "$dir/$tarball"
fi
echo "$exe"
