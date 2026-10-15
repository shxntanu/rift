#!/bin/sh
set -eu

version=${1:?usage: release.sh vX.Y.Z}
if ! printf '%s\n' "$version" | grep -Eq '^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$'; then
    echo 'release version must be vX.Y.Z (for example, v0.1.0)' >&2
    exit 1
fi

cd "$(dirname "$0")/.."
make cross VERSION="$version" BUILD_ID="$(git rev-parse HEAD)" SOURCE_DIR=

stage=$(mktemp -d)
trap 'rm -rf "$stage"' EXIT
trap 'exit 1' HUP INT TERM
for target in linux-amd64 linux-arm64 darwin-amd64 darwin-arm64; do
    cp "bin/rift-$target" "$stage/"
done
cp LICENSE README.md "$stage/"
mkdir -p dist
archive="rift_${version}.tar.gz"
COPYFILE_DISABLE=1 tar -czf "dist/$archive" -C "$stage" .
cp install.sh dist/install.sh
cd dist
if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$archive" > checksums.txt
else
    shasum -a 256 "$archive" > checksums.txt
fi
