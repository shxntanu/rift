#!/bin/sh
set -eu

# Keep the entire installer in a function so a truncated curl pipe cannot
# execute a partially downloaded script.
main() {
    if [ "$#" -gt 1 ]; then
        echo 'usage: install.sh [vX.Y.Z]' >&2
        exit 1
    fi
    version=${1:-latest}
    install_dir=${RIFT_INSTALL_DIR:-"$HOME/.local/bin"}
    releases=https://github.com/shxntanu/rift/releases

    case "$(uname -s)/$(uname -m)" in
        Linux/x86_64|Linux/amd64) target=linux-amd64 ;;
        Linux/aarch64|Linux/arm64) target=linux-arm64 ;;
        Darwin/x86_64) target=darwin-amd64 ;;
        Darwin/arm64) target=darwin-arm64 ;;
        *) echo 'rift supports Linux and macOS on amd64 and arm64' >&2; exit 1 ;;
    esac
    for cmd in curl tar mktemp; do
        command -v "$cmd" >/dev/null 2>&1 || { echo "missing required command: $cmd" >&2; exit 1; }
    done
    if command -v sha256sum >/dev/null 2>&1; then
        checksum=sha256sum
    elif command -v shasum >/dev/null 2>&1; then
        checksum='shasum -a 256'
    else
        echo 'SHA-256 verification requires sha256sum or shasum' >&2
        exit 1
    fi

    if [ "$version" = latest ]; then
        url=$(curl -fsSLI -o /dev/null -w '%{url_effective}' "$releases/latest")
        version=${url##*/}
    fi
    if ! printf '%s\n' "$version" | grep -Eq '^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$'; then
        echo "invalid release version: $version (expected vX.Y.Z)" >&2
        exit 1
    fi

    tmp=$(mktemp -d)
    stage=
    trap 'rm -rf "$tmp"; if [ -n "$stage" ]; then rm -rf "$stage"; fi' EXIT
    trap 'exit 1' HUP INT TERM
    archive="rift_${version}.tar.gz"
    base="$releases/download/$version"
    echo "Downloading rift $version..."
    curl -fsSL "$base/$archive" -o "$tmp/$archive"
    curl -fsSL "$base/checksums.txt" -o "$tmp/checksums.txt"
    # Verify only the expected archive, even if the manifest gains more assets.
    expected=$(awk -v file="$archive" '$2 == file { print $1 }' "$tmp/checksums.txt")
    if [ "${#expected}" -ne 64 ] || ! printf '%s\n' "$expected" | grep -Eq '^[0-9a-fA-F]+$'; then
        echo 'missing or invalid archive checksum' >&2
        exit 1
    fi
    (cd "$tmp"; printf '%s  %s\n' "$expected" "$archive" | $checksum -c -)
    tar -xzf "$tmp/$archive" -C "$tmp"

    mkdir -p "$install_dir"
    stage=$(mktemp -d "$install_dir/.rift-install.XXXXXX")
    # Rift uploads these sibling binaries when the remote platform differs.
    for platform in linux-amd64 linux-arm64 darwin-amd64 darwin-arm64; do
        cp "$tmp/rift-$platform" "$stage/"
        chmod 755 "$stage/rift-$platform"
    done
    cp "$stage/rift-$target" "$stage/rift"
    for name in rift-linux-amd64 rift-linux-arm64 rift-darwin-amd64 rift-darwin-arm64 rift; do
        mv -f "$stage/$name" "$install_dir/$name"
    done
    echo "Installed rift $version to $install_dir/rift"
    # Print a literal $PATH for the user to paste into their shell.
    # shellcheck disable=SC2016
    case ":$PATH:" in
        *":$install_dir:"*) ;;
        *) printf 'Add it to your PATH: export PATH="%s:$PATH"\n' "$install_dir" ;;
    esac
}

main "$@"
