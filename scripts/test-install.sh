#!/bin/sh
set -eu

# Exercise the real installer offline with release assets and a fake curl.
cd "$(dirname "$0")/.."
TEST_ROOT=$(mktemp -d)
export TEST_ROOT
trap 'rm -rf "$TEST_ROOT"' EXIT
trap 'exit 1' HUP INT TERM
mkdir -p "$TEST_ROOT/assets" "$TEST_ROOT/bin" "$TEST_ROOT/payload"
for target in linux-amd64 linux-arm64 darwin-amd64 darwin-arm64; do
    printf '#!/bin/sh\necho "rift v0.1.0 %s"\n' "$target" > "$TEST_ROOT/payload/rift-$target"
done
archive=rift_v0.1.0.tar.gz
COPYFILE_DISABLE=1 tar -czf "$TEST_ROOT/assets/$archive" -C "$TEST_ROOT/payload" .
if command -v sha256sum >/dev/null 2>&1; then
    (cd "$TEST_ROOT/assets"; sha256sum "$archive" > checksums.txt)
else
    (cd "$TEST_ROOT/assets"; shasum -a 256 "$archive" > checksums.txt)
fi
cat > "$TEST_ROOT/bin/curl" <<'EOF'
#!/bin/sh
set -eu
[ "${TEST_DOWNLOAD_FAIL:-0}" = 0 ] || exit 22
output=
url=
while [ "$#" -gt 0 ]; do
    case "$1" in
        -o) output=$2; shift 2 ;;
        -w) shift 2 ;;
        -*) shift ;;
        *) url=$1; shift ;;
    esac
done
case "$url" in
    https://github.com/shxntanu/rift/releases/latest)
        printf 'https://github.com/shxntanu/rift/releases/tag/v0.1.0' ;;
    https://github.com/shxntanu/rift/releases/download/v0.1.0/*)
        cp "$TEST_ROOT/assets/${url##*/}" "$output" ;;
    *) echo "unexpected URL: $url" >&2; exit 1 ;;
esac
EOF
cat > "$TEST_ROOT/bin/uname" <<'EOF'
#!/bin/sh
case "$1" in
    -s) echo "$TEST_OS" ;;
    -m) echo "$TEST_ARCH" ;;
    *) exit 1 ;;
esac
EOF
chmod +x "$TEST_ROOT/bin/curl" "$TEST_ROOT/bin/uname"
PATH="$TEST_ROOT/bin:$PATH"
RIFT_INSTALL_DIR="$TEST_ROOT/install with spaces"
export PATH RIFT_INSTALL_DIR

for target in linux-amd64 linux-arm64 darwin-amd64 darwin-arm64; do
    case "$target" in
        linux-amd64) TEST_OS=Linux; TEST_ARCH=x86_64 ;;
        linux-arm64) TEST_OS=Linux; TEST_ARCH=aarch64 ;;
        darwin-amd64) TEST_OS=Darwin; TEST_ARCH=x86_64 ;;
        darwin-arm64) TEST_OS=Darwin; TEST_ARCH=arm64 ;;
    esac
    export TEST_OS TEST_ARCH
    sh install.sh v0.1.0 > "$TEST_ROOT/log" 2>&1 || { cat "$TEST_ROOT/log"; exit 1; }
    [ "$("$RIFT_INSTALL_DIR/rift" --version)" = "rift v0.1.0 $target" ]
    for sibling in linux-amd64 linux-arm64 darwin-amd64 darwin-arm64; do
        cmp "$TEST_ROOT/payload/rift-$sibling" "$RIFT_INSTALL_DIR/rift-$sibling"
        [ -x "$RIFT_INSTALL_DIR/rift-$sibling" ]
    done
done
# The documented curl-pipe form resolves latest and upgrades an existing install.
# shellcheck disable=SC2002
cat install.sh | sh > "$TEST_ROOT/log" 2>&1 || { cat "$TEST_ROOT/log"; exit 1; }
cmp "$TEST_ROOT/payload/rift-darwin-arm64" "$RIFT_INSTALL_DIR/rift"

expect_failure() {
    if sh install.sh "$@" > "$TEST_ROOT/log" 2>&1; then
        echo 'installer unexpectedly succeeded' >&2
        exit 1
    fi
    # Failures must leave the existing installation intact.
    cmp "$TEST_ROOT/payload/rift-darwin-arm64" "$RIFT_INSTALL_DIR/rift"
}
expect_failure v1.2
expect_failure v01.2.3
expect_failure v0.1.0 extra
TEST_OS=FreeBSD
expect_failure v0.1.0
TEST_OS=Darwin
TEST_DOWNLOAD_FAIL=1
export TEST_DOWNLOAD_FAIL
expect_failure v0.1.0
TEST_DOWNLOAD_FAIL=0
printf 'corrupted\n' >> "$TEST_ROOT/assets/$archive"
expect_failure v0.1.0
grep -Eq 'FAILED|did not match' "$TEST_ROOT/log"
printf 'invalid checksum\n' > "$TEST_ROOT/assets/checksums.txt"
expect_failure v0.1.0
grep -q 'missing or invalid archive checksum' "$TEST_ROOT/log"
echo 'Installer checks passed (all platforms, latest, upgrades, checksum and download failures).'
