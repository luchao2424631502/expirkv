#!/bin/sh
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
crate_dir=$(CDPATH= cd -- "$script_dir/.." && pwd -P)
deps_dir="$crate_dir/.deps"
downloads_dir="$deps_dir/downloads"
archive="$downloads_dir/leveldb-99b3c03.tar.gz"
archive_tmp="$archive.tmp.$$"
source_dir="$deps_dir/leveldb-source"
build_dir="$deps_dir/leveldb-build"
install_dir="$deps_dir/leveldb-install"

leveldb_version="1.23"
leveldb_commit="99b3c03b3284f5886f9ef9a4ef703d57373e61be"
archive_sha256="bc87b9bbc5674c91246a89813355e78401759761342cc049e1c3d56350a8a9d1"
archive_url="https://github.com/google/leveldb/archive/99b3c03.tar.gz"
provenance_sha256="4bbae7fe9c50827b9abdc3c076a872b55f55fd8468ba48de418b933a3e204c29"
expected_provenance=$(printf '%s\n' \
    'version=1.23' \
    'commit=99b3c03b3284f5886f9ef9a4ef703d57373e61be' \
    'archive_sha256=bc87b9bbc5674c91246a89813355e78401759761342cc049e1c3d56350a8a9d1' \
    'build_type=Release' \
    'shared_libraries=off' \
    'crc32c_external=off' \
    'snappy=off' \
    'tcmalloc=off')

cleanup_download() {
    rm -f -- "$archive_tmp"
}

fail() {
    printf 'bootstrap_leveldb.sh: %s\n' "$*" >&2
    exit 1
}

require_command() {
    command -v "$1" >/dev/null 2>&1 || fail "required command not found: $1"
}

sha256_file() {
    if command -v shasum >/dev/null 2>&1; then
        shasum -a 256 "$1" | awk '{print $1}'
    elif command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | awk '{print $1}'
    else
        fail "neither shasum nor sha256sum is available"
    fi
}

valid_install() {
    test -f "$install_dir/include/leveldb/c.h" &&
        test -f "$install_dir/include/leveldb/db.h" &&
        test -f "$install_dir/lib/libleveldb.a" &&
        test -f "$install_dir/share/kv_bench/leveldb-provenance.txt" &&
        grep -Fq 'kMajorVersion = 1;' "$install_dir/include/leveldb/db.h" &&
        grep -Fq 'kMinorVersion = 23;' "$install_dir/include/leveldb/db.h" &&
        test "$(sha256_file "$install_dir/share/kv_bench/leveldb-provenance.txt")" = \
            "$provenance_sha256"
}

if test -L "$deps_dir"; then
    fail "$deps_dir must not be a symbolic link"
fi
if test -e "$deps_dir" && ! test -d "$deps_dir"; then
    fail "$deps_dir exists but is not a directory"
fi
mkdir -p -- "$deps_dir"
physical_deps_dir=$(CDPATH= cd -- "$deps_dir" && pwd -P)
test "$physical_deps_dir" = "$deps_dir" ||
    fail "physical dependency directory escaped $deps_dir"

for generated_path in "$downloads_dir" "$source_dir" "$build_dir" "$install_dir"; do
    test ! -L "$generated_path" ||
        fail "$generated_path must not be a symbolic link"
done

if valid_install; then
    printf 'LevelDB %s (%s) already bootstrapped at %s\n' \
        "$leveldb_version" "$leveldb_commit" "$install_dir"
    exit 0
fi

require_command curl
require_command tar
require_command cmake
require_command awk
require_command grep

mkdir -p -- "$downloads_dir"
physical_downloads_dir=$(CDPATH= cd -- "$downloads_dir" && pwd -P)
test "$physical_downloads_dir" = "$downloads_dir" ||
    fail "physical downloads directory escaped $downloads_dir"
trap cleanup_download EXIT HUP INT TERM
if test -f "$archive"; then
    actual_sha256=$(sha256_file "$archive")
    test "$actual_sha256" = "$archive_sha256" ||
        fail "cached archive checksum mismatch: expected $archive_sha256, got $actual_sha256"
else
    printf 'Downloading official LevelDB commit %s...\n' "$leveldb_commit"
    curl --fail --location --silent --show-error --output "$archive_tmp" "$archive_url"
    actual_sha256=$(sha256_file "$archive_tmp")
    test "$actual_sha256" = "$archive_sha256" ||
        fail "downloaded archive checksum mismatch: expected $archive_sha256, got $actual_sha256"
    mv -- "$archive_tmp" "$archive"
fi

case "$deps_dir" in
    "$crate_dir/.deps") ;;
    *) fail "refusing to rebuild outside the benchmark .deps directory" ;;
esac

rm -rf -- "$source_dir" "$build_dir" "$install_dir"
mkdir -p -- "$source_dir" "$build_dir" "$install_dir"
tar -xzf "$archive" -C "$source_dir" --strip-components=1

grep -Fq 'project(leveldb VERSION 1.23.0 LANGUAGES C CXX)' "$source_dir/CMakeLists.txt" ||
    fail "source archive does not declare LevelDB 1.23.0"
grep -Fq 'kMajorVersion = 1;' "$source_dir/include/leveldb/db.h" ||
    fail "source archive has an unexpected major version"
grep -Fq 'kMinorVersion = 23;' "$source_dir/include/leveldb/db.h" ||
    fail "source archive has an unexpected minor version"

cmake -S "$source_dir" -B "$build_dir" \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX="$install_dir" \
    -DCMAKE_INSTALL_LIBDIR=lib \
    -DBUILD_SHARED_LIBS=OFF \
    -DLEVELDB_BUILD_TESTS=OFF \
    -DLEVELDB_BUILD_BENCHMARKS=OFF \
    -DLEVELDB_INSTALL=ON \
    -DHAVE_CRC32C=OFF \
    -DHAVE_SNAPPY=OFF \
    -DHAVE_TCMALLOC=OFF
cmake --build "$build_dir" --config Release --parallel
cmake --install "$build_dir" --config Release

mkdir -p -- "$install_dir/share/kv_bench"
printf '%s\n' "$expected_provenance" > \
    "$install_dir/share/kv_bench/leveldb-provenance.txt"

valid_install || fail "installed LevelDB artifacts did not pass validation"
printf 'Bootstrapped LevelDB %s (%s) at %s\n' \
    "$leveldb_version" "$leveldb_commit" "$install_dir"
