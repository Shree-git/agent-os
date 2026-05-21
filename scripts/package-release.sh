#!/usr/bin/env sh
set -eu

target="${1:-}"
out_dir="${OUT_DIR:-target/dist}"
package_name="agent-os"
version="${VERSION:-$(cargo metadata --no-deps --format-version 1 | sed -n 's/.*"version":"\([^"]*\)".*/\1/p' | head -n 1)}"

if [ -z "$version" ]; then
  echo "could not determine package version" >&2
  exit 1
fi

target_exe_suffix() {
  case "$1" in
    *windows*|*mingw*|*msvc*) printf '.exe' ;;
    *) printf '' ;;
  esac
}

host_exe_suffix() {
  case "$(uname -s 2>/dev/null || echo unknown)" in
    MINGW*|MSYS*|CYGWIN*) printf '.exe' ;;
    *) printf '' ;;
  esac
}

if [ -n "$target" ]; then
  exe="$(target_exe_suffix "$target")"
  cargo build --locked --release --target "$target"
  bin_path="target/$target/release/$package_name$exe"
  target_name="$target"
else
  exe="$(host_exe_suffix)"
  cargo build --locked --release
  bin_path="target/release/$package_name$exe"
  target_name="$(uname -s | tr '[:upper:]' '[:lower:]')-$(uname -m)"
fi

if [ ! -f "$bin_path" ]; then
  echo "built binary not found: $bin_path" >&2
  exit 1
fi

stage="$out_dir/$package_name-$version-$target_name"
archive="$out_dir/$package_name-$version-$target_name.tar.gz"
rm -rf "$stage" "$archive" "$archive.sha256" "$archive.sig" "$archive.pem"
mkdir -p "$stage/bin" "$stage/completions"
cp "$bin_path" "$stage/bin/$package_name$exe"
cp README.md LICENSE CHANGELOG.md "$stage/"

for shell in bash zsh fish elvish powershell; do
  "$bin_path" completions "$shell" > "$stage/completions/$package_name.$shell"
done

mkdir -p "$out_dir"
tar -czf "$archive" -C "$out_dir" "$(basename "$stage")"

if command -v shasum >/dev/null 2>&1; then
  shasum -a 256 "$archive" > "$archive.sha256"
elif command -v sha256sum >/dev/null 2>&1; then
  sha256sum "$archive" > "$archive.sha256"
else
  echo "neither shasum nor sha256sum is available" >&2
  exit 1
fi

if [ "${AGENT_OS_SIGN_RELEASES:-}" = "1" ]; then
  if command -v cosign >/dev/null 2>&1; then
    cosign sign-blob --yes \
      --output-signature "$archive.sig" \
      --output-certificate "$archive.pem" \
      "$archive"
  else
    echo "AGENT_OS_SIGN_RELEASES=1 requires cosign on PATH" >&2
    exit 1
  fi
fi

printf '%s\n' "$archive"
printf '%s\n' "$archive.sha256"
if [ -f "$archive.sig" ]; then
  printf '%s\n' "$archive.sig"
fi
if [ -f "$archive.pem" ]; then
  printf '%s\n' "$archive.pem"
fi
