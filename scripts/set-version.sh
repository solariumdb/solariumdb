#!/usr/bin/env bash
#
# Set a single version on every packable project and pack the NuGet packages.
#
# The version lives in Directory.Build.props, which every project imports. A project that sets its
# own <Version> overrides it, so the script rewrites that one too. A "packable project" is a .csproj
# that carries a <PackageId> and does not opt out with <IsPackable>false</IsPackable>. Tests and
# benchmarks opt out and are reported as skipped.
#
# Usage:
#   scripts/set-version.sh 1.2.0                  # rewrite <Version> everywhere, then pack
#   scripts/set-version.sh 1.2.0 --no-pack        # only rewrite the versions
#   scripts/set-version.sh 1.2.0 --dry-run        # show what would change, touch nothing
#   scripts/set-version.sh 1.2.0 -o /tmp/nupkg    # pack into a different output directory
#   scripts/set-version.sh 1.2.0 -c Debug         # pack a non-Release configuration
#
# Packages land in <repo>/packages by default, the directory the CI pack step uses.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PROPS="$REPO_ROOT/Directory.Build.props"

VERSION=""
CONFIGURATION="Release"
OUTPUT="$REPO_ROOT/packages"
PACK=1
DRY_RUN=0

usage() {
  sed -n '2,17p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
  exit "${1:-0}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)          usage 0 ;;
    --no-pack)          PACK=0; shift ;;
    --dry-run)          DRY_RUN=1; shift ;;
    -c|--configuration) CONFIGURATION="$2"; shift 2 ;;
    -o|--output)        OUTPUT="$2"; shift 2 ;;
    -*)                 echo "error: unknown option: $1" >&2; usage 1 ;;
    *)
      if [[ -n "$VERSION" ]]; then
        echo "error: unexpected argument: $1" >&2
        usage 1
      fi
      VERSION="$1"; shift ;;
  esac
done

if [[ -z "$VERSION" ]]; then
  echo "error: no version given" >&2
  usage 1
fi

# Accept NuGet-shaped versions: 1.2.3, 1.2.3.4, 1.2.3-beta.1, 1.2.3+build.5. A typo is rejected
# before it is written into any project file.
if [[ ! "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(\.[0-9]+)?(-[0-9A-Za-z.-]+)?(\+[0-9A-Za-z.-]+)?$ ]]; then
  echo "error: '$VERSION' is not a valid NuGet version (expected e.g. 1.2.0, 1.2.0-rc.1)" >&2
  exit 1
fi

read_version() {
  grep -oE '<Version>[^<]+</Version>' "$1" | head -n1 | sed -E 's#</?Version>##g' || true
}

read_package_id() {
  grep -oE '<PackageId>[^<]+</PackageId>' "$1" | head -n1 | sed -E 's#</?PackageId>##g'
}

# Replace only the first <Version> so a version on a nested element is left alone.
write_version() {
  local file="$1"
  VERSION="$VERSION" perl -0777 -pi -e 's{<Version>[^<]*</Version>}{"<Version>$ENV{VERSION}</Version>"}se' "$file"

  local written
  written="$(read_version "$file")"
  if [[ "$written" != "$VERSION" ]]; then
    echo "error: failed to write <Version> into ${file#"$REPO_ROOT"/} (found '${written:-<none>}')" >&2
    exit 1
  fi
}

# Discover the projects instead of a fixed list, so a new package cannot be forgotten.
PROJECTS=()
PACKABLE=()
SKIPPED=()
while IFS= read -r project; do
  PROJECTS+=("$project")
  if grep -qE '<IsPackable>[[:space:]]*false[[:space:]]*</IsPackable>' "$project"; then
    SKIPPED+=("$project (IsPackable=false)")
    continue
  fi
  if ! grep -qE '<PackageId>' "$project"; then
    SKIPPED+=("$project (no PackageId)")
    continue
  fi
  PACKABLE+=("$project")
done < <(find "$REPO_ROOT" -name '*.csproj' -not -path '*/bin/*' -not -path '*/obj/*' | sort)

if [[ ${#PACKABLE[@]} -eq 0 ]]; then
  echo "error: no packable projects found under $REPO_ROOT" >&2
  exit 1
fi

shared="$(read_version "$PROPS")"
if [[ -z "$shared" ]]; then
  echo "error: no <Version> in ${PROPS#"$REPO_ROOT"/}" >&2
  exit 1
fi

echo "Version:       $VERSION"
echo "Configuration: $CONFIGURATION"
echo "Output:        $OUTPUT"
echo "Pack:          $PACK"
echo

printf '  %-30s %s -> %s\n' "${PROPS#"$REPO_ROOT"/}" "$shared" "$VERSION"
[[ "$DRY_RUN" -eq 1 ]] || write_version "$PROPS"

# Every project, packable or not, gets the new version if it overrides the shared one: a test
# project that pins an old version would otherwise build against a different assembly version.
for project in "${PROJECTS[@]}"; do
  current="$(read_version "$project")"
  [[ -z "$current" ]] && continue

  printf '  %-30s %s -> %s\n' "${project#"$REPO_ROOT"/}" "$current" "$VERSION"
  [[ "$DRY_RUN" -eq 1 ]] || write_version "$project"
done

echo
echo "Packable:"
for project in "${PACKABLE[@]}"; do
  printf '  %-22s %s\n' "$(read_package_id "$project")" "${project#"$REPO_ROOT"/}"
done

if [[ ${#SKIPPED[@]} -gt 0 ]]; then
  echo
  echo "Skipped (not packable):"
  for entry in "${SKIPPED[@]}"; do
    echo "  ${entry#"$REPO_ROOT"/}"
  done
fi

if [[ "$DRY_RUN" -eq 1 ]]; then
  echo
  echo "Dry run: nothing written."
fi

if [[ "$PACK" -ne 1 || "$DRY_RUN" -eq 1 ]]; then
  echo
  echo "Skipping pack."
  exit 0
fi

mkdir -p "$OUTPUT"

echo
echo "Packing ${#PACKABLE[@]} project(s)..."

PACKED=()
for project in "${PACKABLE[@]}"; do
  package_id="$(read_package_id "$project")"
  expected="$OUTPUT/$package_id.${VERSION%%+*}.nupkg"

  # Drop a same-version leftover so a stale package cannot be mistaken for a fresh one.
  rm -f "$expected"

  echo
  echo "==> $package_id"
  dotnet pack "$project" -c "$CONFIGURATION" -o "$OUTPUT"

  if [[ ! -f "$expected" ]]; then
    echo "error: expected package not produced: $expected" >&2
    exit 1
  fi
  PACKED+=("$expected")
done

echo
echo "Packed ${#PACKED[@]} package(s) into $OUTPUT:"
for package in "${PACKED[@]}"; do
  printf '  %-40s %s\n' "$(basename "$package")" "$(du -h "$package" | cut -f1)"
done
