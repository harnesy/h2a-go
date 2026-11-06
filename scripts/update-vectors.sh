#!/bin/sh
# Copies the contract's test vectors from a harnsy checkout, unchanged, with
# a first line naming the commit they come from:
#
#   scripts/update-vectors.sh <harnsy checkout>
#
# harnsy writes them (testdata/h2a-v1/, its TestH2ATransportsAgree -update);
# this repository never edits them. Run `go test ./...` after.
set -eu
src=${1:?a harnsy checkout}
commit=$(git -C "$src" rev-parse --short HEAD)
dir=$(cd "$(dirname "$0")/.." && pwd)/testdata
for f in session.json codes.json; do
	[ -s "$src/testdata/h2a-v1/$f" ] || { echo "no $src/testdata/h2a-v1/$f" >&2; exit 1; }
	{ echo "from harnsy $commit, contract \"1\""; cat "$src/testdata/h2a-v1/$f"; } > "$dir/$f"
done
echo "testdata: from harnsy $commit"
