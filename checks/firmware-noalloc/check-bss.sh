#!/usr/bin/env bash
# Links the firmware optimised for the target given as the argument and holds its bus to `.bss`.
# An empty bus is all zero bytes, so the board's `static` takes RAM that the startup code zeroes
# and no flash. One byte of its initial value that is not zero moves the whole `static` into
# `.data`, whose initial image the flash keeps and the startup code copies.
set -euo pipefail
cd "$(dirname "$0")"
target="${1:?usage: check-bss.sh <target>}"
elf="target/$target/release/ruststream-firmware-noalloc"
host="$(rustc -vV | sed -n 's/^host: //p')"
nm="$(rustc --print sysroot)/lib/rustlib/$host/bin/llvm-nm"
if [ ! -x "$nm" ]; then
    echo "error: $nm is missing: add it with \`rustup component add llvm-tools\`" >&2
    exit 1
fi

RUSTFLAGS="" cargo build --release --locked --target "$target"
# A row of the System V format is: name|value|class|type|size|line|section.
row="$("$nm" --format=sysv --demangle "$elf" | grep -E '^ruststream_firmware_noalloc::BUS *\|')" || {
    echo "error: the $target firmware has no symbol for its bus" >&2
    exit 1
}
IFS='|' read -r _ _ _ _ size _ section <<<"$row"
section="${section// /}"
if [ "$section" != .bss ]; then
    echo "error: the $target firmware keeps its bus ($((16#$size)) bytes) in $section, not in" \
        ".bss: a byte of the bus's empty state is not zero, so the flash holds its image" >&2
    exit 1
fi
