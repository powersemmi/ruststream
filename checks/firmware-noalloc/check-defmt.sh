#!/usr/bin/env bash
# Links the firmware optimised for the target given as the argument, in each defmt configuration,
# and holds the ELF to what the app selected:
#   none            defmt is not compiled: no defmt symbol at all.
#   defmt-unselected defmt is compiled and the app selects no logger: the framework brings none of
#                   its code, symbols or lines into the binary (the markers defmt itself keeps
#                   are all that remains).
#   defmt           the app selects `DefmtLogger`: the sink, the logger symbols and the framework's
#                   lines are all there.
set -euo pipefail
cd "$(dirname "$0")"
target="${1:?usage: check-defmt.sh <target>}"
elf="target/$target/release/ruststream-firmware-noalloc"

# A row is: features, then whether the ELF carries, in order: any `_defmt_` symbol, the logger
# symbols (`_defmt_acquire`), the framework's sink, a line the framework logs.
rows=(
    "none no no no no"
    "defmt-unselected yes no no no"
    "defmt yes yes yes yes"
)
for row in "${rows[@]}"; do
    read -r features any_symbol logger sink line <<<"$row"
    args=()
    [ "$features" = none ] || args=(--features "$features")
    DEFMT_LOG=warn RUSTFLAGS="" cargo build --release --locked --target "$target" "${args[@]}"
    for check in "_defmt_:$any_symbol" "_defmt_acquire:$logger" "DefmtSink:$sink" \
        "\"package\":\"ruststream-core\":$line"; do
        text="${check%:*}"
        want="${check##*:}"
        if grep -aq -- "$text" "$elf"; then found=yes; else found=no; fi
        if [ "$found" != "$want" ]; then
            echo "error: the $target firmware built with $features: \"$text\" found=$found, wanted $want" >&2
            exit 1
        fi
    done
done
