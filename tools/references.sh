#!/usr/bin/env bash
set -euo pipefail

scan=${1:?provide the exact reference audit source}
references=$(mktemp -d)
trap 'rm -rf "$references"' EXIT
mkdir -p "$references/healthy/bin" "$references/forbidden/bin" "$references/unwrapped/bin" "$references/elf" "$references/scratch"
export TMPDIR="$references/scratch"
printf '#!/bin/sh\nexec /nix/store/allowed/bin/program\n' > "$references/healthy/bin/notice"
printf '#!/bin/sh\nexit 0\n' > "$references/healthy/bin/.notice-wrapped"
printf '#!/bin/sh\nexec "%s/forbidden"\n' "$TMPDIR" > "$references/forbidden/bin/notice"
printf '#!/bin/sh\nexit 0\n' > "$references/forbidden/bin/.notice-wrapped"
printf '#!/bin/sh\nexec "%s/forbidden"\n' "$TMPDIR" > "$references/unwrapped/bin/notice"

reference_status() {
    local expected=$1 directory=$2 fault=$3 label=$4 status
    if (
        source "$scan"
        case "$fault" in
            none) ;;
            enumeration|enumeration_cleanup)
                if [[ "$fault" == enumeration_cleanup ]]; then rm() { return 41; }; fi
                find() { printf '%s\0' "$references/healthy/bin/notice"; return 37; }
                ;;
            reader) read() { return 43; } ;;
            early_end) read() { return 1; } ;;
            header) od() { return 38; } ;;
            search) grep() { return 39; } ;;
            storage) mktemp() { return 40; } ;;
            cleanup) rm() { return 41; } ;;
            list_open)
                mktemp() { printf '%s/absent/list\n' "$TMPDIR"; }
                rm() { return 0; }
                ;;
            rpath) patchelf() { printf '/nix/store/allowed/lib\n'; } ;;
            forbidden_rpath) patchelf() { printf '/nix/store/allowed/lib:%s/forbidden\n' "$TMPDIR"; } ;;
            first_rpath) patchelf() { printf '%s/forbidden:/nix/store/allowed/lib\n' "$TMPDIR"; } ;;
            similar_rpath) patchelf() { printf '%s-other/lib:/nix/store/prefix%s/lib\n' "$TMPDIR" "$TMPDIR"; } ;;
            rpath_error) patchelf() { return 42; } ;;
            *) printf 'unknown reference fault: %s\n' "$fault" >&2; exit 2 ;;
        esac
        auditTmpdir "$directory"
    ) > "$references/observation.log" 2>&1; then
        status=0
    else
        status=$?
    fi
    if [[ "$status" != "$expected" ]]; then
        cat "$references/observation.log" >&2
        printf 'reference %s: expected exit %s, received %s\n' "$label" "$expected" "$status" >&2
        exit 1
    fi
    if grep -E 'Segmentation fault|Bus error|command not found' "$references/observation.log"; then
        printf 'reference %s contains a tool crash\n' "$label" >&2
        exit 1
    fi
    case "$label" in
        forbidden_script) grep -F "wrapper script $directory/bin/notice contains a forbidden reference to $TMPDIR/" "$references/observation.log" > /dev/null ;;
        filename_preserved)
            local observation expected_notice
            observation=$(cat "$references/observation.log")
            expected_notice="wrapper script $directory/bin/$reference_name contains a forbidden reference to $TMPDIR/"
            [[ "$observation" == *"$expected_notice"* ]]
            ;;
        forbidden_elf|first_forbidden_elf) grep -F "RPATH of binary $directory/object contains a forbidden reference to $TMPDIR/" "$references/observation.log" > /dev/null ;;
    esac
    find "$TMPDIR" -mindepth 1 -print -quit > "$references/residue"
    if [[ "$fault" == cleanup || "$fault" == enumeration_cleanup ]]; then
        grep -F 'cannot remove reference audit list' "$references/observation.log" > /dev/null
        if [[ ! -s "$references/residue" ]]; then
            printf 'cleanup fault did not exercise owned temporary storage\n' >&2
            exit 1
        fi
        rm -rf "$TMPDIR"
        mkdir "$TMPDIR"
    elif [[ -s "$references/residue" ]]; then
        printf 'reference %s left temporary storage after its scan\n' "$label" >&2
        exit 1
    fi
    printf 'PASS reference %s (%s)\n' "$label" "$LC_ALL"
}

for reference_locale in C en_US.UTF-8; do
    export LC_ALL="$reference_locale"
    reference_charmap=$(locale charmap)
    case "$reference_locale:$reference_charmap" in
        C:ANSI_X3.4-1968|C:US-ASCII|en_US.UTF-8:UTF-8) ;;
        *) printf 'unexpected reference locale: %s:%s\n' "$reference_locale" "$reference_charmap" >&2; exit 1 ;;
    esac
    reference_status 0 "$references/healthy" none healthy_script
    reference_status 37 "$references/healthy" enumeration failed_enumeration
    reference_status 1 "$references/forbidden" none forbidden_script
    reference_status 0 "$references/unwrapped" none unwrapped_script
    reference_status 0 "$references/absent" none absent_output
    reference_status 43 "$references/healthy" reader failed_reader
    reference_status 1 "$references/healthy" early_end premature_list_end
    reference_status 38 "$references/healthy" header failed_header
    reference_status 39 "$references/healthy" search failed_search
    reference_status 40 "$references/healthy" storage failed_storage
    reference_status 41 "$references/healthy" cleanup failed_cleanup
    reference_status 37 "$references/healthy" enumeration_cleanup failed_enumeration_and_cleanup
    reference_status 1 "$references/healthy" list_open failed_list_open
    for reference_name in 'two words' $'two\nlines'; do
        mkdir -p "$references/names/bin"
        cp "$references/forbidden/bin/notice" "$references/names/bin/$reference_name"
        cp "$references/healthy/bin/.notice-wrapped" "$references/names/bin/.$reference_name-wrapped"
        reference_status 1 "$references/names" none filename_preserved
        cp "$references/healthy/bin/notice" "$references/names/bin/$reference_name"
        reference_status 0 "$references/names" none healthy_filename
        rm -rf "$references/names"
    done
    mkdir -p "$references/healthy/.build-id/bin"
    cp "$references/forbidden/bin/notice" "$references/healthy/.build-id/bin/notice"
    cp "$references/forbidden/bin/.notice-wrapped" "$references/healthy/.build-id/bin/.notice-wrapped"
    printf '\050\265\057\375\004\130\024\362' > "$references/healthy/archive.zst"
    printf '\000#!\n%s/forbidden\n' "$TMPDIR" > "$references/healthy/bin/null"
    printf '\n#!\n%s/forbidden\n' "$TMPDIR" > "$references/healthy/bin/newline"
    printf '#\n%s/forbidden\n' "$TMPDIR" > "$references/healthy/bin/short"
    for filename in null newline short; do
        printf '#!/bin/sh\nexit 0\n' > "$references/healthy/bin/.$filename-wrapped"
    done
    reference_status 0 "$references/healthy" none exact_headers_and_exclusions
    mkdir -p "$references/near"
    printf '\000\177ELF' > "$references/near/null"
    printf '\n\177ELF' > "$references/near/newline"
    reference_status 0 "$references/near" rpath_error exact_elf_header
    # Four-byte fixtures exercise the patchelf port, not its ELF parser.
    printf '\177ELF' > "$references/elf/object"
    reference_status 0 "$references/elf" rpath healthy_elf
    reference_status 1 "$references/elf" forbidden_rpath forbidden_elf
    reference_status 1 "$references/elf" first_rpath first_forbidden_elf
    reference_status 0 "$references/elf" similar_rpath similar_elf_prefix
    reference_status 42 "$references/elf" rpath_error failed_rpath
    reference_status 38 "$references/elf" header failed_elf_header
    printf '#!/bin/sh\nexec /nix/store/allowed/bin/program\n' > "$references/healthy/bin/notice"
done
