# Replace the existing stdenv audit implementation without registering another hook.
# Check every producer explicitly: fixup callers may disable shell errexit.
auditTmpdir() {
    local directory=$1 list status cleanup_status
    [[ -e "$directory" ]] || return 0
    printf 'checking for references to %s/ in %s...\n' "$TMPDIR" "$directory" || return "$?"
    list=$(mktemp) || return "$?"

    if (
        find "$directory" -type f -not -path '*/.build-id/*' -print0 > "$list" || exit "$?"
        # An empty record cannot be a filename. Reaching it proves complete
        # consumption; an ordinary EOF must not hide a failed list reader.
        printf '\0' >> "$list" || exit "$?"
        while :; do
            if IFS= read -r -d '' file; then
                [[ -n "$file" ]] || break
            else
                status=$?
                printf 'cannot finish reading reference audit list %s\n' "$list" >&2
                exit "$status"
            fi
            header=$(LC_ALL=C od -An -N4 -tx1 -- "$file") || exit "$?"
            header=${header//[[:space:]]/}
            case "$header" in
                7f454c46)
                    rpath=$(patchelf --print-rpath "$file") || exit "$?"
                    case ":$rpath" in
                        *":$TMPDIR/"*)
                            printf 'RPATH of binary %s contains a forbidden reference to %s/\n' "$file" "$TMPDIR"
                            exit 1
                            ;;
                    esac
                    ;;
                2321*)
                    filename=${file##*/}
                    directory=${file%/*}
                    if [[ -e "$directory/.$filename-wrapped" ]]; then
                        if grep -F -- "$TMPDIR/" "$file" > /dev/null; then
                            printf 'wrapper script %s contains a forbidden reference to %s/\n' "$file" "$TMPDIR"
                            exit 1
                        else
                            status=$?
                            [[ "$status" == 1 ]] || exit "$status"
                        fi
                    fi
                    ;;
            esac
        done < "$list" || exit "$?"
    ); then
        status=0
    else
        status=$?
    fi

    if rm -f -- "$list"; then
        return "$status"
    else
        cleanup_status=$?
        printf 'cannot remove reference audit list %s (exit %s)\n' "$list" "$cleanup_status" >&2
        if [[ "$status" != 0 ]]; then
            return "$status"
        fi
        return "$cleanup_status"
    fi
}
