#!/usr/bin/env bash
set -euo pipefail

architecture=$(cd -P -- "${BASH_SOURCE[0]%/*}/.." && pwd -P)
guard=$architecture/owner/guard.sh
fixture=$(mktemp -d)
trap 'rm -rf -- "$fixture"' EXIT
fixture=$(cd -P -- "$fixture" && pwd -P)
export GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1
export HARNESS_MAIN=main HARNESS_OWNER_WORKFLOWS=checks.yml
P=$fixture/owner
W=$fixture/lanes
clone=$P/.cache/harness/rules594/repo
work=$P/.cache/harness/rules594/step
export CLAUDE_PROJECT_DIR=$P
git init -q -b main "$P"
git -C "$P" -c user.name=Fixture -c user.email=fixture@example.invalid \
  commit --allow-empty -qm initial
mkdir -p "$W" "${clone%/*}" "$fixture/scratch"
for lane in server studio nix; do
  git -C "$P" worktree add -qb "$lane" "$W/$lane"
done
git -C "$P" worktree add -qb unicode "$W/Ünicode"
git clone -q --local "$P" "$clone"
git -C "$clone" worktree add -qb feat/x "$work"
git -C "$P" worktree add -qb owner-cache "$P/.cache/linked"
ln -s "$W/server" "$fixture/scratch/lane-link"

allowed=0 denied=0
pending=()
wait_cases() {
  local pid failed=0
  for pid in "${pending[@]+"${pending[@]}"}"; do
    wait "$pid" || failed=1
  done
  pending=()
  return "$failed"
}

queue_case() {
  "$@" &
  pending+=("$!")
  if [ "${#pending[@]}" -eq 4 ]; then wait_cases; fi
}

check_reason() {
  if ! jq -es --arg reason "$1" '
    . == [{hookSpecificOutput:{hookEventName:"PreToolUse",permissionDecision:"deny",
                               permissionDecisionReason:$reason}}]
  ' <<< "$output" >/dev/null; then
    printf 'owner-guard-test: %s\n  expected: %s\n  received: %s\n' \
      "$2" "$1" "${output:-<no output>}" >&2
    return 1
  fi
}

expect_success() {
  local status=0
  "$@" || status=$?
  if [ "$status" != 0 ]; then
    printf 'owner-guard-test: expected exit 0, got %s\n' "$status" >&2
    return 1
  fi
}

call() {
  local tool=$1 value=$2
  jq -cn --arg tool "$tool" --arg value "$value" --arg cwd "${call_cwd:-$P}" \
    --arg key "${path_key:-}" '
    {tool_name:$tool, cwd:$cwd,
     tool_input: {(if $tool == "Bash" then "command"
                   elif $key != "" then $key
                   elif $tool == "NotebookEdit" then "notebook_path"
                   else "file_path" end):$value}}
  ' | expect_success "$BASH" "$guard"
}

check_allow() {
  local tool=$2 result="$fixture/allow.$3"
  call "$tool" "$1" > "$result" || return
  if [ -s "$result" ]; then
    printf 'owner-guard-test: %s %s\n  expected empty output\n' "$tool" "$1" >&2
    cat "$result" >&2
    return 1
  fi
}

allow() {
  allowed=$((allowed + 1))
  queue_case check_allow "$1" "${2:-Bash}" "$allowed"
}

check_deny() {
  local rule=$1 seen=$2 value=$3 tool=${4:-Bash} action output
  case "$rule" in
    lane-worktree) action='Read it; write a card for that lane.' ;;
    own-checkout) action='Make the change as a step in a clone outside the checkout.' ;;
    main-branch) action='Land through the gate and the merge turn (architecture/land.sh).' ;;
    machine) action='Ask Joel in one sentence with the cost and the choices.' ;;
    deploy) action='Put it on the card with the label needs-joel.' ;;
    *) return 1 ;;
  esac
  output=$(call "$tool" "$value") || return
  check_reason "owner guard: $rule: $seen. $action" "$tool $value"
}

deny() {
  denied=$((denied + 1))
  queue_case check_deny "$@"
}

allow "tail -n 1 $W/server/.cache/run.history"
allow "cat $W/nix/.cache/run.status"
# shellcheck disable=SC2016
allow "W=$W;"' for lane in server studio nix; do echo "$lane: $(cat $W/$lane/.cache/run.status)"; done'
allow "tail -n 40 $W/studio/.cache/gate.log > $P/.cache/harness/x.log"
allow "cp $W/server/.cache/gate.log $P/.cache/harness/gate-server.log"
allow "git -C $W/studio log --format=%h -3"
allow "git -C $W/server status --short"
allow "cd $W/nix && git log -1 && ls .cache"
allow "git -C $P fetch -q origin main"
allow "git -C $P merge -q --ff-only origin/main"
allow "git -C $P pull --ff-only"
allow "git -C $P log -1"
allow "printf 'x\n' >> $P/.cache/harness/STATE.md"
for tool in Write Edit; do
  allow "$P/.cache/harness/STATE.md" "$tool"
  allow "$fixture/scratch/note" "$tool"
  allow "$HOME/owner-guard-scratch" "$tool"
done
allow "$P/.cache/harness/notes.ipynb" NotebookEdit
path_key=file_path allow "$P/.cache/harness/notes.ipynb" NotebookEdit
allow "git clone -q $P $P/.cache/harness/x"
allow "git clone -q $P $fixture/scratch/outside"
call_cwd="$fixture/scratch" allow "git clone -q $P"
call_cwd="$P/.cache" allow "git clone -q $P"
allow "git -C $clone commit -q -F /abs/msg"
allow "git -C $clone push origin feat/x"
allow "git -C $clone worktree add ../other feat/y"
allow "git -C $P worktree add '$fixture/outside' feat/y"
allow "git -C $P worktree remove '$fixture/outside'"
allow "codex exec -m m -s workspace-write -C $work 'text'"
allow "sed -i s/a/b/ $work/note"
allow "rm $work/note"
allow "chmod 644 $work/note; truncate -s 0 $work/note; sd a b $work/note"
allow "git -C $P/.cache/linked checkout feat/x"
allow "gh issue comment 5 -R o/r -F body.md"
allow "gh issue edit 5 --title title"
allow "gh issue close 5"
allow "gh project item-edit --id id"
allow "gh pr create --fill"
allow "gh pr merge 7 --squash"
allow "gh pr merge 7 --squash --body '--admin'"
allow "gh api repos/o/r/actions/jobs/1/logs"
allow "gh api graphql -f query='query { viewer { login } }'"
allow "gh api repos/o/r/rulesets -X GET"
allow "gh auth token --user someone"
allow "gh workflow run checks.yml --ref main"
allow "nix build .#x --no-link"
allow "nix eval .#x"
allow 'env'
allow 'exec 2>&1'
allow "bash architecture/measure.sh a b 2"
allow "bash architecture/measure.sh $W name 2"
allow "bash architecture/measure.sh $W/server name 2"
allow "bash architecture/measure.sh '-c' 'sudo true'"
allow "ssh host 'journalctl -n 20'"
allow "ssh host nvidia-smi"
allow "launchctl list"
allow "launchctl kickstart gui/1/a"
allow "systemctl status service"
allow "terraform -chdir=infra plan -var message=apply"
allow "npm install local-package"
allow "rg -n sudo $P/.cache/go.sh"
allow 'echo "run sudo and terraform apply"'
allow 'gh issue comment 5 -b "never use gh auth switch or git push --force"'
allow '   # sudo true'
allow "cat > $P/.cache/rules <<'BODY'"$'\nsudo rm -rf /\nterraform apply\nBODY'
# shellcheck disable=SC2016
allow "cat > $P/.cache/rules <<BODY"$'\n$(sudo true)\nBODY'
allow 'echo x 2>&1'
# shellcheck disable=SC2016
allow 'echo x 2>&"$FD"'
allow "echo x 2>&1 >> $P/.cache/log"
allow "git -C $W/server branch --list 'feat/*'"
allow "git -C $W/server branch --contains HEAD"
allow "git -C $W/server branch -a -v -vv --list --contains HEAD --merged HEAD --no-merged other --format '%(refname)'"
allow "git -C $W/server branch -r --merged"
allow "git -C $W/server branch -avv --show-current"
allow "git -C $W/server config --get core.bare"
allow "git -C $W/server config core.bare"
allow "git -C $W/server symbolic-ref HEAD"
allow "git -C $W/server tag"
allow "git -C $W/server tag -l 'v*'"
allow "git -C $W/server tag --list 'v*'"
allow "git -C $W/server reflog"
allow "git -C $W/server reflog show HEAD"
for sub in ls-remote show-ref diff-tree whatchanged count-objects check-ignore check-attr var version help rev-parse; do
  allow "git -C $W/server $sub"
done
allow "git -C $W/server remote -v; git -C $W/server worktree list; git -C $W/server stash list"
allow "cd $W/server && sed -n '1p' note && awk '{print}' note"
allow "sed -E -e 's/-i/yes/' $W/server/note"
allow "sed -nei.bak $W/server/note"
allow "rsync $W/server/note $P/.cache/"
allow "find $W/server -name note; fd . $W/nix"
allow "tar -tf $W/server/logs.tar"
allow "tar -czf $P/.cache/logs.tar -C $W/server ."
allow "tar cf $P/.cache/logs.tar -C $W/server ."
allow "flock $fixture/scratch/lock cat $W/server/note"
allow "cd $W/server && tee $P/.cache/note"
allow "cd $W/server && cd $P && git log -1"
allow "cd $W/server && cd $P && git fetch -q origin main"
allow "pushd $W/server && popd && git -C $P fetch -q origin main"
for reader in diff cmp comm; do
  allow "$reader $W/server/f $W/studio/f"
done
for reader in dust tokei; do
  allow "$reader $W/server"
done
for reader in more hexdump; do
  allow "$reader $W/server/f"
done
allow "yq . $W/server/f"
allow "ag pattern $W/server"
allow "gh issue comment 5 -F $W/server/.cache/gate.log"
allow "tar -xf $W/server/x.tar -C $P/.cache/harness"
allow "source $W/server/.cache/x.env"
allow ". $W/server/.cache/x.env"
allow "git -C $W/server fetch --dry-run"
allow "git -C $W/server diff -- --ext-diff"
allow "git -C $W/server diff -- --output"
allow "git -C $W/server grep -e --textconv"
# shellcheck disable=SC2016
allow "cd $W/server && "'while read -r l; do echo $l; done < .cache/run.history'
allow "wc -l < $W/server/f"
allow "cat <<< '$W/server/f'"
allow "[ -f $W/server/x ] && cat $W/server/x"
allow "find $W/server -exec cat {} +"
allow "find $W/server -exec wc -l {} +"
allow "find $W/server -exec tail -n 1 {} \;"
allow "find $W/server -name -delete"
allow "fd -t f . $W/server/.cache -x cat"
allow "fd -t f . $W/server/.cache -X ls -l"
allow "find $W/server | xargs wc -l"
allow "parallel cat ::: $W/server/f"
allow "nix eval $W/server#x"
allow "xxd $W/server/f"
allow "sort -- $W/server/-o"
allow "sed -n 's/a/w $W/server/' x"
for copier in cp mv ln install rsync; do
  allow "$copier $W/server/f $P/.cache/f"
done
allow "find $W/server -exec cp {} $P/.cache/ \;"
allow "cd $W/server && some-program f"
allow '=(echo a)'
allow 'ls **/*(.)'
# shellcheck disable=SC2016
allow 'echo ${(f)var}'
allow '[ a \> b ]'
# shellcheck disable=SC2016
allow 'echo ${(f)var}; echo pseudouser pushd'
allow "command -v sudo; echo 'sudo' | cat"
allow "(cd $W/server && ls); touch $fixture/scratch/ok"
allow "cd $W/server | touch $fixture/scratch/ok"
allow "cd $W/server & cargo build"
allow "cd $W/server && ls & cargo build"
allow "$W/server/note" Read
allow 'sudo true' Grep

deny lane-worktree "edits $W/server/apps/server/src/lib.rs" "$W/server/apps/server/src/lib.rs" Edit
deny lane-worktree "edits $W/studio/note" "$W/studio/note" Write
deny lane-worktree "edits $W/nix/note.ipynb" "$W/nix/note.ipynb" NotebookEdit
for tool in Edit Write; do
  deny lane-worktree "edits $W/server/new/note" "$fixture/scratch/lane-link/new/note" "$tool"
  deny lane-worktree "edits $W/sErVeR/new/note" "$W/sErVeR/new/note" "$tool"
done
deny lane-worktree "edits $W/ünicode/note" "$W/ünicode/note" Edit
for redirect in '>' '>>' '&>' '>|' '2>' '&>>' '>&'; do
  deny lane-worktree "redirects to $W/server/.cache/note" "echo x $redirect $W/server/.cache/note"
done
for writer in rm rmdir touch mkdir tee; do
  deny lane-worktree "writes $W/server/note" "$writer $W/server/note"
done
deny lane-worktree "writes $W/server/note" "mv $fixture/scratch/note $W/server/note"
deny lane-worktree "writes $W/server/note" "ln -s $fixture/scratch/note $W/server/note"
deny lane-worktree "writes $W/server/note" "chmod 644 $W/server/note"
deny lane-worktree "writes $W/server/note" "chown someone $W/server/note"
deny lane-worktree "writes $W/server/note" "truncate -s 0 $W/server/note"
deny lane-worktree "writes $W/server/note" "sd a b $W/server/note"
deny lane-worktree "writes $W/server/note" "install -m 644 $fixture/scratch/note $W/server/note"
deny lane-worktree "writes $W/server/note" "patch $W/server/note < $fixture/scratch/patch"
deny lane-worktree "writes $W/server/note" "dd if=/dev/zero of=$W/server/note"
deny lane-worktree "writes $W/server/note" "cp $fixture/scratch/note $W/server/note"
deny lane-worktree "writes $W/nix/note" "rsync $fixture/scratch/note $W/nix/note"
for editor in sed perl ruby; do
  deny lane-worktree "writes $W/studio/note" "$editor -i.bak 's/a/b/' $W/studio/note"
done
for editor in 'sed --in-place' 'sed --in-place=.bak' 'sed -Ei' 'sed -ni' 'perl -pi' 'perl -0pi -e' 'ruby -pi'; do
  deny lane-worktree "writes $W/studio/note" "$editor 's/a/b/' $W/studio/note"
done
deny lane-worktree "writes $W/studio/note" "awk -i inplace '{print}' $W/studio/note"
deny lane-worktree "writes $W/studio/note" "awk --include=inplace '{print}' $W/studio/note"
deny own-checkout "writes $P/AGENTS.md" "sed -Ei 's/a/b/' $P/AGENTS.md"
deny lane-worktree "runs tar with $W/server" "tar -x -C $W/server"
deny lane-worktree "runs tar with $W/server" "tar -xf /tmp/archive -Icat -C $W/server"
deny lane-worktree "runs tar with $W/server/archive.tar" "tar -cf $W/server/archive.tar $fixture/scratch/note"
deny lane-worktree "runs tar with $W/server/archive.tar" "tar -cf$W/server/archive.tar $fixture/scratch/note"
deny lane-worktree "runs unzip with $W/server" "unzip -d $W/server archive.zip"
deny lane-worktree "runs curl with $W/server/f" "curl -o $W/server/f https://example.invalid"
deny lane-worktree "runs wget with $W/server/f" "wget -O $W/server/f https://example.invalid"
deny lane-worktree "runs just with $W/server/justfile" "just --justfile $W/server/justfile"
deny lane-worktree "runs just with $W/server" "just --working-directory $W/server"
deny lane-worktree "runs just with $W/server" "just --working-directory=$W/server"
deny lane-worktree "runs nix with $W/server" "nix build $W/server#x"
deny lane-worktree "runs claude with $W/server" "claude --add-dir $W/server"
deny lane-worktree "runs flock with $W/server/lock" "flock $W/server/lock true"
deny lane-worktree "runs script with $W/server/transcript" "env script $W/server/transcript true"
deny lane-worktree "runs git log in $W/server" "git -C $W/server log --output=x"
deny lane-worktree "runs git log in $W/server" "git -C $W/server log --output $fixture/scratch/log"
deny lane-worktree "runs find with $W/server" "find $W/server -delete"
deny lane-worktree "runs find with $W/server" "find $W/server -exec rm {} +"
for flag in -execdir -ok -okdir; do
  deny lane-worktree "runs find with $W/server" "find $W/server $flag rm {} \;"
done
for flag in -fprint -fprint0 -fprintf; do
  deny lane-worktree "runs find with $W/server" "find $W/server $flag $fixture/scratch/log"
done
for flag in -x -X --exec --exec-batch; do
  deny lane-worktree "runs fd with $W/server" "fd . $W/server $flag rm"
done
deny lane-worktree "runs git commit in $W/studio" "git -C $W/studio commit -m x"
deny lane-worktree "runs git stash in $W/nix" "git -C $W/nix stash"
deny lane-worktree "runs git add in $W/nix/.git" "git --git-dir=$W/nix/.git add --all"
lane_git_dir=$(git -C "$W/studio" rev-parse --absolute-git-dir)
deny lane-worktree "runs git checkout in $lane_git_dir" "git --git-dir='$lane_git_dir' checkout feat/x"
deny lane-worktree "runs git reset in $W/server" "git --work-tree '$W/server' reset"
deny lane-worktree "runs git branch in $W/server" "git -C $W/server branch feat/new"
deny lane-worktree "runs git config in $W/server" "git -C $W/server config core.bare true"
deny lane-worktree "runs git symbolic-ref in $W/server" "git -C $W/server symbolic-ref HEAD refs/heads/main"
deny lane-worktree "runs git tag in $W/server" "git -C $W/server tag v1"
deny lane-worktree "runs git reflog in $W/server" "git -C $W/server reflog expire"
deny lane-worktree "runs git checkout in $W/server" "cd $W/server; git checkout -- ."
deny lane-worktree "runs cargo in $W/server" "cd $W/server && cargo build"
deny lane-worktree "runs cargo in $W/server" "cd $W/server && cargo build & ls"
deny lane-worktree "runs make in $W/studio" "pushd $W/studio; make"
call_cwd="$W/nix" deny lane-worktree "runs cargo in $W/nix" 'cargo build'
deny lane-worktree "runs cargo with $W/studio/Cargo.toml" "cargo check --manifest-path=$W/studio/Cargo.toml"
deny lane-worktree "runs nix with $W/nix/flake.nix" "nix build -f $W/nix/flake.nix"
deny lane-worktree "runs just with $W/studio/justfile" "just -f $W/studio/justfile"
deny lane-worktree "runs make with $W/server" "make -C $W/server"
deny lane-worktree "runs make with $W/server" "make -C$W/server"
deny lane-worktree "runs codex with $W/studio" "codex exec -C $W/studio 'text'"
deny lane-worktree "runs claude with $W/studio" "claude -C $W/studio"
deny lane-worktree "runs bash with $W/server/build.sh" "bash $W/server/build.sh"
deny lane-worktree "runs sh with $W/nix/build.sh" "sh $W/nix/build.sh"
deny lane-worktree "runs zsh with $W/nix/build.sh" "zsh -e $W/nix/build.sh other"
for args in "-o $W/server/f x" "--output=$W/server/f x" "x -o $W/server/f"; do
  deny lane-worktree "writes $W/server/f" "sort $args"
done
deny lane-worktree "writes $W/server/f" "uniq x $W/server/f"
deny lane-worktree "writes $W/server/f" "sed -n 'w $W/server/f' x"
deny lane-worktree "writes $W/server/f" "awk '{print > \"$W/server/f\"}' x"
deny lane-worktree "writes $W/server/f" "cd $W/server && sort -o f g"
deny lane-worktree "writes $W/server/f" "cd $W/server && sed -n 'w f' g"
deny lane-worktree "writes $W/server/f" "cd $W/server && awk '{print > \"f\"}' g"
deny lane-worktree "writes $W/server/f" "xxd -r x $W/server/f"
deny lane-worktree "runs tar with $W/server" "tar --remove-files -cf x.tar $W/server"
deny lane-worktree "runs find with $W/server" "find $W/server -fls x"
deny lane-worktree "runs git log in $W/server" "git -C $W/server -c core.pager=touch log"
deny lane-worktree "runs git log in $W/server" "git -C $W/server -c alias.log=!rm log"
deny lane-worktree "runs git log in $W/server" "git -C $W/server log --ext-diff"
deny lane-worktree "runs git grep in $W/server" "git -C $W/server grep -O"
deny lane-worktree "runs git log in $W/server" "git -C $W/server log --textconv"
deny lane-worktree "runs git grep in $W/server" "git -C $W/server grep --open-files-in-pager=cat x"
deny lane-worktree "runs git ls-remote in $W/server" "git -C $W/server ls-remote --upload-pack=other origin"
deny lane-worktree "runs git fetch in $W/server" "git -C $W/server fetch --dry-run --exec=other origin"
deny lane-worktree "runs git fetch in $W/server" "git -C $W/server fetch"
deny lane-worktree "runs fd with $W/server" "fd . $W/server -xrm"
deny lane-worktree "runs fd with $W/server" "fd . $W/server -x rm"
deny lane-worktree "runs find with $W/server" "find $W/server -exec sed -i s/a/b/ {} +"
deny lane-worktree "runs xargs with $W/server" "find $W/server | xargs rm"
deny lane-worktree "runs xargs in $W/server" "cd $W/server && xargs rm"
deny lane-worktree "runs xargs with $W/server/list" "xargs -a $W/server/list rm"
deny lane-worktree "runs parallel with $W/server/f" "parallel rm ::: $W/server/f"
deny lane-worktree "writes $W/server/f/{}" "parallel 'rm {}' ::: $W/server/f"
deny lane-worktree "writes $W/server/f" "gawk -i inplace 1 $W/server/f"
deny lane-worktree "writes $W/server/f" "gawk -iinplace 1 $W/server/f"
deny lane-worktree "writes $W/server/f" "gsed -i s/a/b/ $W/server/f"
deny lane-worktree "writes $W/server/f" "sed -i.file -e s/a/b/ $W/server/f"
deny lane-worktree "writes $W/server/f" "sed -n -e '1,3w $W/server/f' x"
deny lane-worktree "writes $W/server" "cd $W/server && awk '{print | cmd}' g"
deny lane-worktree "writes $W/server" "git apply --directory=$W/server $P/.cache/change.patch"
deny lane-worktree "writes $W/server" "patch -d$W/server -i $P/.cache/change.patch"
deny lane-worktree "writes $W/server" "cp --target-directory=$W/server $P/.cache/f"
deny lane-worktree "writes $W/server" "mv -t $W/server $P/.cache/f"
deny lane-worktree "runs wget with $W/server" "wget -P $W/server https://example.invalid"
deny lane-worktree "runs curl in $W/server" "cd $W/server && curl -O https://example.invalid"
deny lane-worktree "runs tar in $W/server" "cd $W/server && tar xf x.tar"
deny lane-worktree "runs npm with $W/server" "npm --cwd $W/server run test"
deny lane-worktree "runs npm in $W/server" "cd $W/server && npm --version >/dev/null"
deny lane-worktree "runs pnpm with $W/server" "pnpm -C $W/server test"
deny lane-worktree "runs make with $W/server/Makefile" "make --file $W/server/Makefile"
deny lane-worktree "runs nix with $W/server" "nix flake check $W/server#x"
deny lane-worktree "runs nix with $W/server" "nix develop $W/server#x -c cat"
deny lane-worktree "runs nix with $W/server" "nix shell $W/server#x --command cat"
# shellcheck disable=SC2016
deny lane-worktree "writes $W/server/.cache/run.pid" "W=$W;"' rm $W/server/.cache/run.pid'
# shellcheck disable=SC2016
deny lane-worktree "writes $W/studio/note" "export W='$W'; rm "'"${W}/studio/note"'
# shellcheck disable=SC2016
deny lane-worktree "writes $W/server/x" "for l in server nix; do rm $W/"'$l/x; done'
# shellcheck disable=SC2016
deny lane-worktree "writes $W/nix/x" "for l in missing nix; do rm $W/"'$l/x; done'
# shellcheck disable=SC2016
deny lane-worktree "writes $W" "rm $W/"'$unknown/x'
# shellcheck disable=SC2016
deny lane-worktree "writes $W/server" "rm $W/server/"'$unknown/x'
deny lane-worktree "writes $W/server/x" "rm $W/{server,nix}/x"
deny lane-worktree "writes $W/server/x" "rm $W/{s{erver,tudio},nix}/x"
deny lane-worktree "writes $W/server/x" "rm $W/{o..s}erver/x"
deny lane-worktree "writes $W" "rm -rf $W/*"
deny lane-worktree "writes $W/s" "rm -rf $W/s*"
deny lane-worktree "writes $W" "rm -rf $W/*/.cache"
deny lane-worktree "writes $W/serv" "rm -rf $W/serv?r/x"
deny lane-worktree "writes $W" "rm -rf $W/[sn]*/x"
allow "rm '$W/{server,nix}/x'"
allow "rm $W/nonlane*"
deny lane-worktree "writes $W/nix/note" "rm $W/server/../nix/./note"
deny lane-worktree "writes $W/nix/note with spaces" "rm '$W/nix/note with spaces'"
call_cwd="$W" deny lane-worktree "edits $W/nix/a" 'studio/../nix/a' Edit
deny lane-worktree "writes $W/nix/note" "rm >/dev/null $W/nix/note"
deny lane-worktree "redirects to $W/nix/note" "cat <<EOF > $W/nix/note"$'\nsudo true\nEOF'
deny lane-worktree "writes $W/studio" "git -C $P worktree remove $W/studio"
deny lane-worktree "writes $W/studio" "git -C $clone worktree add -b feat/y $W/studio"

deny own-checkout "edits $P/AGENTS.md" "$P/AGENTS.md" Write
deny own-checkout "edits $P/AGENTS.md" 'AGENTS.md' Edit
deny own-checkout "redirects to $P/AGENTS.md" 'echo x > ./AGENTS.md'
deny own-checkout "writes $P/AGENTS.md" "sed -i s/a/b/ $P/AGENTS.md"
deny own-checkout "writes $P/AGENTS.md" "touch $P/.cache/../AGENTS.md"
deny own-checkout "writes $P/-note" 'rm -- -note'
deny own-checkout "runs git commit in $P" "git -C $P commit -m x"
deny own-checkout "runs git checkout in $P" "git -C $P checkout feat/x"
deny own-checkout "runs git merge in $P" 'git merge origin/main'
deny own-checkout "runs git pull in $P" 'git pull --ff'
deny own-checkout "runs git checkout in $P/.cache" 'cd .cache; git checkout feat/x'
deny own-checkout "writes $P/x" "git clone -q $P x"
deny own-checkout "writes $P/owner" "git clone -q $P"
deny own-checkout "writes $P/AGENTS.md" "sort --output=$P/AGENTS.md x"
deny own-checkout "writes $P/AGENTS.md" "sed -n 'w $P/AGENTS.md' x"
deny own-checkout "writes $P/AGENTS.md" "awk '{print > \"$P/AGENTS.md\"}' x"

for ref in main HEAD:main x:main refs/heads/main x:refs/heads/main :main; do
  deny main-branch 'pushes to main' "git push origin $ref"
done
deny main-branch 'pushes to main' 'git push'
deny main-branch 'pushes to main' "git -C $clone push origin"
deny main-branch 'pushes to main' "git -C $clone push origin HEAD"
call_cwd="$clone" deny main-branch 'pushes to main' 'git push -u origin HEAD'
call_cwd="$clone" deny main-branch 'pushes to main' 'git push --follow-tags origin HEAD'
deny main-branch 'pushes to main' "cd $clone && git push origin HEAD"
for ref in @ '@{push}' '@{upstream}' '@{u}'; do
  call_cwd="$clone" deny main-branch 'pushes to main' "git push origin $ref"
  call_cwd="$work" allow "git push origin $ref"
done
call_cwd="$work" deny main-branch 'pushes to main' 'git push origin refs/heads/*:refs/heads/*'
for option in --all --branches; do
  call_cwd="$work" deny main-branch 'pushes to main' "git push $option origin"
done
call_cwd="$fixture/scratch" deny main-branch "can not read the current branch in $fixture/scratch" 'git push origin HEAD'
allow "git -C $work push origin HEAD"
allow "git -C $clone push origin HEAD:feat/x"
deny main-branch 'pushes to main' "git -C $work push --repo=origin HEAD:main"
HARNESS_MAIN=trunk deny main-branch 'pushes to trunk' "git -C $work push origin HEAD:trunk"
allow "git -C $work push origin"
allow 'git push --tags'
allow 'git push origin --tags'
allow "git -C $clone push origin --tags"
deny main-branch 'pushes to main' 'git push origin --tags HEAD:main'
call_cwd="$work" deny main-branch 'sets a push refspec with git -c' 'git -c remote.origin.push=HEAD:main push origin feat/x'
deny main-branch 'uses a force or mirror push' 'git push --force origin --tags'
runtime_branch='the branch of this push is known only at run time; name the branch'
# shellcheck disable=SC2016
deny main-branch "$runtime_branch" 'git push origin $(git branch --show-current)'
# shellcheck disable=SC2016
deny main-branch "$runtime_branch" 'git push origin $B'
# shellcheck disable=SC2016
deny main-branch "$runtime_branch" 'git push origin "$B"'
# shellcheck disable=SC2016
deny main-branch "$runtime_branch" 'git push origin "HEAD:$(git branch --show-current)"'
# shellcheck disable=SC2016
deny main-branch "$runtime_branch" 'git push origin `git branch --show-current`'
# shellcheck disable=SC2016
call_cwd="$work" allow 'B=feat/x; git push origin $B'
# shellcheck disable=SC2016
call_cwd="$work" allow "git push origin 'feat/literal"'$B'"'"
# shellcheck disable=SC2016
call_cwd="$work" allow 'git push origin "feat/literal\$B"'
# shellcheck disable=SC2016
deny main-branch 'pushes to main' 'B=main; git push origin $B'
for force in --force -f --force-with-lease --force-with-lease=feat/x --mirror -uf; do
  deny main-branch 'uses a force or mirror push' "git -C $clone push $force origin feat/x"
done
deny main-branch 'uses a force or mirror push' 'git push origin +feat/x'
deny main-branch 'runs gh pr merge --admin' 'gh pr merge 7 --squash --admin'
deny main-branch 'runs gh pr review --approve' 'gh pr review 7 --approve'
deny main-branch 'runs gh pr review --approve' 'gh pr review 7 -a'
for method in PUT POST PATCH DELETE; do
  deny main-branch 'writes to repos/o/r/branches/main/protection' \
    "gh api repos/o/r/branches/main/protection -X $method"
done
for field in '-f why=x' '-F count=1' '--field count=1' '--raw-field why=x' '--input rules.json'; do
  deny main-branch 'writes to repos/o/r/rulesets' "gh api repos/o/r/rulesets $field"
done
deny main-branch 'writes to repos/o/r/branches/topic/about' 'gh api --method=PATCH repos/o/r/branches/topic/about'
for sub in delete rename archive edit; do
  deny main-branch "runs gh repo $sub" "gh repo $sub repo"
done

for name in sudo doas shutdown reboot halt nixswitch; do
  deny machine "runs $name" "$name"
done
deny machine 'runs sudo' 'bash -c "sudo true"'
deny machine 'runs sudo' "\$'su\\x64o' true"
deny machine 'runs sudo' "sh -ec 'sudo true'"
deny machine 'runs sudo' "zsh -c 'eval sudo true'"
deny machine 'runs sudo' "eval '/usr/bin/sudo true'"
deny machine 'runs sudo' 'env A=1 sudo true'
deny machine 'runs sudo' 'env - sudo true'
deny machine 'runs sudo' 'env -u A A="quoted value" /usr/bin/sudo true'
deny machine 'runs sudo' 'xargs sudo rm < list'
deny machine 'runs sudo' 'xargs -n 1 -I % sudo rm % < list'
deny machine 'runs sudo' 'nohup sudo true &'
deny machine 'runs sudo' 'time -p sudo true'
deny machine 'runs sudo' 'command -- sudo true'
deny machine 'runs sudo' 'exec -a new-name sudo true'
deny machine 'runs sudo' 'caffeinate -t 2 sudo true'
deny machine 'runs sudo' 'nice -n 5 sudo true'
deny machine 'runs sudo' 'timeout -s TERM 2s sudo true'
deny machine 'runs sudo' "direnv exec $fixture/scratch sudo true"
deny machine 'runs sudo' 'nix develop .#server --command sudo true'
deny machine 'runs sudo' 'nix shell nixpkgs#coreutils -c sudo true'
deny machine 'runs sudo' 'stdbuf -o0 sudo true'
deny machine 'runs sudo' 'watch sudo true'
deny machine 'runs sudo' "watch 'sudo true'"
deny machine 'runs sudo' "parallel 'sudo true' ::: 1"
deny machine 'runs sudo' 'find . -exec sudo rm {} +'
deny machine 'runs sudo' 'fd -x sudo'
deny machine 'runs sudo' 'ssh host -- sudo x'
deny machine 'runs remote pkill' 'ssh host -- pkill -f codex'
for wrapper in 'stdbuf -o 0' 'watch -n 1' 'script /dev/null' 'script -c' 'setsid' 'ionice -c 3' 'chrt 1' 'parallel -j 2' 'flock /tmp/lock' 'flock /tmp/lock -c' 'unbuffer'; do
  deny machine 'runs shutdown' "$wrapper shutdown"
done
for flag in -exec -execdir -ok; do
  deny machine 'runs shutdown' "find . $flag shutdown {} \;"
done
deny machine 'runs shutdown' 'fd -X shutdown'
deny machine 'runs sudo' 'some-command sudo true'
deny machine 'runs doas' 'some-command doas true'
allow 'some-command sudo'
allow 'printf "%s\n" sudo doas true'
deny machine 'runs sudo' 'true && sudo true'
deny machine 'runs sudo' 'true; (sudo true)'
# shellcheck disable=SC2016
deny machine 'runs sudo' '$(sudo true)'
deny machine 'runs sudo' "ssh host 'sudo reboot'"
deny machine 'runs sudo' "ssh -o ConnectTimeout=5 host sudo reboot"
deny machine 'runs remote pkill' 'ssh host "pkill -f codex"'
deny machine 'runs remote kill' 'ssh host kill 1'
deny machine 'runs remote killall' 'ssh host killall codex'
for name in darwin-rebuild nixos-rebuild; do
  for sub in switch activate boot test; do
    deny machine "runs $name $sub" "$name $sub"
  done
done
for option in -i -iA --install -e --uninstall; do
  deny machine "runs nix-env $option" "nix-env $option pkg"
done
for sub in install remove upgrade; do
  deny machine "runs nix profile $sub" "nix profile $sub pkg"
done
deny machine 'runs nix profile remove' 'nix profile --profile /tmp/profile remove pkg'
deny machine 'runs nix profile remove' "nix --option experimental-features 'nix-command flakes' profile remove pkg"
for name in brew pip pip3 cargo; do
  deny machine "runs $name install" "$name install pkg"
done
deny machine 'runs npm install' 'npm -g install pkg'
deny machine 'runs pnpm install' 'pnpm install --global pkg'
deny machine 'runs cargo install' 'cargo +nightly install pkg'
deny machine 'runs pip install' 'pip --python /usr/bin/python install pkg'
deny machine 'runs launchctl bootstrap' 'launchctl bootstrap gui/1 file'
deny machine 'runs systemctl restart' 'systemctl --user restart service'
for sub in switch login logout refresh setup-git; do
  deny machine "runs gh auth $sub" "gh auth $sub"
done
for group in secret variable; do
  allow "gh $group list"
  for sub in set delete remove; do
    deny machine "runs gh $group $sub" "gh $group $sub example"
  done
done
for endpoint in dispatches actions/secrets actions/variables; do
  allow "gh api repos/o/r/$endpoint -X GET"
  deny machine "writes to repos/o/r/$endpoint" "gh api repos/o/r/$endpoint -X POST"
done

deny deploy 'runs terraform apply' 'terraform -chdir=infra apply -auto-approve'
for sub in destroy import taint untaint 'state rm' 'state mv' 'state push' 'workspace new' 'workspace delete'; do
  deny deploy "runs tofu $sub" "tofu $sub resource"
done
for name in gcloud kubectl helm; do
  deny deploy "runs $name" "$name"
done
deny deploy 'runs docker push' 'docker --context test push image'
deny deploy 'runs podman login' 'podman login registry'
deny deploy 'runs gh workflow run deploy.yml' 'gh workflow run deploy.yml'
HARNESS_OWNER_WORKFLOWS='checks.yml test.yml' allow 'gh workflow run test.yml'
for sub in create delete edit; do
  deny deploy "runs gh release $sub" "gh release $sub v1"
done
wait_cases

input_error() {
  local output=$1 cause=$2
  check_reason "owner guard: can not read this call ($cause); start the session from the Nix shell of the repository" "$cause"
}

for payload in '{bad json' '{}'; do
  output=$(expect_success "$BASH" "$guard" <<< "$payload")
  input_error "$output" 'invalid JSON or tool input'
done
for command in 'if true; then' 'cat <<EOF'; do
  output=$(call Bash "$command")
  input_error "$output" 'Bash syntax error'
done
# shellcheck disable=SC2016
allow "stat -f %m $W/server/run.pid | "'xargs -I{} echo $(( $(date +%s) - {} ))'
# shellcheck disable=SC2016
fallback='echo ${(f)var};'
allow "$fallback ls /private/tmp/claude-501/x"
allow "$fallback ls /private/tmp/claude-501/x $W/server/note"
allow "$fallback ls /private/tmp/sudo-501/x $W/server/note"
allow "$fallback ls /private/tmp/sort-501/x $W/server/note"
allow "$fallback ls $W/server"
allow "$fallback ls $W/server/note"
allow "$fallback W=$W/server/note"
allow "$fallback ls $W"
allow "$fallback cat ../lanes/server/note"
allow "$fallback sorter $W/server/note"
allow "$fallback sort -o /tmp/f x"
allow "$fallback cat $W/server/note 2>&1"
allow "$fallback cat $W/server/note >&2"
allow "$fallback cat $W/server/note 2>/dev/null"
zsh_prefix='=(echo a)'
allow "$zsh_prefix; cat /private/tmp/claude-501/x"
allow "$zsh_prefix; ls /x/sudo-notes"
allow "$zsh_prefix; cat docs/push.md"
allow "$zsh_prefix; cat $W/server/f"
allow "$zsh_prefix; cat claude_notes $W/server/f"
allow "$zsh_prefix; git log -1"
# shellcheck disable=SC2016
allow "$zsh_prefix; stat -f %m $W/server/run.pid | "'xargs -I{} echo $(( $(date +%s) - {} ))'
wait_cases
while IFS='|' read -r risk command; do
  output=$(call Bash "$command")
  check_reason "owner guard: can not read this call (the Bash grammar does not parse it, and it holds $risk). Write it in plain Bash." "$command"
  denied=$((denied + 1))
done <<CASES
sudo|$fallback sudo true
git push|$fallback git push origin main
gh pr merge|$fallback gh pr merge 7
sort|$fallback sort -o $W/server/f x
just|$fallback just --justfile=$W/server/justfile
claude|$fallback claude --add-dir $W/server
>|$fallback echo a > $W/server/note
>|$fallback cat $W/server/note 2>/dev/null.log
>|$fallback echo a > /tmp/file
push|$zsh_prefix; git -C $clone push origin main
push|$zsh_prefix && git -C $clone push origin HEAD
git push|$zsh_prefix; time git push origin main
git push|$zsh_prefix; env git push origin main
sudo|$zsh_prefix; env sudo true
sudo|$zsh_prefix; nohup sudo true
sudo|$zsh_prefix; xargs sudo rm
sudo|$zsh_prefix; command sudo true
sudo|$zsh_prefix; exec sudo true
sudo|$zsh_prefix; time sudo true
sudo|$zsh_prefix; stdbuf -o0 sudo true
sudo|$zsh_prefix; FOO=1 sudo true
sudo|$zsh_prefix; { sudo true; }
sudo|$zsh_prefix; if true; then sudo true; fi
sudo|$zsh_prefix; for i in 1; do sudo true; done
sudo|$zsh_prefix; ! sudo true
sudo|$zsh_prefix; echo \`sudo true\`
sudo|$zsh_prefix; bash -c "sudo true"
rm|$zsh_prefix; env rm $W/server/x
claude|$zsh_prefix; nohup claude --add-dir $W/server
CASES
mkdir "$fixture/bin"
output=$(PATH="$fixture/bin" expect_success "$BASH" "$guard" <<< '{}')
input_error "$output" 'jq is missing'
output=$(PATH="$fixture/bin" expect_success "$BASH" "$guard" --agy <<< '{}')
ln -s "$(command -v jq)" "$fixture/bin/jq"
agy_no_jq=$output
fixture_call=$(jq -cn --arg cwd "$P" '{tool_name:"Bash",cwd:$cwd,tool_input:{command:"true"}}')
output=$(PATH="$fixture/bin" expect_success "$BASH" "$guard" <<< "$fixture_call")
input_error "$output" 'ast-grep is missing'

jq -es '
  . == [{
    hooks: {PreToolUse: [{
      matcher:"Bash|Edit|Write|NotebookEdit",
      hooks:[{type:"command",command:"bash \"${CLAUDE_PROJECT_DIR}/architecture/owner/guard.sh\""}]
    }]},
    permissions: {deny:[
      "Bash(sudo:*)", "Bash(gh auth switch:*)", "Bash(gh auth logout:*)",
      "Bash(terraform apply:*)", "Bash(terraform destroy:*)"
    ]}
  }]
' "$architecture/owner/settings.json" >/dev/null
printf 'owner-guard-test: %s allowed and %s denied cases, input errors and the settings file pass\n' "$allowed" "$denied"

agy_payload() {
  jq -cn --arg cwd "$P" --arg lane "$W/server" --arg scratch "$fixture/scratch" \
    --arg name "${2:-run_command}" '
    {cwd:$cwd, hookEventName:"PreToolUse", workspacePaths:[$cwd],
     toolCall:{name:$name, args:{CommandLine:"true"}}} | '"$1"
}

agy_expect() {
  local label=$1 reason=$2 result=$3 expected
  if [ -n "$reason" ]; then
    expected=$(jq -cn --arg reason "$reason" '{decision:"deny",reason:$reason}')
  else
    expected='{"decision":"allow"}'
  fi
  if [ "$result" != "$expected" ]; then
    printf 'owner-guard-test: %s\n  expected: %s\n  received: %s\n' \
      "$label" "$expected" "${result:-<no output>}" >&2
    return 1
  fi
}

agy_case() {
  local label=$1 reason=$2 payload=$3 output
  shift 3
  output=$(expect_success "$BASH" "$guard" "$@" <<< "$payload") || return
  agy_expect "$label" "$reason" "$output"
}

agy_unreadable() {
  printf 'owner guard: can not read this call (%s); start the session from the Nix shell of the repository' "$1"
}

machine_deny='owner guard: machine: runs sudo. Ask Joel in one sentence with the cost and the choices.'
sudo_call=$(agy_payload '.toolCall.args.CommandLine = "sudo true"')
agy_case 'run_command allows a read' '' "$(agy_payload '.toolCall.args.CommandLine = "git status"')"
agy_case 'run_command denies sudo' "$machine_deny" "$sudo_call"
agy_case 'toolCall takes priority over tool_name' "$machine_deny" \
  "$(jq -c '.tool_name = "Read"' <<< "$sudo_call")"
agy_case 'run_command uses Cwd first' "owner guard: lane-worktree: runs git add in $W/server. Read it; write a card for that lane." \
  "$(agy_payload '.toolCall.args = {Cwd:$lane, CommandLine:"git add note"}')"
agy_case 'run_command uses workspacePaths when cwd is absent' "owner guard: lane-worktree: runs git add in $W/server. Read it; write a card for that lane." \
  "$(agy_payload 'del(.cwd) | .workspacePaths = [$lane] | .toolCall.args.CommandLine = "git add note"')"
agy_case 'run_command uses cwd before workspacePaths' '' \
  "$(agy_payload '.cwd = $scratch | .workspacePaths = [$lane] | .toolCall.args.CommandLine = "touch note"')"
agy_case 'run_command resolves a symlinked cwd' "owner guard: lane-worktree: runs git add in $W/server. Read it; write a card for that lane." \
  "$(agy_payload '.toolCall.args = {Cwd:($scratch + "/lane-link"), CommandLine:"git add note"}')"
agy_case 'run_command checks an absolute cwd that does not exist' '' \
  "$(agy_payload '.toolCall.args = {Cwd:($scratch + "/absent"), CommandLine:"git status"}')"
for runner in agy antigravity; do
  agy_case "$runner cannot run in a lane" "owner guard: lane-worktree: runs $runner in $W/server. Read it; write a card for that lane." \
    "$(agy_payload '.toolCall.args = {Cwd:$lane, CommandLine:"'"$runner"' --version"}')"
done

for tool in write_file write_to_file edit_file replace_file_content multi_replace_file_content; do
  agy_case "$tool allows a scratch file" '' \
    "$(agy_payload '.cwd = $scratch | .workspacePaths = [$lane]
                    | .toolCall.args = {Path:"note",TargetFile:($lane + "/note"),Cwd:$lane}' "$tool")"
  agy_case "$tool denies a lane edit" "owner guard: lane-worktree: edits $W/server/note. Read it; write a card for that lane." \
    "$(agy_payload 'del(.cwd) | .workspacePaths = [$lane] | .toolCall.args = {TargetFile:"note"}' "$tool")"
done
agy_case 'notebook_edit allows a scratch notebook' '' \
  "$(agy_payload '.toolCall.args = {Path:($scratch + "/notes.ipynb")}' notebook_edit)"
agy_case 'notebook_edit denies a lane edit' "owner guard: lane-worktree: edits $W/server/notes.ipynb. Read it; write a card for that lane." \
  "$(agy_payload '.toolCall.args = {notebook_path:($lane + "/notes.ipynb")}' notebook_edit)"
agy_case 'notebook_edit checks TargetFile' "owner guard: lane-worktree: edits $W/server/notes.ipynb. Read it; write a card for that lane." \
  "$(agy_payload '.toolCall.args = {TargetFile:($lane + "/notes.ipynb")}' notebook_edit)"
interactive_deny='owner guard: interactive input to a background task is not allowed; run a direct command'
agy_case 'manage_task allows status' '' "$(agy_payload '.toolCall.args = {Action:"status"}' manage_task)"
agy_case 'manage_task denies send_input' "$interactive_deny" \
  "$(agy_payload '.toolCall.args = {Action:"send_input"}' manage_task)"
agy_case 'send_command_input is denied' "$interactive_deny" "$(agy_payload '.' send_command_input)"
agy_case 'an unknown tool is denied' 'owner guard: unknown Antigravity tool new_tool; add it to guard.sh' \
  "$(agy_payload '.' new_tool)"
for tool in read_file view_file list_dir find_by_name grep_search search_web read_url_content \
  schedule ask_question update_steps list_permissions ask_permission invoke_subagent define_subagent \
  manage_subagents send_message generate_image list_resources read_resource; do
  agy_case "$tool passes without fields" '' \
    "$(agy_payload 'del(.cwd, .workspacePaths, .toolCall.args)' "$tool")"
done
invalid=$(agy_unreadable 'invalid JSON or tool input')
agy_expect 'jq is missing' "$(agy_unreadable 'jq is missing')" "$agy_no_jq"
agy_case 'truncated JSON' "$invalid" '{"toolCall":' --agy
agy_case 'multiple JSON objects' "$invalid" "$sudo_call"$'\n'"$sudo_call" --agy
agy_case 'forced Antigravity mode rejects a Claude call' "$invalid" "$fixture_call" --agy
agy_case 'unexpected hook argument' "$(agy_unreadable 'unexpected arguments')" "$sudo_call" --agy unexpected
agy_case 'relative cwd' "$invalid" "$(agy_payload '.toolCall.args.Cwd = "relative"')"
agy_case 'cwd has a separator' "$invalid" "$(agy_payload '.cwd += "\u001f"')"
agy_case 'Cwd has a null byte' "$invalid" "$(agy_payload '.toolCall.args.Cwd = ($lane + "\u0000")')"
agy_case 'cwd has the wrong type' "$invalid" "$(agy_payload '.cwd = 3')"
agy_case 'tool name has the wrong type' "$invalid" "$(agy_payload '.toolCall.name = 3')"
agy_case 'tool call is null' "$invalid" "$(agy_payload '.toolCall = null')"
agy_case 'command is missing' "$invalid" "$(agy_payload 'del(.toolCall.args.CommandLine)')"
agy_case 'command has the wrong type' "$invalid" "$(agy_payload '.toolCall.args.CommandLine = true')"
agy_case 'command has a null byte' "$invalid" "$(agy_payload '.toolCall.args.CommandLine += "\u0000"')"
agy_case 'file path is missing' "$invalid" "$(agy_payload '.' edit_file)"
agy_case 'Bash syntax error' "$(agy_unreadable 'Bash syntax error')" \
  "$(agy_payload '.toolCall.args.CommandLine = "if true; then"')"
PATH="$fixture/bin" agy_case 'ast-grep is missing' "$(agy_unreadable 'ast-grep is missing')" "$sudo_call"

if ! hook=$(jq -er '
  .["owner-guard"].PreToolUse
  | select(length == 1)
  | .[0]
  | select(.matcher == "*")
  | .hooks | select(length == 1) | .[0]
  | select(.type == "command") | .command
' "$architecture/../.agents/hooks.json"); then
  echo 'owner-guard-test: expected one PreToolUse command with matcher *' >&2
  exit 1
fi
output=$(expect_success env -u HARNESS_OWNER_SESSION -u CLAUDE_PROJECT_DIR PATH="$fixture/bin/no-tools" \
  "$BASH" -c "$hook" <<< "$sudo_call")
agy_expect 'ordinary session passes without tools or project folder' '' "$output"
hook_failure='owner guard: hook execution failed'
mkdir "$fixture/hook-bin"
cat > "$fixture/hook-bin/bash" <<'STUB'
#!/bin/sh
echo '{"decision":"allow"}'
STUB
chmod +x "$fixture/hook-bin/bash"
env_bin=$(command -v env)
output=$(HARNESS_OWNER_SESSION=1 PATH="$fixture/hook-bin" \
  expect_success "$env_bin" -u CLAUDE_PROJECT_DIR "$BASH" -c "$hook" <<< "$sudo_call")
agy_expect 'owner hook denies a missing project folder' "$hook_failure" "$output"
output=$(HARNESS_OWNER_SESSION=1 CLAUDE_PROJECT_DIR='' PATH="$fixture/hook-bin" \
  expect_success "$BASH" -c "$hook" <<< "$sudo_call")
agy_expect 'owner hook denies an empty project folder' "$hook_failure" "$output"
output=$(HARNESS_OWNER_SESSION=1 PATH="$fixture/bin/no-tools" expect_success "$BASH" -c "$hook" <<< "$sudo_call")
agy_expect 'owner hook denies a missing bash' "$hook_failure" "$output"
output=$(HARNESS_OWNER_SESSION=1 expect_success "$BASH" -c "$hook" <<< "$sudo_call")
agy_expect 'owner hook denies a missing guard' "$hook_failure" "$output"
mkdir -p "$P/architecture/owner"
hook_guard=$P/architecture/owner/guard.sh
echo 'exit 0' > "$hook_guard"
output=$(HARNESS_OWNER_SESSION=1 expect_success "$BASH" -c "$hook" <<< "$sudo_call")
agy_expect 'owner hook denies an empty response' "$hook_failure" "$output"
printf '%s\n' 'echo unexpected output' 'exit 1' > "$hook_guard"
output=$(HARNESS_OWNER_SESSION=1 expect_success "$BASH" -c "$hook" <<< "$sudo_call")
agy_expect 'owner hook denies a failed guard' "$hook_failure" "$output"
rm "$hook_guard"
ln -s "$guard" "$hook_guard"
cp "$architecture/project.env" "$P/architecture/project.env"
output=$(HARNESS_OWNER_SESSION=1 expect_success "$BASH" -c "$hook" <<< "$sudo_call")
agy_expect 'owner session invokes the guard' "$machine_deny" "$output"
output=$(HARNESS_OWNER_SESSION=1 expect_success "$BASH" -c "$hook" <<< '{"toolCall":')
agy_expect 'owner hook selects Antigravity output for an early error' "$invalid" "$output"
output=$(HARNESS_OWNER_SESSION=1 expect_success "$BASH" -c "$hook" <<< "$(agy_payload '.' read_file)")
agy_expect 'owner hook delegates an allowed tool to session settings' '' "$output"

agent=$architecture/../.agents/agents/harness-owner.md
[ "$(head -n 1 "$agent")" = '---' ]
frontmatter=$(sed -n '1d; /^---$/q; p' "$agent")
for field in 'name: harness-owner' 'mainAgent: true' 'subagent: false' 'tools:' '  - run_command'; do
  if ! grep -Fxq -- "$field" <<< "$frontmatter"; then
    printf 'owner-guard-test: agent frontmatter is missing %s\n' "$field" >&2
    exit 1
  fi
done
jq -es '
  . == [{permissions:{
    allow:["command(*)", "read_file(*)", "write_file(*)", "edit_file(*)"],
    deny:["command(sudo)", "command(gh auth switch)", "command(gh auth logout)",
          "command(terraform apply)", "command(terraform destroy)"]
  }}]
' "$architecture/owner/agy-settings.json" >/dev/null
echo 'owner-guard-test: Antigravity calls, hook and settings pass'
