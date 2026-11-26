#!/usr/bin/env bash
# This hook checks direct calls and uses the rules in AGENTS.md (Lanes and Merge)
# and the harness-owner skill (What the owner decides, A harness step, Where state lives).
# The lists below define the contract for direct calls.
# A program outside the lists that writes to its argument is outside this hook and joins the list when found.
# The operating system sandbox is the wall.
set -euo pipefail
export LC_ALL=C CDPATH=
hook_format=claude

allow() {
  if [ "$hook_format" = agy ]; then printf '{"decision":"allow"}\n'; fi
  exit 0
}

unreadable() {
  # All callers pass fixed text so this also works when jq is missing.
  local reason="owner guard: can not read this call ($1); start the session from the Nix shell of the repository"
  if [ "$hook_format" = agy ]; then
    printf '{"decision":"deny","reason":"%s"}\n' "$reason"
  else
    printf '{"hookSpecificOutput":{"hookEventName":"PreToolUse","permissionDecision":"deny","permissionDecisionReason":"%s"}}\n' "$reason"
  fi
  exit 0
}

deny() {
  local action
  case "$1" in
    lane-worktree) action='Read it; write a card for that lane.' ;;
    own-checkout) action='Make the change as a step in a clone outside the checkout.' ;;
    main-branch) action='Land through the gate and the merge turn (architecture/land.sh).' ;;
    machine) action='Ask Joel in one sentence with the cost and the choices.' ;;
    deploy) action='Put it on the card with the label needs-joel.' ;;
    *) unreadable 'rule check failed' ;;
  esac
  deny_reason "owner guard: $1: $2. $action"
}

deny_reason() {
  jq -cn --arg reason "$1" --arg format "$hook_format" '
    if $format == "agy" then {decision: "deny", reason: $reason}
    else {hookSpecificOutput: {hookEventName: "PreToolUse", permissionDecision: "deny",
                               permissionDecisionReason: $reason}} end
  ' || unreadable 'jq failed'
  exit 0
}

if [ "${1:-}" = --agy ]; then
  hook_format=agy
  shift
fi
[ "$#" -eq 0 ] || unreadable 'unexpected arguments'
command -v jq >/dev/null || unreadable 'jq is missing'
# The separator is forbidden in cwd. The final dot preserves trailing newlines.
# Keep the format marker when an Antigravity field is invalid.
if ! call=$(jq -jers --arg format "$hook_format" '
  def fields($tool; $cwd; $input):
    if ($cwd | type != "string" or (startswith("/") | not) or contains("\u001f") or contains("\u0000"))
    then error("cwd")
    elif ($input | type != "string" or contains("\u0000"))
    then error("tool input")
    else [$tool, $cwd, $input + "."] | join("\u001f") end;
  def agy_call:
    .toolCall.name as $name
    | if ($name | type) != "string" then error("tool name")
      elif $name == "run_command" then
        fields("Bash"; .toolCall.args.Cwd // .cwd // .workspacePaths[0]; .toolCall.args.CommandLine)
      elif (["write_file", "write_to_file", "edit_file", "replace_file_content",
             "multi_replace_file_content"] | index($name)) != null then
        fields((if $name == "write_file" or $name == "write_to_file" then "Write" else "Edit" end);
               .cwd // .workspacePaths[0]; .toolCall.args.Path // .toolCall.args.TargetFile)
      elif $name == "notebook_edit" then
        fields("NotebookEdit"; .cwd // .workspacePaths[0];
               .toolCall.args.Path // .toolCall.args.TargetFile // .toolCall.args.notebook_path)
      elif $name == "manage_task" then
        if (.toolCall.args.Action // "") == "send_input" then "send_input" else "other" end
      elif $name == "send_command_input" then "send_input"
      elif (["read_file", "view_file", "list_dir", "find_by_name", "grep_search", "search_web",
             "read_url_content", "schedule", "ask_question", "update_steps", "list_permissions",
             "ask_permission", "invoke_subagent", "define_subagent", "manage_subagents",
             "send_message", "generate_image", "list_resources", "read_resource"] | index($name)) != null
      then "other"
      else "unknown:" + $name end;
  def claude_call:
    .tool_name as $tool
  | if $tool != "Bash" and $tool != "Edit" and $tool != "Write" and $tool != "NotebookEdit"
    then "other"
    else fields($tool; .cwd; if $tool == "Bash" then .tool_input.command
                            else .tool_input.file_path // .tool_input.notebook_path end)
    end;
  if length != 1 or (.[0] | type != "object") then error("tool call")
  else .[0]
    | if $format == "agy" or has("toolCall") then "agy\u001f" + (try agy_call catch "invalid")
      elif (.tool_name | type) == "string" then claude_call
      else error("tool name") end
  end' 2>/dev/null); then
  unreadable 'invalid JSON or tool input'
fi
case "$call" in
  agy$'\x1f'*)
    hook_format=agy
    call=${call#*$'\x1f'}
    ;;
esac
case "$call" in
  invalid) unreadable 'invalid JSON or tool input' ;;
  other) allow ;;
  send_input) deny_reason 'owner guard: interactive input to a background task is not allowed; run a direct command' ;;
  unknown:*)
    unknown_tool=${call#unknown:}
    deny_reason "owner guard: unknown Antigravity tool $unknown_tool; add it to guard.sh"
    ;;
esac
tool=${call%%$'\x1f'*}
call=${call#*$'\x1f'}
cwd=${call%%$'\x1f'*}
input=${call#*$'\x1f'}
input=${input%.}
if [ -d "$cwd" ]; then
  cwd=$(cd -P -- "$cwd" 2>/dev/null && pwd -P) || unreadable 'cwd is unavailable'
fi

command -v ast-grep >/dev/null || unreadable 'ast-grep is missing'
here=${BASH_SOURCE[0]%/*}
# shellcheck source=architecture/project.env
source "$here/../project.env" || unreadable 'project settings are unavailable'
[ -n "${CLAUDE_PROJECT_DIR:-}" ] || unreadable 'project folder is unavailable'
project=$(cd -P -- "$CLAUDE_PROJECT_DIR" && pwd -P) 2>/dev/null ||
  unreadable 'project folder is unavailable'
checkout=$(git -C "$project" rev-parse --show-toplevel 2>/dev/null) ||
  unreadable 'project checkout is unavailable'
worktrees=$(git -C "$project" worktree list --porcelain -z 2>/dev/null |
  while IFS= read -r -d '' record; do
    case "$record" in
      ('worktree '*)
        folder=${record#worktree }
        git_dir=$folder/.git
        if [ -f "$git_dir" ] && IFS= read -r pointer < "$git_dir"; then
          case "$pointer" in
            ('gitdir: '/*) git_dir=${pointer#gitdir: } ;;
            ('gitdir: '*) git_dir=$folder/${pointer#gitdir: } ;;
          esac
        fi
        printf '%s\0%s\0' "$folder" "$git_dir"
        ;;
    esac
  done | jq -Rc '
    (split("\u0000") | .[:-1]) as $parts
    | [range(0; $parts | length; 2) as $i | {folder: $parts[$i], git_dir: $parts[$i+1]}]') ||
  unreadable 'worktree list is unavailable'

# ast-grep only supports the first child of a repeated field in an inside
# rule. Collect the relevant node kinds and use their ranges to find parents.
rules=
for kind in ERROR command command_name declaration_command redirected_statement \
  file_redirect heredoc_redirect heredoc_body herestring_redirect variable_assignment for_statement do_group subshell \
  list pipeline if_statement while_statement case_statement function_definition \
  word number string raw_string ansi_c_string translated_string concatenation \
  simple_expansion expansion command_substitution process_substitution arithmetic_expansion; do
  rules+="id: $kind
language: bash
severity: hint
rule: {kind: $kind}
---
"
done
rules=${rules%$'---\n'}

read -r -d '' check <<'JQ' || true
def one_of($words): . as $word | ($words | split(" ") | index($word)) != null;
def path_matches($root; $suffix):
  # The common path is ASCII. Use the regex engine for Unicode case folding.
  if test("[^\\x00-\\x7f]") or ($root | test("[^\\x00-\\x7f]")) then
    test("\\A\\Q" + ($root | gsub("\\\\E"; "\\E\\\\E\\Q")) + "\\E" + $suffix; "i")
  else
    ($root | ascii_downcase) as $folded | ascii_downcase
    | if $suffix == "\\z" then . == $folded
      elif $suffix == "" then startswith($folded)
      else . == $folded or startswith($folded + "/") end
  end;
def under($root): path_matches($root; "(/|\\z)");
def same_path($root): path_matches($root; "\\z");
def path($cwd):
  (if startswith("/") then . else $cwd + "/" + . end)
  | split("/") | reduce .[] as $part ([];
      if $part == "" or $part == "." then .
      elif $part == ".." then .[:-1]
      else . + [$part] end)
  | "/" + join("/");
def path_prefix($cwd):
  if test("[*?$\\[]") then
    sub("[*?$\\[].*"; ""; "s") as $literal
    | [$literal | path($cwd),
       (if $literal == "" or ($literal | endswith("/")) then "folder" else "partial" end)]
  else [path($cwd), "literal"] end;
def hits($root; $prefix; $kind):
  ($prefix | under($root))
  or ($kind != "literal" and ($prefix == "/" or ($root | under($prefix))))
  or ($kind == "partial" and ($root | path_matches($prefix; "")));
def cache: $project + "/.cache";
def lane($cwd):
  path_prefix($cwd) as [$prefix, $dynamic]
  | select(($prefix | under(cache)) | not)
  | first($worktrees[] | select((.folder | same_path($checkout) | not)
        and (.folder | under(cache) | not))
      | select(hits(.folder; $prefix; $dynamic) or hits((.git_dir | path("/")); $prefix; "literal")));
def own($cwd):
  path_prefix($cwd) as [$prefix, $dynamic]
  | ($prefix | under(cache) | not) and hits($project; $prefix; $dynamic);
def finding($rule; $seen): {rule: $rule, seen: $seen};
def write_target($verb; $cwd):
  path_prefix($cwd)[0] as $shown
  | if (lane($cwd) // null) != null then finding("lane-worktree"; $verb + " " + $shown)
    elif own($cwd) then finding("own-checkout"; $verb + " " + $shown)
    else empty end;

# Decode one word selected by the parser. Single quotes and escaped dollars
# keep their literal text. Only simple expansions with known values are used.
def mask_literal($mask):
  if $mask and type == "string" then gsub("[$\u0060]"; "\u001d") else . end;
def expand($vars; $mask):
  gsub("\\$(?:\\{(?<braced>[A-Za-z_][A-Za-z_0-9]*)\\}|(?<name>[A-Za-z_][A-Za-z_0-9]*))";
    (($vars[.braced // .name] | mask_literal($mask)) //
      (if .braced then "${" + .braced + "}" else "$" + .name end)));
def double_word($vars; $mask):
  [scan("\\\\[\\\\\"`$\n]|[^\\\\$]+|\\$(?:\\{[A-Za-z_][A-Za-z_0-9]*\\}|[A-Za-z_][A-Za-z_0-9]*)|.")
    | if startswith("\\") and length == 2 then .[1:] | select(. != "\n") | mask_literal($mask)
      else expand($vars; $mask) end] | join("");
def escape_number($base):
  explode | reduce .[] as $digit (0;
    . * $base + ("0123456789abcdef" | index([$digit] | implode | ascii_downcase)));
def ansi_word:
  [scan("\\\\(?:x[0-9a-fA-F]{1,2}|u[0-9a-fA-F]{1,4}|U[0-9a-fA-F]{1,8}|[0-7]{1,3}|.)|[^\\\\]+")
    | if startswith("\\") then .[1:] as $escape
      | if $escape | test("^[xuU].") then $escape[1:] | escape_number(16) | [.] | implode
        elif $escape | test("^[0-7]") then $escape | escape_number(8) | . % 256 | [.] | implode
        else {a:"\u0007", b:"\b", e:"\u001b", E:"\u001b", f:"\f", n:"\n", r:"\r",
              t:"\t", v:"\u000b", "\\":"\\", "'":"'", "\"":"\""}[$escape] // ("\\" + $escape) end
      else . end] | join("") | split("\u0000")[0];
def word($vars; $mask):
  [scan("(?s)\\$'(?:(?:\\\\.)|[^'\\\\])*'|\\$?\"(?:\\\\.|[^\"\\\\])*\"|'[^']*'|\\\\.|\\$(?:\\{[A-Za-z_][A-Za-z_0-9]*\\}|[A-Za-z_][A-Za-z_0-9]*)|[^\\\\$'\" ]+|.")
    | if startswith("$'") then .[2:-1] | ansi_word | mask_literal($mask)
      elif startswith("'") then .[1:-1] | mask_literal($mask)
      elif startswith("\"") or startswith("$\"") then ltrimstr("$") | .[1:-1] | double_word($vars; $mask)
      elif startswith("\\") and length == 2 then .[1:] | select(. != "\n") | mask_literal($mask)
      else expand($vars; $mask) end] | join("");
def word($vars): word($vars; false);

# The quoted pieces and parameter expansions are opaque to brace expansion.
def brace_range:
  capture("^(?<first>-?[0-9]+|[A-Za-z])\\.\\.(?<last>-?[0-9]+|[A-Za-z])(?:\\.\\.(?<step>-?[0-9]+))?$")
  | . as $range
  | ($range.first | test("^-?[0-9]+$")) as $numeric
  | select($numeric == ($range.last | test("^-?[0-9]+$")))
  | (if $numeric then [$range.first, $range.last] | map(tonumber)
     else [$range.first, $range.last] | map(explode[0]) end) as [$first, $last]
  | ([1, ($range.step // "1" | tonumber | fabs)] | max) as $step
  | (if $first <= $last then 1 else -1 end) as $direction
  | (if $range.first | test("^-?0[0-9]") or ($range.last | test("^-?0[0-9]"))
     then ([$range.first, $range.last] | map(length) | max) else 0 end) as $width
  | range($first; $last + $direction; $step * $direction)
  | if $numeric then
      tostring as $number
      | if ($number | length) >= $width then $number
        else ("0" * ($width - ($number | length))) as $padding
        | if $number | startswith("-") then "-" + $padding + $number[1:] else $padding + $number end
        end
    else [.] | implode end;
def brace_choices:
  . as $tokens
  | reduce range(length) as $i ({place: [], groups: []};
      $tokens[$i] as $token
      | if $token == "{" then .place += [{start: $i, commas: []}]
        elif $token == "," and (.place | length) > 0 then .place[-1].commas += [$i]
        elif $token == "}" and (.place | length) > 0 then
          .place[-1] as $group | .place |= .[:-1] | .groups += [$group + {end: $i}]
        else . end)
  | .groups | sort_by(.start)[] | . as $group
  | (if .commas != [] then
       ([.start] + .commas + [.end]) as $bounds
       | range(($bounds | length) - 1) as $i | $tokens[$bounds[$i]+1:$bounds[$i+1]]
     else $tokens[.start+1:.end] | join("") | brace_range | [.] end) as $choice
  | {start: $group.start, end: $group.end, choice: $choice};
def expand_braces:
  . as $tokens
  | (first(brace_choices).start // null) as $start
  | if $start == null then join("")
    else ($tokens | brace_choices | select(.start == $start)) as $group
    | ($tokens[:$group.start] + $group.choice + $tokens[$group.end+1:]) | expand_braces end;
def brace_words($vars; $mask):
  if contains("{") then
    [scan("(?s)\\\\.|\\$?'(?:\\\\.|[^'\\\\])*'|\\$?\"(?:\\\\.|[^\"\\\\])*\"|\\$\\{[^}]*\\}|[{},]|[^\\\\'\"$ {},]+|.")]
    | expand_braces | word($vars; $mask)
  else word($vars; $mask) end;

# Split the options before a command or subcommand. These tables name only
# options that take a separate value. Other options consume their own word.
def option_split($values; $assignments):
  if .[0] == "--" then {options: [], words: .[1:]}
  elif (.[0] // "") | (startswith("-") and . != "-") or
      ($assignments and test("^[A-Za-z_][A-Za-z_0-9]*="))
  then (if .[0] | one_of($values) then if .[0] == "--option" then 3 else 2 end else 1 end) as $size
    | [.[0:$size] | join("=")] as $option
    | .[$size:] | option_split($values; $assignments)
    | .options = ($option + .options)
  else {options: [], words: .} end;
def option_values($names):
  . as $args | range(length) as $i | $args[$i] as $arg
  | if $arg | one_of($names) then $args[$i+1] // empty
    elif ($arg | split("=")[0] | one_of($names)) and ($arg | contains("="))
    then $arg | sub("^[^=]*="; "")
    else empty end;
def base: split("/")[-1] // "";
def wrapper_options($name):
  if $name == "stdbuf" then "-i -o -e --input --output --error"
  elif $name == "watch" then "-n --interval"
  elif $name == "script" then "-c --command -I -O -B -T -m --log-in --log-out --log-io --log-timing"
  elif $name == "ionice" then "-c -n -p -P -u --class --classdata --pid --pgid --uid"
  elif $name == "chrt" then "-p --pid -T -P -D --sched-runtime --sched-period --sched-deadline"
  elif $name == "parallel" then "-j --jobs -S --sshlogin -a --arg-file --joblog --results --workdir --tmpdir"
  elif $name == "flock" then "-w --wait --timeout -E --conflict-exit-code"
  else "" end;
def wrapper_script($name):
  if $name | one_of("script flock") then first(option_values("-c --command")) // null
  elif $name == "watch" then
    option_split(wrapper_options($name); false)
    | if .options | index("-x") != null then null
      elif .words[0] // "" | test("[\\s;|&]") then .words | join(" ")
      else null end
  else null end;
def unwrap($cwd):
  .[0] as $first | ($first // "" | base) as $name | .[1:] as $args
  | (if $name == "env" then
       ($args | if .[0] == "-" then .[1:] else . end
        | option_split("-u --unset -C --chdir -S --split-string -a --argv0"; true))
     elif $name == "command" and ($args[0] | one_of("-v -V")) then {words: .}
      elif $name | one_of("nohup time command exec caffeinate nice timeout") then
       ($args | option_split(
         if $name == "time" then "-f --format -o --output"
         elif $name == "exec" then "-a"
         elif $name == "caffeinate" then "-t -w"
         elif $name == "nice" then "-n --adjustment"
         elif $name == "timeout" then "-s --signal -k --kill-after"
         else "" end; false))
       | if $name == "timeout" then .words |= .[1:] else . end
      elif ($name | one_of("script flock watch")) and ($args | wrapper_script($name) != null) then
       {words: .}
      elif $name | one_of("stdbuf watch script setsid ionice chrt flock unbuffer") then
       ($args | option_split(wrapper_options($name); false))
       | if $name == "flock" or $name == "chrt" then .words |= .[1:]
         elif $name == "script" then .words |= .[1:]
         else . end
     elif $name == "direnv" and $args[0] == "exec" then
       {words: $args[2:], cwd: ($args[1] | path($cwd))}
     elif $name == "nix" and ($args | option_split("--extra-experimental-features --option"; false)
         | .words[0] | one_of("develop shell")) then
       ([$args | to_entries[] | select(.value | one_of("-c --command")) | .key][0]) as $index
       | if $index == null then {words: .} else {words: $args[$index+1:]} end
     else {words: .} end) as $next
  | if $next.words == . then {words: ., cwd: $cwd, wrappers: []}
    else .[1:(length - ($next.words | length))] as $prefix
      | $next.words | unwrap($next.cwd // ($next.options // [] |
          [option_values("-C --chdir")][-1] | if . == null then $cwd else path($cwd) end))
       | if ($name | one_of("nix script flock")) and $prefix != [] then
          .wrappers += [{name: $name, args: $prefix, cwd: $cwd}]
        else . end
    end;

def arguments($values):
  if length == 0 then {options: [], words: []}
  elif .[0] == "--" then {options: [], words: .[1:]}
  elif (.[0] | startswith("-") and . != "-") then
    (if .[0] | one_of($values) then 2 else 1 end) as $size
    | [if $size == 2 then .[0] + "=" + (.[1] // "") else .[0] end] as $option
    | .[$size:] | arguments($values) | .options = ($option + .options)
  else .[0] as $word | .[1:] | arguments($values) | .words = ([$word] + .words) end;
def has_option($names): any(.[]; split("=")[0] | one_of($names));
# Separate attached values before using the same option parser for both spellings.
def short_values($letters; $stop):
  . as $args
  | [foreach $args[] as $arg ({options: true, value: false, words: []};
      if .value then .value = false | .words = [$arg]
      elif $arg == "--" then .options = false | .words = [$arg]
      elif .options and ($arg | test("^-[^-" + $letters + $stop + "]*[" + $letters + "]")) then
        ($arg | capture("^-(?<flags>[^-" + $letters + $stop + "]*)(?<flag>[" + $letters + "])(?<value>.*)$")) as $parts
        | .value = ($parts.value == "")
        | .words = [(if $parts.flags == "" then empty else "-" + $parts.flags end),
                    "-" + $parts.flag + (if .value then "" else "=" + $parts.value end)]
      else .words = [$arg] end;
      .words[])];
def short_values($letters): short_values($letters; "");
def editors: "sed gsed perl ruby";
def awks: "awk gawk";
def editors_with_program: "sed gsed awk gawk";
def editor_options: "-e -f --expression --file --source";
def editor_args($name):
  if $name | one_of(awks) then
    short_values("efiFv") | arguments(editor_options + " -i --include -F -v")
  else short_values("ef"; "i") | arguments(editor_options) end;
def in_place($name):
  if $name | one_of(editors) then
    any(.options[]; split("=")[0] == "--in-place" or test("^-[^-ef]*i"))
  elif $name | one_of(awks) then any(.options | option_values("-i --include"); . == "inplace")
  else false end;

def git_options:
  short_values("Oe")
  | arguments("-e --regexp -O --output --upload-pack --exec --open-files-in-pager")
  | .options;
def git_read($sub):
  if git_options | has_option("--output") then false
  elif $sub | one_of("log show status diff rev-parse rev-list ls-files ls-tree cat-file describe blame grep merge-base for-each-ref shortlog name-rev ls-remote show-ref diff-tree whatchanged count-objects check-ignore check-attr var version help") then true
  elif $sub == "symbolic-ref" then
    arguments("--reason -m") | (.words | length) == 1 and (.options | has_option("--delete -d -m --reason") | not)
  elif $sub == "branch" then
    arguments("--format") as $args
    | all($args.options[]; split("=")[0] | one_of("--show-current --list --contains --merged --no-merged --format")
        or test("^-[arv]+$"))
      and ($args.words == [] or ($args.options | has_option("--list --contains --merged --no-merged")))
  elif $sub == "config" then
    has_option("--get --list") or (length == 1 and (.[0] | startswith("-") | not))
  elif $sub == "tag" then
    . == [] or (has_option("-l --list") and (has_option("-d --delete -f --force -a --annotate -s --sign") | not))
  elif $sub == "reflog" then
    option_split(""; false) | .words == [] or .words[0] == "show"
  elif $sub == "remote" then . == [] or . == ["-v"]
  elif $sub == "worktree" then .[0] == "list"
  elif $sub == "stash" then .[0] == "list"
  elif $sub == "fetch" then
    arguments("--upload-pack --depth --deepen --shallow-since --shallow-exclude --negotiation-tip --refmap --server-option -o")
    | .options | has_option("--dry-run")
  else false end;
def push_args:
  arguments("--repo --receive-pack --exec -o --push-option")
  | .refs = (if any(.options | option_values("--repo"); true) then .words else .words[1:] end);
def tags_only: .refs == [] and (.options | has_option("--tags"));
def git_sync($sub; $cwd):
  if $sub == "fetch" then true
  elif $sub == "push" then push_args | tags_only
  elif $sub | one_of("merge pull") then
    (arguments("") | .options | index("--ff-only") != null)
  elif $sub == "worktree" and (.[0] | one_of("add remove")) then
    (.[1:] | arguments("-b -B --reason") | .words[0]) as $target
    | $target != null and ($target | (lane($cwd) // null) == null)
  else false end;
def git_info($cwd):
  option_split("-C -c --git-dir --work-tree --namespace --super-prefix --config-env"; false) as $git
  | ($git.options | [option_values("-C")]) as $dirs
  | (reduce $dirs[] as $dir ($cwd; . as $from | $dir | path($from))) as $folder
  | ($git.options | [option_values("--git-dir --work-tree") | path($folder)]) as $paths
  | ([$git.options | option_values("--git-dir") | path($folder)][-1] // "") as $git_dir
  | {sub: $git.words[0], args: $git.words[1:], options: $git.options, folder: $folder, git_dir: $git_dir,
     targets: (if $git_dir == "" or $dirs != [] or ($git.options | has_option("--work-tree") | not)
       then [$paths[], $folder] else $paths end | unique)};
def runtime_ref: test("\\$[({A-Za-z_0-9@*#?!$-]|\u0060");
def push_check($git; $runtime_args):
  ($git.args | push_args) as $push | $push.refs as $refs
  | if ($push.options | has_option("--force --force-with-lease --mirror")
        or any(.[]; test("^-[unqvdf46]*f[unqvdf46]*$"))) or any($refs[]; startswith("+"))
    then finding("main-branch"; "uses a force or mirror push")
  elif any((if $runtime_args == null then $refs else $runtime_args | push_args | .refs end)[]; runtime_ref)
  then finding("main-branch"; "the branch of this push is known only at run time; name the branch")
  elif any($git.options | option_values("-c"); test("^remote\\..+\\.push="; "i"))
  then finding("main-branch"; "sets a push refspec with git -c")
  elif any($refs[]; contains("*") or (split(":")[-1] | . == $main or . == ("refs/heads/" + $main)))
    or ($push.options | has_option("--all --branches"))
  then finding("main-branch"; "pushes to " + $main)
  elif $push | tags_only then empty
  elif $refs == [] or any($refs[]; one_of("HEAD @ @{push} @{upstream} @{u}")) then
    {branch: $git.folder, git_dir: $git.git_dir}
  else empty end;
def gh_check:
  (.[1:] | option_split("-R --repo -h --hostname"; false) | .words) as $gh
  | ($gh[1:] | option_split("-R --repo -h --hostname"; false) | .words) as $group
  | $group[0] as $sub | $group[1:] as $args
  | if $gh[0] == "pr" and $sub == "merge" and ($args | arguments("-R --repo -t --subject -b --body -F --body-file --match-head-commit") | .options | has_option("--admin"))
    then finding("main-branch"; "runs gh pr merge --admin")
    elif $gh[0] == "pr" and $sub == "review" and ($args | arguments("-R --repo -b --body -F --body-file") | .options | has_option("--approve -a"))
    then finding("main-branch"; "runs gh pr review --approve")
    elif $gh[0] == "api" then
      ($gh[1:] | arguments("-X --method -f -F --field --raw-field --input -H --header --hostname -q --jq -t --template --cache")) as $api
      | select($api.options | any(option_values("-X --method"); ascii_upcase | one_of("PUT POST PATCH DELETE"))
        or has_option("-f -F --field --raw-field --input")
        or any(.[]; test("^-[fF].+|^-X(PUT|POST|PATCH|DELETE)$")))
      | ($api.words[0] // "") as $endpoint
      | if $endpoint | test("/dispatches|/actions/(secrets|variables)") then
          finding("machine"; "writes to " + $endpoint)
        elif $endpoint | test("/(protection|rulesets)(/|[?]|$)|/branches/[^/?]+/") then
          finding("main-branch"; "writes to " + $endpoint)
        else empty end
    elif $gh[0] == "repo" and ($sub | one_of("delete rename archive edit"))
    then finding("main-branch"; "runs gh repo " + $sub)
    elif $gh[0] == "auth" and ($sub | one_of("switch login logout refresh setup-git"))
    then finding("machine"; "runs gh auth " + $sub)
    elif ($gh[0] | one_of("secret variable")) and ($sub | one_of("set delete remove"))
    then finding("machine"; "runs gh " + $gh[0] + " " + $sub)
    elif $gh[0] == "workflow" and $sub == "run" then
      ($args | arguments("-R --repo -r --ref -f --raw-field -F --field") | .words[0] // "") as $name
      | select($name == "" or ($name | one_of($workflows) | not))
      | finding("deploy"; "runs gh workflow run " + ($name | if . == "" then "(no name)" else . end))
    elif $gh[0] == "release" and ($sub | one_of("create delete edit"))
    then finding("deploy"; "runs gh release " + $sub)
    else empty end;
def machine_check($name; $args; $words; $remote):
  $words[0] as $sub
  | if $name | one_of("sudo doas shutdown reboot halt") then finding("machine"; "runs " + $name)
  elif ($name | one_of("echo printf rg grep cat head tail wc jq awk sed sd man which type whence gh git") | not)
    and any($args[:-1][]; one_of("sudo doas"))
  then finding("machine"; "runs " + first($args[:-1][] | select(one_of("sudo doas"))))
  elif $remote and ($name | one_of("kill pkill killall")) then finding("machine"; "runs remote " + $name)
  elif $name == "nix-env" then
    ($args | arguments("") | .options[] | split("=")[0]
     | select(one_of("-i --install -e --uninstall") or test("^-[ie][A-Za-z]+$")))
    | finding("machine"; "runs nix-env " + .)
  elif $name == "nix" and $sub == "profile" then
    ($words[1:] | arguments("--profile") | .words[0]
     | select(one_of("install remove upgrade")))
    | finding("machine"; "runs nix profile " + .)
  elif $name == "nixswitch"
    or (($name | one_of("darwin-rebuild nixos-rebuild")) and ($sub | one_of("switch activate boot test")))
    or (($name | one_of("brew pip pip3 npm pnpm cargo")) and $sub == "install" and
        (($name | one_of("npm pnpm") | not) or ($args | has_option("-g --global"))))
  then finding("machine"; "runs " + $name + (if $sub == null then "" else " " + $sub end))
  elif $name == "launchctl" and ($sub != null and ($sub | one_of("list print blame kickstart") | not))
  then finding("machine"; "runs launchctl " + $sub)
  elif $name == "systemctl" and ($sub != null and ($sub | one_of("status show list-units is-active cat") | not))
  then finding("machine"; "runs systemctl " + $sub)
  else empty end;
def deploy_check($name; $sub; $words):
  if ($name | one_of("terraform tofu")) and
      (($sub | one_of("apply destroy import taint untaint"))
       or ($sub == "state" and ($words[1] | one_of("rm mv push")))
       or ($sub == "workspace" and ($words[1] | one_of("new delete"))))
  then finding("deploy"; "runs " + $name + " " + ($words[:(if $sub | one_of("state workspace") then 2 else 1 end)] | join(" ")))
  elif $name | one_of("gcloud kubectl helm") then finding("deploy"; "runs " + $name)
  elif ($name | one_of("docker podman")) and ($sub | one_of("push login"))
  then finding("deploy"; "runs " + $name + " " + $sub)
  else empty end;

def shell_args:
  if length == 0 or .[0] == "--" then {args: (if .[0] == "--" then .[1:] else . end)}
  elif .[0] | test("^[-+][^-]") and length > 1 then
    if .[0] | test("^-[a-zA-Z]*c[a-zA-Z]*$") then
      .[1:] | if .[0] == "--" then .[1:] else . end | {code: .[0]}
    else (if .[0] | one_of("-O +O -o +o") then .[2:] else .[1:] end) | shell_args end
  elif .[0] | startswith("--") then .[1:] | shell_args
  else {args: .} end;
def all_argument_writers: "rm rmdir touch mkdir chmod chown truncate sd patch tee";
def last_argument_writers: "cp mv ln install rsync";
def runners: "cargo nix just make npm pnpm codex claude agy antigravity";
def shells: "bash sh zsh";
def directory_options: "-d --directory";
def runner_options:
  "-C --manifest-path --justfile --working-directory -f --file --add-dir --cwd";
def program_options: "-e --expression --source";
def editor_program:
  (.options | option_values(program_options)),
  (if .options | has_option(program_options + " -f --file") then empty else .words[0] // empty end);
def edited_files($name):
  if $name | one_of(editors + " " + awks) then
    editor_args($name) | select(in_place($name))
    | if .options | has_option(program_options + " -f --file") then .words[]
      elif ($name | one_of("sed gsed")) and .words[0] == "" then .words[2:][]
      else .words[1:][] end
  else empty end;
def writes($name):
  if $name == "dd" then .[] | select(startswith("of=")) | ltrimstr("of=")
  elif $name | one_of(last_argument_writers) then
    short_values("t" + (if $name == "install" then "mogS" elif $name | one_of("mv ln cp") then "S" else "" end))
    | arguments("-t --target-directory -m --mode -o --owner -g --group -S --suffix")
    | (.options | option_values("-t --target-directory")), (.words[-1] // empty)
  elif $name == "patch" then
    short_values("doiFpr") | arguments(directory_options + " -o --output -i --input -F --fuzz -p --strip -r --reject-file")
    | .words[], (.options | option_values(directory_options + " -o --output -r --reject-file -i --input"))
  elif $name == "sd" then arguments("") | .words[2:][]
  elif $name | one_of("chmod chown") then
    arguments("--reference") | if .options | has_option("--reference") then .words[] else .words[1:][] end
  elif $name | one_of("mkdir truncate touch") then
    (if $name == "mkdir" then short_values("m") | arguments("-m --mode")
     elif $name == "truncate" then short_values("sr") | arguments("-s --size -r --reference")
     else short_values("dtr") | arguments("-d --date -t -r --reference") end)
    | .words[]
  elif $name | one_of(all_argument_writers) then
    (index("--") // length) as $end
    | (.[:$end][] | if startswith("-") then capture("^--[^=]+=(?<path>.+)$").path else . end),
      (.[$end+1:][])
  elif $name == "sort" then short_values("okStT") | arguments("-o --output -k --key -S --buffer-size -t --field-separator -T --temporary-directory")
    | .options | option_values("-o --output")
  elif $name | one_of("uniq xxd") then
    (if $name == "uniq" then short_values("fsw") | arguments("-f -s -w --skip-fields --skip-chars --check-chars")
     else short_values("colsg") | arguments("-c -o -l -s -g")
       | select(.options | any(.[]; test("^-[^-=]*r[^-=]*$"))) end)
    | .words[1] // empty
  else edited_files($name) end;

def tar_options:
  (.[0] // "") as $first
  | if $first == "" then empty
    elif $first == "--" then .[1:][] | {operand: .}
    elif $first | startswith("--") then
      if $first | one_of("--create --extract --get --remove-files") then
        {mode: {"--create":"c", "--extract":"x", "--get":"x", "--remove-files":"remove"}[$first]},
        (.[1:] | tar_options)
      elif $first | one_of("--file --directory") then
        {($first[2:]): .[1]}, (.[2:] | tar_options)
      elif $first | test("^--(file|directory)=") then
        ($first | capture("^--(?<name>[^=]+)=(?<value>.*)$") | {(.name): .value}), (.[1:] | tar_options)
      elif $first | one_of("--files-from --exclude --exclude-from --use-compress-program --format")
      then .[2:] | tar_options
      else .[1:] | tar_options end
    elif $first | test("^-[A-Za-z]") then
      ($first[1:] | capture("^(?<flags>[A-Za-z]*?)(?:(?<value>[bCfFHIKLNTX])(?<attached>.*)|$)")) as $short
      | ({mode: $short.flags},
         (if $short.value | one_of("f C") then
            {(if $short.value == "f" then "file" else "directory" end):
               (if $short.attached == "" then .[1] else $short.attached end)}
          else empty end),
         (if $short.value != null and $short.attached == "" then .[2:] else .[1:] end | tar_options))
    else {operand: $first}, (.[1:] | tar_options) end;
def tar_details:
  (.[0] // "") as $first
  | if $first | test("^[A-Za-z]+$") then
      [$first | scan("[bCfFHIKLNTX]")] as $values | . as $args
      | {mode: $first}, (range($values | length) as $i | $values[$i]
        | select(one_of("f C")) | {(if . == "f" then "file" else "directory" end): $args[1+$i]}),
        (.[1 + ($values | length):] | tar_options)
    else tar_options end;
def tar_targets($cwd):
  [tar_details] as $options
  | ([$options[].mode // empty | select(. != "remove")] | join("")) as $modes
  | ([$options[].directory // empty]) as $dirs
  | (if $modes | contains("c") then $options[].file // empty | select(. != "-") else empty end),
    (if $modes | contains("x") then
       if $dirs == [] then $cwd
       else
         foreach $dirs[] as $directory ($cwd; . as $base | $directory | path($base); .)
       end
     else empty end),
    (if any($options[]; .mode == "remove") then
       foreach $options[] as $option ({cwd: $cwd, target: null};
         if $option.directory != null then .cwd = ($option.directory | path(.cwd))
         else . end | .target = (if $option.operand == null then null else .cwd as $base | $option.operand | path($base) end);
         .target // empty)
     else empty end);
def find_starts:
  if .[0] | one_of("-D -O") then .[2:] | find_starts
  elif (.[0] // "") | test("^-(?:[HLP]+|O[0-9]+|D.+)$") then .[1:] | find_starts
  else . end;
def find_args:
  find_starts
  | (map(startswith("-") and . != "-" or one_of("! ( ) ,")) | index(true)) as $expression
  | {starts: .[:$expression], expression: (if $expression == null then [] else .[$expression:] end)};
def find_actions:
  if length == 0 then empty
  elif .[0] | one_of("-exec -execdir -ok -okdir") then
    .[1:] as $tail | ($tail | map(one_of("; +")) | index(true)) as $end
    | {command: $tail[:$end], in_dir: (.[0] | one_of("-execdir -okdir"))},
      (if $end != null then $tail[$end+1:] | find_actions else empty end)
  elif .[0] | one_of("-name -iname -path -ipath -wholename -iwholename -regex -iregex -regextype -lname -ilname -type -xtype -size -user -group -uid -gid -perm -mtime -mmin -newer -mindepth -maxdepth -printf") then
    .[2:] | find_actions
  elif .[0] | one_of("-fprint -fprint0 -fls -fprintf -delete") then
    {write: true}, (.[(if .[0] == "-delete" then 1 elif .[0] == "-fprintf" then 3 else 2 end):] | find_actions)
  else .[1:] | find_actions end;
def writer_paths($name; $cwd):
  if $name == "tar" then tar_targets($cwd)
  elif $name == "curl" then
    short_values("owHdF") | arguments("-o --output --url --output-dir -w --write-out -H --header -d --data -F --form")
    | (.options | option_values("-o --output") | select(. != "-")),
      (if .options | has_option("--remote-name --remote-name-all") or any(.[]; test("^-[^-=]*O[^-=]*$"))
       then $cwd else empty end)
  elif $name == "wget" then short_values("OP") | arguments("-O -P --output-document --directory-prefix")
    | .options | (option_values("-O --output-document") | select(. != "-")),
      option_values("-P --directory-prefix")
  elif $name == "unzip" then short_values("d") | arguments("-d") | .options | option_values("-d")
  elif $name == "find" then find_args
    | select(any(.expression | find_actions; .write))
    | if .starts == [] then $cwd else .starts[] end
  elif $name == "flock" then option_split(wrapper_options($name); false)
    | .words[0] // empty | select(test("^[0-9]+$") | not)
  elif $name == "script" then option_split(wrapper_options($name); false)
    | (.words[0] // empty), (.options | option_values("-I -O -B -T --log-in --log-out --log-io --log-timing"))
  else empty end;
def program_targets($name; $cwd):
  if $name | one_of(editors_with_program) then
    editor_args($name) | editor_program
    | if $name | one_of("sed gsed") then
        scan("(?:^|[;{}\\n])[ \\t]*(?:(?:[0-9]+(?:~[0-9]+)?|\\$|/(?:\\\\.|[^/\\\\])*/)(?:[ \\t]*,[ \\t]*(?:[0-9]+|\\$|/(?:\\\\.|[^/\\\\])*/))?[ \\t]*!?)?[ \\t]*w[ \\t]*(?<file>[^\\n]+)")
        | ((.[0] | gsub("^[ \\t]+|[ \\t]+$"; "")), (if ($cwd | lane("/") // null) != null then $cwd else empty end))
      elif test("[>|]") then
        (scan("[>|][ \\t]*(\"(?:\\\\.|[^\"\\\\])*\")") | .[0] | word({})),
        (scan("\"(?:\\\\.|[^\"\\\\])*\"|[^\\s\"{}();,>|]+") | word({})
         | select(startswith("/") and (lane($cwd) // null) != null)),
        (if ($cwd | lane("/") // null) != null then $cwd else empty end)
      else empty end
  else empty end;
def runner_paths($name; $sub; $words; $cwd):
  if $name | one_of(shells) then shell_args | .args[0] // empty
  elif $name | one_of(runners) then
    select($name != "nix" or ($sub | one_of("build run develop shell")) or
           ($sub == "flake" and $words[1] == "check"))
    | ($cwd, (short_values("Cf") | arguments(runner_options) | .options | option_values(runner_options)),
       (if $name == "nix" then
          arguments(runner_options + " --arg --argstr --override-input --extra-experimental-features")
          | .words[1:][] | split("#")[0]
        else empty end))
  else empty end;
def lane_folder($cwd):
  select(. != "" and (lane($cwd) // null) != null) | path_prefix($cwd)[0];
def lane_run($name; $cwd):
  lane_folder($cwd) | finding("lane-worktree";
    "runs " + $name + (if . == $cwd then " in " else " with " end) + .);
def check_writes($targets; $cwd; $name):
  first(
    ($targets[] | lane_folder($cwd) |
     if $name == null then finding("lane-worktree"; "writes " + .) else lane_run($name; $cwd) end),
    ($targets[] | write_target("writes"; $cwd)));
def wrapper_checks:
  .wrappers[] | . as $wrapper
  | if .name == "nix" then
      .args | option_split("--extra-experimental-features --option"; false) | .words as $words
      | $wrapper.args | runner_paths("nix"; $words[0]; $words; $wrapper.cwd) | lane_run("nix"; $wrapper.cwd)
    else .args | writer_paths($wrapper.name; $wrapper.cwd)
      | first(lane_run($wrapper.name; $wrapper.cwd), write_target("writes"; $wrapper.cwd)) end;
def git_lane_risk:
  [.options[], (.args | git_options[])]
  | has_option("-c --output --ext-diff --textconv -O --open-files-in-pager --upload-pack --exec");
def executor_names: "find fd xargs parallel";
def fd_args:
  (first(to_entries[] | select(.value | test("^-(?:[xX].*|-exec(?:-batch)?(?:=.+)?)$"))) // {}) as $exec
  | (if $exec.key == null then . else .[:$exec.key] end
     | arguments("-t --type -e --extension -E --exclude -d --max-depth --min-depth --exact-depth -j --threads -S --size -o --owner --base-directory --search-path --path-separator --color --ignore-file --changed-within --changed-before --max-results")) as $search
  | {starts: [$search.words[1:][], ($search.options | option_values("--search-path"))],
     command: (if $exec.key == null then null
       else [($exec.value | capture("^(?:-[xX]|--exec(?:-batch)?=)(?<command>.+)$").command), .[$exec.key+1:][]] end)};
def protected_start($cwd):
  first(.[] | select((lane($cwd) // null) != null or own($cwd)) | path($cwd)) // null;
def search_start($name; $cwd):
  if $name == "find" then find_args | .starts
  elif $name == "fd" then fd_args | .starts
  else [] end
  | if . == [] and ($name | one_of("find fd")) then [$cwd] else . end
  | protected_start($cwd);
def executor_commands($name; $cwd; $state):
  search_start($name; $cwd) as $start
  | if $name == "find" then
      find_args | .expression | find_actions | select(.command != null)
      | . + {start: $start, replace: "{}"}
    elif $name == "fd" then
      fd_args | select(.command != null) | {command, start: $start, replace: "{}", append: true}
    elif $name == "xargs" then
      short_values("adEeIiLlnPs") |
      option_split("-a --arg-file -d --delimiter -E -e --eof -I -i --replace -L -l --max-lines -n --max-args -P --max-procs -s --max-chars"; false)
      | .options as $options
      | ([$options | option_values("-a --arg-file")] | protected_start($cwd)) as $file
      | (last($options | option_values("-I -i --replace")) // null) as $replace
      | {command: .words, start: ($file // $state.executor_start // ([$cwd] | protected_start("/"))),
         replace: $replace, append: ($replace == null)}
    elif $name == "parallel" then
      option_split(wrapper_options($name); false)
      | (.words | map(one_of("::: :::: :::+ ::::+")) | index(true)) as $end
      | ([.options | option_values("--workdir -a --arg-file")] | protected_start($cwd)) as $directory
      | (if $end == null then [] else .words[$end+1:] end) as $inputs
      | {command: .words[:$end], replace: "{}", append: true,
         start: ($directory // ($inputs | protected_start($cwd)) // $state.executor_start // ([$cwd] | protected_start("/"))),
         in_dir: (.options | has_option("--workdir"))}
    else empty end;
def executor_words:
  if .start == null or .command == [] then .command
  else (.start + "/{}") as $path | .replace as $replace | .append as $append
    | .command
    | if $replace != null and any(.[]; contains($replace)) then
        map(split($replace) | join($path))
      elif $append then . + [$path] else . end
  end;
def clone_target:
  arguments("-b --branch -o --origin -u --upload-pack --template --reference --reference-if-able -c --config --depth --shallow-since --shallow-exclude --separate-git-dir --filter -j --jobs --server-option --revision") as $clone
  | $clone.words[1] // ($clone.words[0] // empty | rtrimstr("/") | rtrimstr("/.git")
      | sub("^[^/]*:"; "") | base | rtrimstr(".git")
      | if $clone.options | has_option("--bare --mirror") then . + ".git" else . end);
def redirection($cwd):
  .operator as $op | .descriptor as $fd | .target
  | select($op | one_of("> >> &> &>> >| <> >&"))
  | select($op != ">&" or ($fd == "" and (test("^[0-9]+-?$|^-$") | not)))
  | write_target("redirects to"; $cwd);

def command_checks($name; $args; $cwd; $state; $remote):
  ($args | if $name == "cargo" and (.[0] // "" | startswith("+")) then .[1:] else . end
  | option_split(
    if $name == "systemctl" then "-H --host -M --machine -t --type --state --property -p"
    elif $name | one_of("docker podman") then "-c --context -H --host --config --connection --log-level"
    elif $name | one_of("terraform tofu") then "-chdir"
    elif $name == "nix" then "--extra-experimental-features --option"
    elif $name == "cargo" then "--config -Z"
    elif $name | one_of("pip pip3") then "--python --proxy --timeout --log"
    elif $name | one_of("npm pnpm") then "--prefix -C --dir"
    elif $name | one_of("darwin-rebuild nixos-rebuild") then "--flake"
    else "" end; false) | .words) as $words
  | $words[0] as $sub
  | (if $name == "git" then $args | git_info($cwd) else null end) as $git
  | (if $git != null then $git.args | git_read($git.sub) else false end) as $read_git
  | (machine_check($name; $args; $words; $remote), deploy_check($name; $sub; $words)),
    (if $name == "gh" then ([$name] + $args | gh_check) else empty end),
     (if $git.sub == "push" then push_check($git; $state.runtime_args) else empty end),
    (if $name == "git" and $git.sub == "worktree" and ($git.args[0] | one_of("add remove")) then
       $git.args[1:] | arguments("-b -B --reason") | .words[0] // empty
       | select((lane($git.folder) // null) != null) | write_target("writes"; $git.folder)
     else empty end),
     (if $git != null and (($read_git | not) or ($git | git_lane_risk)) then
        $git.targets[] | lane_folder($cwd)
        | finding("lane-worktree"; "runs git " + ($git.sub // "(no subcommand)") + " in " + .)
      else empty end),
     check_writes([$args | (writes($name), program_targets($name; $cwd)) | select(. != "")]; $cwd; null),
     check_writes([$args | writer_paths($name; $cwd) | select(. != "")]; $cwd; $name),
     (if $git.sub == "apply" then
        $git.args | short_values("d") | arguments(directory_options) | .options
        | option_values(directory_options) | write_target("writes"; $git.folder)
      else empty end),
     ($args | runner_paths($name; $sub; $words; $cwd) | lane_run($name; $cwd)),
     (if $git.sub == "clone" then $git.args | clone_target | write_target("writes"; $git.folder)
     else empty end),
    (if $git != null and $git.sub != "clone" and ($read_git | not) and ($git.args | git_sync($git.sub; $git.folder) | not) then
       ($git.targets[] | select(own($cwd))) as $target
       | finding("own-checkout"; "runs git " + ($git.sub // "(no subcommand)") + " in " + $target)
     else empty end),
    (if $git != null and $git.sub != "clone" and ($read_git | not) and ($git.git_dir == "") and
        ($git.args | git_sync($git.sub; $git.folder) | not) and ($git.folder | under(cache))
     then {checkout: $git.folder, sub: ($git.sub // "(no subcommand)")} else empty end),
     (if $name | one_of(executor_names) then
        ($args | executor_commands($name; $cwd; $state)) as $executor
        | ($executor | executor_words | unwrap(if $executor.in_dir then $executor.start // $cwd else $cwd end)) as $child
        | select(($child.words | length) > 0)
        | (if $name == "parallel" and ($executor.command[0] | test("[\\s;|&]")) then
             {script: ($child.words | join(" ")), cwd: $child.cwd, remote: $remote, vars: {}}
           else ($child | wrapper_checks),
           (command_checks(($child.words[0] | base); $child.words[1:]; $child.cwd;
             ($state | .executor_start = null); $remote)) end)
        | if .rule == "lane-worktree" and $executor.start != null then
            $executor.start | lane_run($name; $cwd)
          else . end
     else empty end),
     (if $name | one_of(shells) then
       $args | shell_args | .code // empty
     elif $name == "eval" then $args | join(" ")
     elif $name == "ssh" then
       $args | option_split("-B -b -c -D -E -e -F -I -i -J -L -l -m -O -o -P -p -Q -R -S -W -w"; false)
       | .words[1:] | if .[0] == "--" then .[1:] else . end | select(length > 0) | join(" ")
     elif $name | one_of("script flock watch") then
       $args | wrapper_script($name) // empty
     else empty end
     | {script: ., cwd: $cwd, remote: ($remote or $name == "ssh"), vars: (if $name == "eval" then $state.vars else {} end)});

# All source spans are byte offsets. Compare spans without indexing UTF-8 text.
def contains_node($node):
  .start <= $node.start and $node.end <= .end;
def encloses($node):
  contains_node($node) and (.start != $node.start or .end != $node.end);
def children:
  reduce (sort_by(.start, -.end, .kind)[]) as $node
    ([]; if length > 0 and (.[-1] | contains_node($node)) then . else . + [$node] end);
def word_node:
  .kind | one_of("word number string raw_string ansi_c_string translated_string concatenation simple_expansion expansion command_substitution process_substitution arithmetic_expansion");
def scope_node:
  (.kind | one_of("for_statement subshell pipeline if_statement while_statement case_statement function_definition command_substitution process_substitution"))
  or (.background and (.kind | one_of("list redirected_statement")));
def within($parent; $nodes):
  $nodes[] | . as $node | select($parent | encloses($node));
def bare_word($vars; $mask):
  if .kind | one_of("command_substitution process_substitution arithmetic_expansion") then "$()"
  else .text | brace_words($vars; $mask) end;
def bare_word($vars): bare_word($vars; false);
def redir_words($nodes):
  . as $redirect | [within($redirect; $nodes) | select(word_node)] | children;
def command_words($nodes; $vars; $redirects; $mask):
  . as $cmd
  | ([ $nodes[] | . as $node | select(.kind == "command_name" and ($cmd | contains_node($node))) ]
     | sort_by(.start, -.end) | .[0]) as $name
  | if $name == null then []
    else
      [$name | bare_word($vars; $mask)] +
      ([within($cmd; $nodes) | select(.start >= $name.end and (word_node or
         (.kind | one_of("file_redirect heredoc_redirect herestring_redirect"))))] | children |
       map(if .kind == "file_redirect" then redir_words($nodes)[1:] | map(bare_word($vars; $mask))
           elif .kind | one_of("heredoc_redirect herestring_redirect") then []
           else [bare_word($vars; $mask)] end) | add // []) +
      [$redirects[] | select(.start >= $cmd.end) | redir_words($nodes)[1:][] | bare_word($vars; $mask)]
    end;
# Nodes are in source order, with enclosing spans first. A statement only
# looks at the slice covered by its span, or by its redirection wrapper.
def after($nodes; $i; $end):
  $i | until(. >= ($nodes | length) or $nodes[.].start >= $end; . + 1);
def statements($nodes):
  foreach range($nodes | length) as $i
    ({skip: 0, assigned: 0, redirected: 0, wrapper: null, item: null};
     .item = null
     | if $i < .skip then .
       else $nodes[$i] as $node
       | if $node | scope_node or .kind == "heredoc_body" then
           .skip = after($nodes; $i+1; $node.end)
           | if $node.kind == "heredoc_body" then .
             else .item = ($node + {nodes: $nodes[$i+1:.skip]}) end
         elif $node.kind == "redirected_statement" then .wrapper = $node
         elif $node.kind == "command" then
           .assigned = $node.end
           | .redirected = (if .wrapper.start == $node.start then .wrapper.end else $node.end end)
           | $nodes[$i+1:after($nodes; $i+1; .redirected)] as $body
           | .item = ($node + {nodes: $body,
               redirects: ($body | map(select(.kind | one_of("file_redirect heredoc_body"))))})
           | .item.redirects |= (children | map(select(.kind == "file_redirect")))
         elif $node.kind == "declaration_command" or
              ($node.kind == "variable_assignment" and $node.start >= .assigned) or
              ($node.kind == "file_redirect" and $node.start >= .redirected) then
           .assigned = (if $node.kind == "declaration_command" then $node.end else .assigned end)
           | .item = ($node + {nodes: $nodes[$i+1:after($nodes; $i+1; $node.end)]})
         else . end
       end;
     .item // empty);

def set_vars($assignments; $nodes):
  reduce $assignments[] as $node (.;
    ($node.text | split("=")[0]) as $key
    | ($node.text | sub("^[^=]*="; "")) as $value
    | if ($value | word({}; true) | runtime_ref) or any(within($node; $nodes); scope_node) then del(.vars[$key])
      else .vars[$key] = ($value | word({})) end);
def redirect_checks($nodes; $state):
  . as $redirect | redir_words($nodes)[0] | select(. != null) | bare_word($state.vars) as $target
  | ($redirect.text | capture("^(?<descriptor>[0-9]*)(?<operator>&>>|&>|>>|>\\||>&|<>|>|<)"))
  | . + {target: $target} | redirection($state.cwd);

def execute($nodes; $state):
  reduce statements($nodes) as $item ($state;
    . as $before
    | if $item | scope_node then
        ([within($item; $item.nodes)]) as $body
        | if $item.kind == "for_statement" then
            ([$body[] | select(.kind == "do_group")] | sort_by(.start, -.end) | .[0]) as $group
            | ($item.text | capture("^(?:for|select)[ \\t]+(?<name>[A-Za-z_][A-Za-z_0-9]*)").name) as $var
            | ([$body[] | select(.end <= $group.start and word_node)] | children
                | map(bare_word($before.vars))) as $values
            | .events += [(if $values == [] then [null] else $values end)[] as $value |
                execute([within($group; $body)];
                  ($before | .vars[$var] = $value | .events = [])) | .events[]]
          else
            .events += (execute($body; ($before | .events = [] | .executor_start = null |
              .track_cwd = ($item.kind != "pipeline"))) | .events)
          end
      elif $item.kind == "variable_assignment" then
        if $item.background then . else set_vars([$item]; $item.nodes) end
      elif $item.kind == "declaration_command" then
        if ($item.background | not) and ($item.text | test("^export([ \\t]|$)")) then
          set_vars([within($item; $item.nodes) | select(.kind == "variable_assignment")]; $item.nodes)
        else . end
      elif $item.kind == "file_redirect" then
        .events += [$item | redirect_checks($item.nodes; $before)]
      else
        ($item | command_words($item.nodes; $before.vars; $item.redirects; false)) as $raw
        | ($raw | unwrap($before.cwd)) as $command
        | ($command.words[0] // "" | base) as $name | $command.words[1:] as $args
        | (if $name == "git" and ($args | index("push")) != null and any($args[]; runtime_ref) then
             $item | command_words($item.nodes; $before.vars; $item.redirects; true)
             | unwrap($before.cwd) | .words[1:] | git_info($command.cwd) | .args
           else null end) as $runtime_args
        | .events += [($item.redirects[] | redirect_checks($item.nodes; $before)),
            ($command | wrapper_checks),
            (if $name != "" and ($name | contains("$") | not) then
              command_checks($name; $args; $command.cwd; ($before | .runtime_args = $runtime_args); $remote)
             else empty end)]
        | if .track_cwd then . else .executor_start = ($args | search_start($name; $command.cwd)) end
        | ($args | option_split(""; false) | .words[0]) as $directory
        | if (.track_cwd and ($item.background | not)) | not then .
          elif $name == "popd" and .dirs != [] then .cwd = .dirs[-1] | .dirs |= .[:-1]
          elif ($name | one_of("cd pushd")) and $directory != null
              and ($directory | test("[\\$*?\\[~]") | not) then
            (if $name == "pushd" then .dirs += [.cwd] else . end)
            | .cwd = ($directory | path($before.cwd))
          else . end
      end);

def node:
  # The trailing source is outside the parsed node. Distinguish the background
  # separator from a logical operator or a redirection on the same line.
  {kind: .ruleId, text, start: .range.byteOffset.start, end: .range.byteOffset.end,
   background: (.charCount.trailing as $trailing |
     $trailing > 0 and (.lines[-$trailing:] | test("^[ \\t]*&([^&>]|$)")))};
def fallback_lane:
  any($input | scan("\"[^\"\\n]+\"|'[^'\\n]+'|[^\\s;|&<>()\"']+")
     | word($vars) | sub("^(?:[A-Za-z_][A-Za-z0-9_]*=|--[^=]+=|-[Cf](?=/))"; "")
     | split("#")[0] | select(. != "");
     (lane($cwd) // null) != null);
def fallback_word($words):
  # A slash or a file name character next to the match makes it part of a path or name.
  first($input | scan("(?<![[:alnum:]/._-])(" + $words + ")(?![[:alnum:]/._-])") | .[0]);
def fallback:
  (fallback_word("sudo|doas|push|terraform|tofu|gcloud|kubectl|helm|nixswitch|darwin-rebuild|nixos-rebuild|shutdown|reboot|halt|launchctl|systemctl|nix-env|brew|pip|pip3|npm|pnpm|docker|podman|ssh|git[ \\t]+push|gh[ \\t]+(?:auth|repo|secret|variable|release|workflow|api|pr[ \\t]+(?:merge|review))|nix[ \\t]+profile|cargo[ \\t]+install") //
   (if $input | gsub("(?<![0-9])2(?:>&1|>/dev/null)(?=$|[\\s;|&)<>])|(?<![0-9])>&2(?=$|[\\s;|&)<>])"; "") | contains(">")
    then ">" else empty end) //
   (fallback_word(([all_argument_writers, last_argument_writers, editors, awks, runners, shells,
      "dd sort uniq xxd curl wget unzip tar find flock script"] | join(" ") | gsub(" "; "|")) + "|git[ \\t]+apply")
    | select(($cwd | lane("/") // null) != null or fallback_lane))) as $risk
  | {reason: ("owner guard: can not read this call (the Bash grammar does not parse it, and it holds " +
              $risk + "). Write it in plain Bash.")};
if $tool != "Bash" then ($input, $real_path) | write_target("edits"; $cwd)
else
  [inputs | node] as $nodes
  | [$nodes[] | select(.kind == "ERROR") | .text
       | select(test("^\\[[ \\t]+[A-Za-z0-9_.-]+[ \\t]+\\\\>[ \\t]+[A-Za-z0-9_.-]+[ \\t]+\\]$") | not)] as $errors
  | if any($errors[]; test("^<<-?[^<]")) then {syntax: true}
    elif $errors != [] or ($bash_syntax and any($nodes[]; .kind == "command" and (.text | startswith("=("))))
    then fallback
    elif $bash_syntax then {syntax: true}
    else $nodes | sort_by(.start, -.end, (if .kind == "redirected_statement" then 0 else 1 end), .kind)
      | execute(.; {cwd: $cwd, dirs: [], vars: $vars, events: [], track_cwd: true}) | .events[]
    end
end
JQ

# Use a separator after each JSON record. jq escapes newlines within the record.
inspect() {
  local tool_name=$1 code=$2 folder=$3 remote=$4 depth=$5 vars=$6
  local events event rule seen nested nested_cwd nested_remote nested_vars branch git_dir sub root result syntax reason real_path
  local bash_syntax=false
  if [ "$tool_name" = Bash ]; then
    # Bash catches missing tokens that can be zero-width in the parser tree.
    if ! syntax=$(BASH_ENV='' bash -n <<< "$code" 2>&1) || [ -n "$syntax" ]; then
      bash_syntax=true
    fi
    events=$(printf '%s' "$code" |
      ast-grep scan --stdin --inline-rules "$rules" --json=stream 2>/dev/null |
      jq -cn --arg tool "$tool_name" --arg cwd "$folder" --arg project "$project" \
        --arg checkout "$checkout" --arg main "$HARNESS_MAIN" --arg workflows "$HARNESS_OWNER_WORKFLOWS" \
        --argjson worktrees "$worktrees" --argjson remote "$remote" --argjson vars "$vars" \
        --arg input "$code" --arg real_path '' --argjson bash_syntax "$bash_syntax" "$check" 2>/dev/null) ||
      unreadable 'Bash parse or rule check failed'
  else
    # Resolve links in the existing prefix, even when the edit creates a file.
    if ! IFS= read -r -d '' real_path < <(cd -- "$folder" && realpath -mz -- "$code"); then
      unreadable 'file path check failed'
    fi
    events=$(jq -cn --arg tool "$tool_name" --arg cwd "$folder" --arg project "$project" \
      --arg checkout "$checkout" --arg main "$HARNESS_MAIN" --arg workflows "$HARNESS_OWNER_WORKFLOWS" \
      --argjson worktrees "$worktrees" --argjson remote "$remote" --argjson vars "$vars" \
      --arg input "$code" --arg real_path "$real_path" --argjson bash_syntax false "$check" 2>/dev/null) ||
      unreadable 'file path check failed'
  fi
  [ -n "$events" ] || return 0
  while IFS= read -r event; do
    # The trailing marker prevents read from mistaking a truncated record for
    # a complete one. Never evaluate a value from the call in this shell.
    {
      IFS= read -r -d '' rule &&
      IFS= read -r -d '' seen &&
      IFS= read -r -d '' nested &&
      IFS= read -r -d '' nested_cwd &&
      IFS= read -r -d '' nested_remote &&
      IFS= read -r -d '' nested_vars &&
      IFS= read -r -d '' branch &&
      IFS= read -r -d '' git_dir &&
      IFS= read -r -d '' root &&
      IFS= read -r -d '' sub &&
      IFS= read -r -d '' reason &&
      IFS= read -r -d '' syntax
    } < <(jq -j '
      [.rule, .seen, .script, .cwd, (.remote // false | tostring), (.vars // {} | tojson),
       .branch, .git_dir, .checkout, .sub, .reason, (if .syntax then "yes" else "" end)] | .[] | (. // ""), "\u0000"
    ' <<< "$event") || unreadable 'rule check failed'
    if [ -n "$rule" ]; then
      deny "$rule" "$seen"
    elif [ -n "$reason" ]; then
      deny_reason "$reason"
    elif [ -n "$syntax" ]; then
      unreadable 'Bash syntax error'
    elif [ -n "$root" ]; then
      result=$(git -C "$root" rev-parse --show-toplevel 2>/dev/null) || continue
      if [ "$result" = "$checkout" ]; then
        deny own-checkout "runs git $sub in $root"
      fi
    elif [ -n "$branch" ]; then
      if [ -n "$git_dir" ]; then
        result=$(git -C "$branch" --git-dir="$git_dir" branch --show-current 2>/dev/null) || result=
      else
        result=$(git -C "$branch" branch --show-current 2>/dev/null) || result=
      fi
      [ -n "$result" ] || deny main-branch "can not read the current branch in $branch"
      if [ "$result" = "$HARNESS_MAIN" ]; then
        deny main-branch "pushes to $HARNESS_MAIN"
      fi
    elif [ -n "$nested" ] && [ "$depth" -lt 2 ]; then
      inspect Bash "$nested" "$nested_cwd" "$nested_remote" "$((depth + 1))" "$nested_vars"
    fi
  done <<< "$events"
}

inspect "$tool" "$input" "$cwd" false 0 '{}' || unreadable 'rule check failed'
allow
