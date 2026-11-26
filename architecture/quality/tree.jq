include "report";

def path: .file | ltrimstr("./");
def line: .range.start.line + 1;
def site: [path, line];
def in_scope($scope): $scope == "." or startswith($scope + "/");
def tsv_key: gsub("\t"; "%09") | gsub("\r"; "%0D") | gsub("\n"; "%0A");
def location:
  .[0] as $path | .[1] as $line |
  ($path | gsub("%"; "%25") | gsub(" "; "%20") | tsv_key) +
    ":" + ($line | tostring);

def row($signal; $key; $count; $sites):
  {signal: $signal, key: $key, count: $count, places: ($sites | map(location) | places)};

def check($signal; $key; $sites):
  row($signal; $key; ($sites | length); $sites);

def name($capture): .metaVariables.single[$capture].text | ltrimstr("r#");
def name_id($capture): [.file, .metaVariables.single[$capture].range.byteOffset.start] | tojson;
def reaches_helper($names): any(.references[]; in($names));

def helper_names($helpers; $names):
  reduce ($helpers[] | select(reaches_helper($names))) as $helper
    ($names; .[$helper.name] = true) as $next |
  if ($next | length) == ($names | length) then $names
  else helper_names($helpers; $next)
  end;

def test_code($tests_by_file):
  (path | test($test_path)) or
  (.range.byteOffset as $macro | any($tests_by_file[.file][]?;
    .range.byteOffset | .start <= $macro.start and .end >= $macro.end));

def comment_lines:
  . as $comment |
  (.text | split("\n") | if .[-1] == "" then .[:-1] else . end) as $lines |
  range($lines | length) as $offset |
  {file: path, line: (line + $offset),
   marker: ($lines[$offset] | contains("ast-grep-ignore")),
   whole: (
     ($offset > 0 or ($comment.lines[:$comment.charCount.leading] | test("^\\s*$"))) and
     ($offset < ($lines | length) - 1 or $comment.charCount.trailing == 0 or
       ($comment.lines[-$comment.charCount.trailing:] | test("^\\s*$")))
   )};

def condition:
  .metaVariables.single |
  if has("IS_OK") then "is_ok"
  elif has("IS_ERR") then "is_err"
  elif has("IS_SOME") then "is_some"
  elif has("IS_NONE") then "is_none"
  elif has("NOT_EMPTY") then "!is_empty"
  elif has("CONTAINS") then "contains"
  elif has("LEN_GT") then "len > 0"
  elif has("LEN_GE") then "len >= 1"
  else "true"
  end;

# Different rules can report the same node. Use its byte offset for identity,
# since two attributed items can start on the same line.
($nodes | map(select(.ruleId == "reference")) | group_by(name_id("OWNER")) |
  INDEX(.[0] | name_id("OWNER")) | map_values(
    map(if .metaVariables.single.INVOKED != null then "!" + name("NAME") else name("NAME") end) | unique
  )) as $references |
($nodes | map(select(.ruleId != "reference" and .ruleId != "assertion")) |
  group_by([.file, .range.byteOffset.start]) | map(INDEX(.ruleId))) as $items |
[$items[] | select(has("test"))] as $tests |
($tests | map(.test) | group_by(.file) | INDEX(.[0].file)) as $tests_by_file |
[$items[] | select(has("function")) |
  (.function | name_id("NAME")) as $id |
  {id: $id, name: (.function | name("NAME")), path: (.function | path),
   site: (.function | site), test: has("test"), asserting: has("asserting"),
   references: ($references[$id] // [])}] as $functions |
[$items[] | .["macro-helper"]? | select(. != null and test_code($tests_by_file)) |
  {name: ("!" + name("NAME")), path: path}] as $macros |
($scopes | split("\n") | map(select(length > 0))) as $scopes |
[$scopes[] as $scope |
  ($functions | map(select(.path | in_scope($scope)))) as $local |
  ($local | map(select((.test | not) and (.path | test($test_path))))) as $helpers |
  (reduce (($helpers[] | select(.asserting)), ($macros[] | select(.path | in_scope($scope)))) as $helper
    ({}; .[$helper.name] = true)) as $seeds |
  helper_names($helpers; $seeds) as $names |
  {scope: $scope, tests: [$local[] | select(.test and (.asserting | not) and reaches_helper($names))]}
] as $indirect |
([$indirect[].tests[].id] | INDEX(.)) as $covered |
[$nodes[] | select(.ruleId == "assertion" and test_code($tests_by_file)) | path] as $macro_paths |
[$conditions[] | condition as $form |
  (.metaVariables.single.ID.text | ltrimstr("quality_") | tonumber) as $index |
  $assertions[0][$index] | select(test_code($tests_by_file)) | {form: $form, site: site}
] as $weak |
[$items[] | select(has("lint") and (.lint.text | test("^allow"))) ] as $allows |
[$items[] | select(has("lint") and (.lint.text | test("^expect"))) ] as $expects |
[$comments[] | comment_lines] as $comments |
($comments | map(select(.whole) | [.file, .line] | tojson) | INDEX(.)) as $whole_comments |
[$comments[] | select(.marker) | [.file, .line]] | unique |
group_by([.[0], (.[1] - 1)] | tojson | in($whole_comments)) |
INDEX((.[0] | [.[0], (.[1] - 1)] | tojson | in($whole_comments)) | tostring) as $markers |

[
  $scopes[] as $scope |
    row("assertion-count"; $scope; ($macro_paths | map(select(in_scope($scope))) | length); []),
    check("test-count"; $scope; [$tests[].test | select(path | in_scope($scope)) | site])
] + [
  ([$tests[] | select(.ignored.metaVariables.single.REASON != null)] |
    row("ignored-test"; "with reason"; length; [])),
  check("ignored-test"; "without reason"; [
    $tests[] | select(has("ignored") and .ignored.metaVariables.single.REASON == null) | .test | site
  ])
] + [
  $indirect[] | check("helper-assertion"; .scope; [.tests[].site])
] + [
  [$items[] | .["unseen-block"]? | select(. != null) | site] | group_by(.[0])[] |
  check("unseen-block"; (.[0][0] | tsv_key); .)
] + [
  $functions | map(select(.test and (.asserting | not) and (.id | in($covered) | not))) |
    group_by(.path)[] | check("no-assertion"; (.[0].path | tsv_key); map(.site))
] + [
  $scopes[] as $scope |
    $weak | map(select(.site[0] | in_scope($scope))) | group_by(.form)[] |
    check("weak-assertion"; ($scope + " " + .[0].form); map(.site))
] + [
  row("exemption"; "allow with reason"; ($allows | map(select(has("lint-reason"))) | length); []),
  check("exemption"; "allow without reason"; [$allows[] | select(has("lint-reason") | not) | .lint | site]),
  row("exemption"; "ast-grep-ignore with comment"; ($markers.true | length); []),
  check("exemption"; "ast-grep-ignore without comment"; ($markers.false // [])),
  row("exemption"; "expect attribute"; ($expects | length); [])
] |
sort_by([.signal, (if .signal == "no-assertion" or .signal == "weak-assertion" then -.count else 0 end), .key])[] |
"\(.signal)\t\(.key)\t\(.count)\t\(.places)"
