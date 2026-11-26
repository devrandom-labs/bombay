include "report";

def in_scope: . as $path |
  any($scope_list[0][]; . as $scope | $scope == "." or ($path | startswith($scope + "/")));

# Keep each occurrence. Equal copies in the same file cancel before copies
# in different files, so unchanged assertions cannot take another file's place.
def groups($old; $new; key):
  ([$old[] | [., 0]] + [$new[] | [., 1]]) |
  group_by(.[0] | key)[] |
  [map(select(.[1] == 0) | .[0]), map(select(.[1] == 1) | .[0])];

def unmatched: [.[0][(.[1] | length):], .[1][(.[0] | length):]];

def sides:
  [map(.[0][]), map(.[1][])];

def location:
  gsub("%"; "%25") | gsub(" "; "%20") | gsub("\t"; "%09") |
  gsub("\r"; "%0D") | gsub("\n"; "%0A");

def entries($blob; $path):
  ($assertions[0][$blob].assertions // []) | map(. + {path: $path});

def snapshot_delta($files):
  first($files[] | select(.[2] == $snapshot) | . as [$old, $new] |
    $snapshots[0][$old] as $before | $snapshots[0][$new] as $after |
    select($before != null and $after != null) |
    {added: ($after - $before | length), removed: ($before - $after | length)})
  // null;

def expected_count($pattern):
  # Subtraction uses binary64. Keep only integers it can represent exactly.
  capture($pattern).count | gsub("_"; "") | tonumber | select(. <= 9007199254740991);

def authorized_pair($before; $after; $patterns; $delta):
  any($patterns[]; . as [$file, $assertion] |
    ($before.path | test($file)) and ($after.path | test($file)) and
    any(($before.text | expected_count($assertion)) as $old |
        ($after.text | expected_count($assertion)) - $old;
        . > 0 and . == $delta.added and $delta.removed == 0));

def scan($files; $patterns):
  snapshot_delta($files) as $delta |
  [$files[] | . as [$old, $new, $path] |
    groups(entries($old; $path); entries($new; $path); .text) | unmatched] | sides |
  [groups(.[0]; .[1]; .text) | unmatched] | sides |
  [groups(.[0]; .[1]; .shape)] as $pairs |
  ($pairs | map(unmatched) | sides) as [$old, $new] |
  [$pairs[] | . as [$before, $after] | range([($before | length), ($after | length)] | min) as $i |
    $before[$i] | select(.path | in_scope) |
    . + {authorized: authorized_pair(.; $after[$i]; $patterns; $delta)}] as $paired |
  [$old[] | select(.path | in_scope)] as $removed |
  [$paired[] | select(.authorized | not)] as $changed |
  {removed: ($removed | length), changed: ($changed | length),
   authorized: ([$paired[] | select(.authorized)] | length),
   added: ([$new[] | select(.path | in_scope)] | length),
   files: ([$removed[].path, $changed[].path] | unique | map(location)),
   unseen: ([$files[] | . as [$old, $new, $path] |
     select(($path | endswith(".rs") and in_scope) and
       ($assertions[0][$old].unseen or $assertions[0][$new].unseen)) |
     $path] | unique | map(location))};

def counts: "removed=\(.removed) changed=\(.changed) authorized=\(.authorized) added=\(.added)";
def edits: .removed + .changed;

($paths[0] | group_by(.[0]) | INDEX(.[0][0]) | map_values(map(.[1:]))) as $by_commit |
($authorized | split("\n") | map(select(length > 0 and (startswith("#") | not)) | split("\t"))) as $patterns |
[$commits | split("\n")[] | select(length > 0) |
  index("\t") as $tab | .[:$tab] as $id | .[$tab + 1:] as $subject |
  scan($by_commit[$id] // []; $patterns) +
  {key: ($id[:12] + " " + ($subject | gsub("[\t\r]"; " ") | .[:60]))}
] as $rows |
if $branch then
  $rows[0] | [.removed, .changed, .authorized, .added,
    (.files | sort | join(" ")), (.unseen | sort | join(" "))] | join("\t")
else
  ($rows | map(select(edits > 0)) | sort_by([-edits, .key])) as $edits |
  ($edits[] | ["oracle-edit", .key, edits, (counts + " " + (.files | places))]),
  (reduce $rows[] as $row ({removed: 0, changed: 0, authorized: 0, added: 0};
    .removed += $row.removed | .changed += $row.changed |
    .authorized += $row.authorized | .added += $row.added) |
   ["oracle-edit-total", "\($edits | length) of \($rows | length)", edits, counts]),
  ($rows | sort_by(.key)[] | select(.unseen | length > 0) |
    ["oracle-unseen", .key, (.unseen | length), (.unseen | places)]) |
  join("\t")
end
