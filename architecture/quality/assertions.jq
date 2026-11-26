# ast-grep gives character columns as well as byte offsets. Use the columns
# to replace integer tokens without treating digits in strings as literals.
def compact: gsub("[[:space:]]"; "");

def lower_bound($value): bsearch($value) | if . < 0 then -1 - . else . end;

# The selector for macro names returns the invocation as a secondary match.
def unseen:
  (.text | startswith("macro_rules!")) or
  (.metaVariables.multi.secondary != null and (.text | test("\\bproptest$")));

def shape($integers):
  .range.start as $start |
  reduce ($integers | reverse[]) as $integer
    (.text | split("\n");
     ($integer.range.start.line - $start.line) as $line |
     (if $line == 0 then $start.column else 0 end) as $offset |
     .[$line] |= .[:($integer.range.start.column - $offset)] + "\u0000" +
       .[($integer.range.end.column - $offset):]) |
  join("\n") | compact;

group_by(.file) | map(
  (.[0].file | ltrimstr("./") | rtrimstr(".rs")) as $blob |
  (map(select(.text | test("^[0-9]"))) | sort_by(.range.byteOffset.start)) as $integers |
  ($integers | map(.range.byteOffset.start)) as $starts |
  {key: $blob, value: {unseen: any(.[]; unseen), assertions: [
    .[] | (.text | compact) as $text |
    select($text | test("^(?:::)?(?:insta::)?(?:r#)?(?:assert|prop_assert)[a-zA-Z0-9_]*![({\\[]")) |
    .range.byteOffset as $range |
    ($starts | lower_bound($range.start)) as $first |
    ($starts | lower_bound($range.end)) as $end |
    {text: $text, shape: shape($integers[$first:$end])}
  ]}}
) | from_entries
