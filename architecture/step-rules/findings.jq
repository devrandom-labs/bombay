# Callback matches mark individual slots and capture the containing function.
# The other rules match the whole item directly.
($ranges | split("\n") | map(split(" ") | map(tonumber))) as $hunks
| group_by([.ruleId, (.metaVariables.single.ITEM.range // .range).byteOffset.start])[]
| select(.[0].ruleId != "callback-soup" or length >= $callbacks)
| .[0]
| (.metaVariables.single.ITEM.range // .range) as $item
| ($item.start.line + 1) as $first
| ($item.end.line + if $item.end.column > 0 then 1 else 0 end) as $last
| select(any($hunks[]; .[0] <= $last and .[1] >= $first))
| "error[signature-design]: \(.ruleId) at \($file):\($first) (\(.metaVariables.single.NAME.text))",
  "note: See bombay.md, Signature & Module Integrity. \(.message)"
