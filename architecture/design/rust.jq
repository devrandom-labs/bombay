def whitespace: gsub("\\s+"; " ");

# Keep the source name and the last segment of each type path.
def primitive:
  "(u(8|16|32|64|128|size)|i(8|16|32|64|128|size)|f(16|32|64|128)|bool|String|&str|Uuid|Instant|Duration)" as $types
  | gsub("&\\s*'\\w+\\s+"; "&")
  | gsub("\\s+"; "")
  | sub("^::"; "")
  | gsub("(?<before>[<&])::"; "\(.before)")
  | gsub("(?<before>^|[<&])(?:(?:r#)?[[:alpha:]_][[:alnum:]_]*::)+"; "\(.before)")
  | select(test("^(" + $types + "|Option<" + $types + ">)$"));

def record($signal; $key):
  [$signal, $key, .file, (.range.start.line + 1)];

def match_records:
  .metaVariables.single as $m
  | if .ruleId == "primitive" then
      record("primitive"; "\($m.NAME.text): \($m.TYPE.text | primitive)")
    elif .ruleId == "column" then
      $m.COLUMN.text as $column
      | record("condition";
          (($column | sub("(?s)^.*Column(?=\\s*::)"; "Column"))
           + (.text | ltrimstr($column)) | whitespace))
    elif .ruleId == "length-comparison" or .ruleId == "length-range" then
      record("condition"; .text | whitespace)
    elif .ruleId == "state-comparison" then
      record("state-literal"; $m.LITERAL.text)
    elif .ruleId == "same-name" or .ruleId == "module" then
      record(.ruleId; $m.NAME.text)
    else empty end;

def imports:
  .[] | .metaVariables.single as $m
  | if .ruleId == "module-group" then
      .text | match("^(?:r#)?[[:alpha:]_][[:alnum:]_]*|^\\*").string
    elif .ruleId == "module-path" then
      ($m.NAME.text // "*")
    else empty end as $name
  | [($m.CRATE.text | sub("^::\\s*"; "") | ltrimstr("r#")),
     ($name | ltrimstr("r#")), .file];

def mirrors:
  map(select(.ruleId == "variant"))
  | group_by([.file, .metaVariables.single.ENUM.range.byteOffset.start])[]
  | (map(.metaVariables.single.NAME.text) | unique) as $variants
  | select($variants | length >= 2)
  | .[0] | .metaVariables.single.ENUM as $enum
  | ["mirror-enum", ($variants | join("|")), .file, ($enum.range.start.line + 1), $enum.text];

(if $mode == "imports" then imports else (.[] | match_records), mirrors end)
# Keep literal spelling and escape physical line breaks and tabs in a field.
| map(tostring | gsub("\t"; "\\t") | gsub("\r"; "\\r") | gsub("\n"; "\\n"))
| join("\t")
