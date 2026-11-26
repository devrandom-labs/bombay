# Find PostgreSQL declaration tokens outside comments and quoted values.
# This is a lexical scan of the declaration forms, with no SQL name resolution.
BEGIN { RS = "\0"; OFS = "\t" }

# Keep a byte cursor so each step reads only the token it consumes.
function advance() {
    if (substr(sql, at, 1) == "\n") line++
    at++
}

function block_comment(    depth, pair) {
    depth = 1
    at += 2
    while (depth && at <= limit) {
        pair = substr(sql, at, 2)
        if (pair == "/*" || pair == "*/") {
            depth += pair == "/*" ? 1 : -1
            at += 2
        } else advance()
    }
}

function dollar_tag(start,    end) {
    end = start + 1
    if (substr(sql, end, 1) ~ /[a-zA-Z_]/)
        while (substr(sql, end, 1) ~ /[a-zA-Z_0-9]/) end++
    return substr(sql, end, 1) == "$" ? end : 0
}

function dollar_quote(tag_end,    width, tag, end) {
    width = tag_end - at + 1
    tag = substr(sql, at, width)
    at = tag_end + 1
    while (at <= limit) {
        if (substr(sql, at, 1) == "$" && (end = dollar_tag(at))) {
            if (end - at + 1 == width && substr(sql, at, width) == tag) {
                at = end + 1
                return
            }
            at = end
        } else advance()
    }
}

function quoted(quote, escapes,    ch) {
    at++
    while (at <= limit) {
        ch = substr(sql, at, 1)
        advance()
        if (ch == quote) {
            if (substr(sql, at, 1) != quote) return 1
            at++
        } else if (escapes && ch == "\\" && at <= limit) advance()
    }
    return 0
}

function tokenize(    size, ch, pair, start, end, token) {
    delete names
    delete keywords
    delete lines
    size = 0
    line = at = 1
    limit = length(sql)
    while (at <= limit) {
        ch = substr(sql, at, 1)
        if (ch ~ /[[:space:]]/) { advance(); continue }
        pair = substr(sql, at, 2)
        if (pair == "--") {
            while (at <= limit && substr(sql, at, 1) != "\n") at++
            continue
        }
        if (pair == "/*") { block_comment(); continue }
        lines[++size] = line
        if (ch == "$" && (end = dollar_tag(at))) {
            dollar_quote(end)
        } else if (pair == "E'" || pair == "e'") {
            at++
            quoted("'", 1)
        } else if (ch == "'") {
            quoted(ch, 0)
        } else if (ch == "\"") {
            start = at + 1
            if (quoted(ch, 0)) {
                token = substr(sql, start, at - start - 1)
                gsub(/""/, "\"", token)
                names[size] = token
            }
        } else if (ch ~ /[a-zA-Z_]/) {
            start = at++
            while (substr(sql, at, 1) ~ /[a-zA-Z_0-9$]/) at++
            names[size] = keywords[size] = tolower(substr(sql, start, at - start))
        } else {
            keywords[size] = ch
            at++
        }
    }
    return size
}

{
    sql = $0
    path = substr(FILENAME, 3)
    size = tokenize()
    for (i = 1; i <= size; i++) {
        if (keywords[i] == "constraint" && names[i + 1] != "" && keywords[i + 2] == "check")
            print "sql-rule", names[i + 1], path, lines[i]
        if (keywords[i] != "create") continue
        j = i + 1
        if (keywords[j] == "or" && keywords[j + 1] == "replace") j += 2
        else if (keywords[j] == "constraint" && keywords[j + 1] == "trigger") j++
        if (keywords[j] == "trigger" && names[j + 1] != "")
            print "sql-rule", names[j + 1], path, lines[i]
        if (keywords[j] != "function" || names[j + 1] == "") continue
        j++
        while (keywords[j + 1] == "." && names[j + 2] != "") j += 2
        if (keywords[j + 1] == "(") print "sql-rule", names[j], path, lines[i]
    }
}
