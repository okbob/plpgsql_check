When format function is used in EXECUTE clause, then we can do more
static check. If format string contains only placeholer %L, then we
can replace placeholders by NULL, and we can use modified format
string as constant string. When placeholders %I are used, then we
can replace it by any valid SQL identifier, and we can do basic
syntax check.