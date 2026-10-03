# Readable local view of laravel-cloud-logging's JSON lines; anything else passes through.
# Usage: <command> 2>&1 | jq -Rr --unbuffered -f scripts/pretty.jq
(fromjson? // null) as $r
| if ($r | type) == "object" and $r.level_name then
    "\($r.datetime[11:19]) \($r.level_name | .[0:5]) \($r.message)  "
    + ($r.context | del(.exception) | to_entries | map("\(.key)=\(.value | tostring)") | join(" "))
    + (if $r.context.exception then "\n    \($r.context.exception.class): \($r.context.exception.message)\n    at \($r.context.exception.file)" else "" end)
  else . end
